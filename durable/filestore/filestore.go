// Package filestore is the standard-library durable Store reference adapter
// for a single local orchestration controller. Each state change appends one
// checksummed, fsynced snapshot frame; an incomplete final frame is discarded
// on recovery. The file is process-locked and mode 0600.
package filestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/kernel/canonicaljson"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

const (
	diskVersion   = 1
	frameHeader   = 8
	frameChecksum = sha256.Size
	maxFrameBytes = 64 << 20
)

type commandRecord struct {
	IdentityDigest string          `json:"identityDigest"`
	OutcomeDigest  string          `json:"outcomeDigest"`
	Outcome        json.RawMessage `json:"outcome"`
}

type diskState struct {
	Version    uint32                           `json:"version"`
	Aggregates map[string]store.AggregateRecord `json:"aggregates"`
	Events     map[string]contracts.DomainEvent `json:"events"`
	Commands   map[string]commandRecord         `json:"commands"`
	Intents    map[string]store.IntentRecord    `json:"intents"`
	Inbox      map[string]store.InboxRecord     `json:"inbox"`
}

type Store struct {
	mu       sync.RWMutex
	file     *os.File
	path     string
	state    diskState
	closed   bool
	poisoned error
}

func Open(path string) (*Store, error) {
	if path == "" || strings.TrimSpace(path) != path {
		return nil, errors.New("filestore path is required and canonical")
	}
	_, statErr := os.Stat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			_ = file.Close()
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return nil, err
	}
	if created {
		if err := syncParent(path); err != nil {
			return nil, err
		}
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, store.ErrLocked
		}
		return nil, err
	}
	state, recoveredOffset, err := load(file)
	if err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if recoveredOffset < info.Size() {
		if err := file.Truncate(recoveredOffset); err != nil {
			return nil, err
		}
		if err := file.Sync(); err != nil {
			return nil, err
		}
	}
	failed = false
	return &Store{file: file, path: path, state: state}, nil
}

func (fileStore *Store) Close() error {
	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()
	if fileStore.closed {
		return nil
	}
	fileStore.closed = true
	unlockErr := syscall.Flock(int(fileStore.file.Fd()), syscall.LOCK_UN)
	closeErr := fileStore.file.Close()
	return errors.Join(unlockErr, closeErr)
}

func (fileStore *Store) Commit(ctx context.Context, transaction store.Transaction) (store.CommitResult, error) {
	if err := ctx.Err(); err != nil {
		return store.CommitResult{}, err
	}
	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()
	if err := fileStore.ensureOpen(); err != nil {
		return store.CommitResult{}, err
	}
	if !contracts.ValidIdentifier(transaction.CommandID) || !contracts.ValidDigest(transaction.IdentityDigest) {
		return store.CommitResult{}, errors.New("transaction command id or identity digest is invalid")
	}
	if prior, exists := fileStore.state.Commands[transaction.CommandID]; exists {
		if prior.IdentityDigest != transaction.IdentityDigest {
			return store.CommitResult{}, store.ErrIdentityConflict
		}
		return store.CommitResult{Replay: true, OutcomeDigest: prior.OutcomeDigest, Outcome: cloneRaw(prior.Outcome)}, nil
	}
	if transaction.At.IsZero() {
		return store.CommitResult{}, errors.New("transaction time is required")
	}
	next, err := cloneState(fileStore.state)
	if err != nil {
		return store.CommitResult{}, err
	}
	outcome, outcomeDigest, err := normalizeRaw(transaction.Outcome)
	if err != nil {
		return store.CommitResult{}, fmt.Errorf("transaction outcome: %w", err)
	}
	if err := applyTransaction(&next, transaction); err != nil {
		return store.CommitResult{}, err
	}
	next.Commands[transaction.CommandID] = commandRecord{
		IdentityDigest: transaction.IdentityDigest, OutcomeDigest: outcomeDigest, Outcome: outcome,
	}
	if err := fileStore.append(next); err != nil {
		return store.CommitResult{}, err
	}
	fileStore.state = next
	return store.CommitResult{OutcomeDigest: outcomeDigest, Outcome: cloneRaw(outcome)}, nil
}

func (fileStore *Store) GetAggregate(ctx context.Context, key store.AggregateKey) (store.AggregateRecord, error) {
	if err := ctx.Err(); err != nil {
		return store.AggregateRecord{}, err
	}
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	if err := fileStore.ensureOpen(); err != nil {
		return store.AggregateRecord{}, err
	}
	record, exists := fileStore.state.Aggregates[keyString(key)]
	if !exists {
		return store.AggregateRecord{}, store.ErrNotFound
	}
	return cloneAggregate(record), nil
}

// ListAggregates returns one stable type-scoped page ordered by aggregate ID.
// afterID is exclusive, so callers can resume without offsets drifting while
// other aggregate types are appended.
func (fileStore *Store) ListAggregates(ctx context.Context, aggregateType, afterID string, limit int) ([]store.AggregateRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !contracts.ValidIdentifier(aggregateType) || (afterID != "" && !contracts.ValidIdentifier(afterID)) || limit <= 0 || limit > 1000 {
		return nil, errors.New("aggregate list type, cursor, or limit is invalid")
	}
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	if err := fileStore.ensureOpen(); err != nil {
		return nil, err
	}
	ids := make([]string, 0)
	for _, record := range fileStore.state.Aggregates {
		if record.Key.Type == aggregateType && record.Key.ID > afterID {
			ids = append(ids, record.Key.ID)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	result := make([]store.AggregateRecord, 0, len(ids))
	for _, id := range ids {
		key := store.AggregateKey{Type: aggregateType, ID: id}
		result = append(result, cloneAggregate(fileStore.state.Aggregates[keyString(key)]))
	}
	return result, nil
}

func (fileStore *Store) EventsAfter(ctx context.Context, key store.AggregateKey, revision uint64, limit int) ([]contracts.DomainEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, errors.New("event limit must be positive")
	}
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	if err := fileStore.ensureOpen(); err != nil {
		return nil, err
	}
	result := make([]contracts.DomainEvent, 0)
	for _, event := range fileStore.state.Events {
		if event.AggregateType == key.Type && event.AggregateID == key.ID && event.AggregateRevision > revision {
			result = append(result, cloneEvent(event))
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].AggregateRevision < result[right].AggregateRevision })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (fileStore *Store) ClaimIntents(ctx context.Context, request store.ClaimRequest) ([]store.ClaimedIntent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()
	if err := fileStore.ensureOpen(); err != nil {
		return nil, err
	}
	if !contracts.ValidIdentifier(request.OwnerRef) || request.Now.IsZero() || !request.LeaseExpiresAt.After(request.Now) || request.Limit <= 0 {
		return nil, errors.New("intent claim owner, times, or limit is invalid")
	}
	leaseHash, err := execution.PrivateTokenDigest(request.LeaseToken)
	if err != nil {
		return nil, err
	}
	kinds := make(map[contracts.DurableIntentKind]bool, len(request.Kinds))
	for _, kind := range request.Kinds {
		if !validIntentKind(kind) {
			return nil, errors.New("intent claim kind is invalid")
		}
		kinds[kind] = true
	}
	type candidate struct {
		id     string
		record store.IntentRecord
	}
	candidates := make([]candidate, 0)
	for id, record := range fileStore.state.Intents {
		if len(kinds) != 0 && !kinds[record.Intent.Kind] {
			continue
		}
		eligible := record.Status == store.IntentPending && !record.AvailableAt.After(request.Now)
		if record.Status == store.IntentLeased && record.LeaseExpiresAt != nil && !record.LeaseExpiresAt.After(request.Now) {
			eligible = true
		}
		if eligible {
			candidates = append(candidates, candidate{id: id, record: record})
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].record.AvailableAt.Equal(candidates[right].record.AvailableAt) {
			return candidates[left].id < candidates[right].id
		}
		return candidates[left].record.AvailableAt.Before(candidates[right].record.AvailableAt)
	})
	if len(candidates) > request.Limit {
		candidates = candidates[:request.Limit]
	}
	if len(candidates) == 0 {
		return []store.ClaimedIntent{}, nil
	}
	next, err := cloneState(fileStore.state)
	if err != nil {
		return nil, err
	}
	claimed := make([]store.ClaimedIntent, 0, len(candidates))
	for _, item := range candidates {
		record := next.Intents[item.id]
		record.Status = store.IntentLeased
		record.AttemptCount++
		record.LeaseOwner = request.OwnerRef
		record.LeaseTokenHash = leaseHash
		expires := request.LeaseExpiresAt.UTC()
		record.LeaseExpiresAt = &expires
		record.Revision++
		next.Intents[item.id] = record
		claimed = append(claimed, store.ClaimedIntent{Record: cloneIntent(record), LeaseToken: request.LeaseToken})
	}
	if err := fileStore.append(next); err != nil {
		return nil, err
	}
	fileStore.state = next
	return claimed, nil
}

func (fileStore *Store) CompleteIntent(ctx context.Context, fence store.IntentFence) (store.IntentRecord, error) {
	return fileStore.foldIntent(ctx, fence, func(record *store.IntentRecord) error {
		record.Status = store.IntentCompleted
		completed := fence.At.UTC()
		record.CompletedAt = &completed
		record.LastFailure = nil
		return nil
	})
}

func (fileStore *Store) FailIntent(ctx context.Context, failure store.IntentFailure) (store.IntentRecord, error) {
	if !failure.Failure.Class.Valid() || !contracts.ValidIdentifier(failure.Failure.Code) || failure.Failure.Message == "" {
		return store.IntentRecord{}, errors.New("intent failure is invalid")
	}
	if !failure.Dead && (failure.AvailableAt == nil || !failure.AvailableAt.After(failure.Fence.At)) {
		return store.IntentRecord{}, errors.New("retryable intent failure requires a future available time")
	}
	return fileStore.foldIntent(ctx, failure.Fence, func(record *store.IntentRecord) error {
		record.LastFailure = cloneFailure(&failure.Failure)
		if failure.Dead {
			record.Status = store.IntentDead
			completed := failure.Fence.At.UTC()
			record.CompletedAt = &completed
		} else {
			record.Status = store.IntentPending
			record.AvailableAt = failure.AvailableAt.UTC()
		}
		return nil
	})
}

func (fileStore *Store) GetIntent(ctx context.Context, id string) (store.IntentRecord, error) {
	if err := ctx.Err(); err != nil {
		return store.IntentRecord{}, err
	}
	fileStore.mu.RLock()
	defer fileStore.mu.RUnlock()
	if err := fileStore.ensureOpen(); err != nil {
		return store.IntentRecord{}, err
	}
	record, exists := fileStore.state.Intents[id]
	if !exists {
		return store.IntentRecord{}, store.ErrNotFound
	}
	return cloneIntent(record), nil
}

func (fileStore *Store) RecordInbox(ctx context.Context, record store.InboxRecord) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()
	if err := fileStore.ensureOpen(); err != nil {
		return false, err
	}
	if !contracts.ValidIdentifier(record.SourceRef) || !contracts.ValidIdentifier(record.MessageID) || !contracts.ValidDigest(record.PayloadDigest) || record.ObservedAt.IsZero() {
		return false, errors.New("inbox identity, digest, or time is invalid")
	}
	key := record.SourceRef + "\x00" + record.MessageID
	if prior, exists := fileStore.state.Inbox[key]; exists {
		if prior.PayloadDigest != record.PayloadDigest {
			return false, store.ErrIdentityConflict
		}
		return true, nil
	}
	next, err := cloneState(fileStore.state)
	if err != nil {
		return false, err
	}
	record.ObservedAt = record.ObservedAt.UTC()
	next.Inbox[key] = record
	if err := fileStore.append(next); err != nil {
		return false, err
	}
	fileStore.state = next
	return false, nil
}

func (fileStore *Store) foldIntent(ctx context.Context, fence store.IntentFence, fold func(*store.IntentRecord) error) (store.IntentRecord, error) {
	if err := ctx.Err(); err != nil {
		return store.IntentRecord{}, err
	}
	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()
	if err := fileStore.ensureOpen(); err != nil {
		return store.IntentRecord{}, err
	}
	record, exists := fileStore.state.Intents[fence.IntentID]
	if !exists {
		return store.IntentRecord{}, store.ErrNotFound
	}
	leaseHash, err := execution.PrivateTokenDigest(fence.LeaseToken)
	if err != nil || record.Status != store.IntentLeased || record.Revision != fence.ExpectedRevision ||
		record.LeaseOwner != fence.OwnerRef || record.LeaseTokenHash != leaseHash || record.LeaseExpiresAt == nil ||
		fence.At.IsZero() || !fence.At.Before(*record.LeaseExpiresAt) {
		return store.IntentRecord{}, store.ErrLeaseConflict
	}
	next, err := cloneState(fileStore.state)
	if err != nil {
		return store.IntentRecord{}, err
	}
	nextRecord := next.Intents[fence.IntentID]
	if err := fold(&nextRecord); err != nil {
		return store.IntentRecord{}, err
	}
	nextRecord.LeaseOwner = ""
	nextRecord.LeaseTokenHash = ""
	nextRecord.LeaseExpiresAt = nil
	nextRecord.Revision++
	next.Intents[fence.IntentID] = nextRecord
	if err := fileStore.append(next); err != nil {
		return store.IntentRecord{}, err
	}
	fileStore.state = next
	return cloneIntent(nextRecord), nil
}

func applyTransaction(state *diskState, transaction store.Transaction) error {
	expected := make(map[string]uint64, len(transaction.Expected))
	for _, item := range transaction.Expected {
		if err := validateKey(item.Key); err != nil {
			return err
		}
		key := keyString(item.Key)
		if _, duplicate := expected[key]; duplicate {
			return errors.New("transaction repeats an expected aggregate")
		}
		expected[key] = item.Revision
	}
	mutations := make(map[string]store.AggregateRecord, len(transaction.Mutations))
	for _, mutation := range transaction.Mutations {
		if err := validateKey(mutation.Key); err != nil {
			return err
		}
		key := keyString(mutation.Key)
		wantRevision, exists := expected[key]
		if !exists || mutation.Revision != wantRevision+1 {
			return store.ErrRevisionConflict
		}
		if _, duplicate := mutations[key]; duplicate {
			return errors.New("transaction repeats an aggregate mutation")
		}
		current, exists := state.Aggregates[key]
		currentRevision := uint64(0)
		if exists {
			currentRevision = current.Revision
		}
		if currentRevision != wantRevision {
			return store.ErrRevisionConflict
		}
		payload, digest, err := normalizeRaw(mutation.Payload)
		if err != nil || digest != mutation.PayloadDigest {
			return errors.New("aggregate payload or digest is invalid")
		}
		mutation.Payload = payload
		mutations[key] = mutation
	}
	if len(expected) != len(mutations) {
		return errors.New("every expected aggregate must have exactly one mutation")
	}
	eventByAggregate := make(map[string]contracts.DomainEvent, len(transaction.Events))
	for _, event := range transaction.Events {
		if err := validateEvent(event); err != nil {
			return err
		}
		if event.CommandID != transaction.CommandID {
			return errors.New("event command id does not match its transaction")
		}
		key := keyString(store.AggregateKey{Type: event.AggregateType, ID: event.AggregateID})
		mutation, exists := mutations[key]
		if !exists || mutation.Revision != event.AggregateRevision {
			return errors.New("event does not match a transaction mutation")
		}
		if _, duplicate := eventByAggregate[key]; duplicate {
			return errors.New("transaction emits multiple events for one aggregate revision")
		}
		if prior, exists := state.Events[event.EventID]; exists && prior.PayloadDigest != event.PayloadDigest {
			return store.ErrIdentityConflict
		}
		eventByAggregate[key] = event
	}
	if len(eventByAggregate) != len(mutations) {
		return errors.New("every aggregate mutation requires one matching event")
	}
	intentIDs := make(map[string]bool, len(transaction.Intents))
	for _, seed := range transaction.Intents {
		if err := validateIntentSeed(seed, transaction.At); err != nil {
			return err
		}
		if intentIDs[seed.Intent.Identity] {
			return errors.New("transaction repeats a durable intent identity")
		}
		intentIDs[seed.Intent.Identity] = true
		if _, exists := state.Intents[seed.Intent.Identity]; exists {
			return store.ErrIdentityConflict
		}
	}
	for key, mutation := range mutations {
		state.Aggregates[key] = cloneAggregate(mutation)
	}
	for _, event := range transaction.Events {
		state.Events[event.EventID] = cloneEvent(event)
	}
	for _, seed := range transaction.Intents {
		available := seed.AvailableAt
		if available.IsZero() {
			available = transaction.At
		}
		state.Intents[seed.Intent.Identity] = store.IntentRecord{
			Intent: cloneDurableIntent(seed.Intent), Status: store.IntentPending, AvailableAt: available.UTC(), Revision: 1,
		}
	}
	return nil
}

func validateEvent(event contracts.DomainEvent) error {
	if !contracts.ValidIdentifier(event.EventID) || !contracts.ValidIdentifier(event.AggregateType) || !contracts.ValidIdentifier(event.AggregateID) ||
		event.AggregateRevision == 0 || !contracts.ValidIdentifier(event.Type) || !contracts.ValidIdentifier(event.CommandID) ||
		!contracts.ValidDigest(event.PayloadSchemaDigest) || !contracts.ValidDigest(event.PayloadDigest) || event.OccurredAt.IsZero() {
		return errors.New("domain event envelope is invalid")
	}
	digest, err := canonicaljson.DigestValue(event.Payload)
	if err != nil || digest != event.PayloadDigest {
		return errors.New("domain event payload digest is invalid")
	}
	expectedID, err := execution.StableEventID(event.AggregateType, event.AggregateID, event.AggregateRevision)
	if err != nil || expectedID != event.EventID {
		return errors.New("domain event stable identity is invalid")
	}
	return nil
}

func validateIntentSeed(seed store.IntentSeed, transactionAt time.Time) error {
	intent := seed.Intent
	if !validIntentKind(intent.Kind) || !contracts.ValidIdentifier(intent.Identity) || !contracts.ValidIdentifier(intent.AggregateID) || !contracts.ValidDigest(intent.PayloadDigest) {
		return errors.New("durable intent envelope is invalid")
	}
	digest, err := canonicaljson.DigestValue(intent.Payload)
	if err != nil || digest != intent.PayloadDigest {
		return errors.New("durable intent payload digest is invalid")
	}
	if !seed.AvailableAt.IsZero() && seed.AvailableAt.Before(transactionAt) {
		return errors.New("durable intent availability precedes its transaction")
	}
	return nil
}

func validIntentKind(kind contracts.DurableIntentKind) bool {
	switch kind {
	case contracts.IntentOutbox, contracts.IntentReconcile, contracts.IntentCleanup, contracts.IntentWaitResolution:
		return true
	default:
		return false
	}
}

func validateKey(key store.AggregateKey) error {
	if !contracts.ValidIdentifier(key.Type) || !contracts.ValidIdentifier(key.ID) {
		return errors.New("aggregate key is invalid")
	}
	return nil
}

func keyString(key store.AggregateKey) string { return key.Type + "\x00" + key.ID }

func (fileStore *Store) append(state diskState) error {
	payload, err := canonicaljson.Marshal(state)
	if err != nil {
		return err
	}
	if len(payload) > maxFrameBytes {
		return fmt.Errorf("durable frame exceeds %d bytes", maxFrameBytes)
	}
	frame := make([]byte, frameHeader+len(payload)+frameChecksum)
	binary.BigEndian.PutUint64(frame[:frameHeader], uint64(len(payload)))
	copy(frame[frameHeader:], payload)
	sum := sha256.Sum256(payload)
	copy(frame[frameHeader+len(payload):], sum[:])
	if _, err := fileStore.file.Seek(0, io.SeekEnd); err != nil {
		fileStore.poisoned = err
		return err
	}
	if err := writeAll(fileStore.file, frame); err != nil {
		fileStore.poisoned = err
		return err
	}
	if err := fileStore.file.Sync(); err != nil {
		fileStore.poisoned = err
		return err
	}
	return nil
}

func load(file *os.File) (diskState, int64, error) {
	state := emptyState()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return diskState{}, 0, err
	}
	offset := int64(0)
	for {
		header := make([]byte, frameHeader)
		read, err := io.ReadFull(file, header)
		if errors.Is(err, io.EOF) && read == 0 {
			return state, offset, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || (errors.Is(err, io.EOF) && read > 0) {
			return state, offset, nil
		}
		if err != nil {
			return diskState{}, offset, err
		}
		length := binary.BigEndian.Uint64(header)
		if length == 0 || length > maxFrameBytes {
			return diskState{}, offset, store.ErrCorrupt
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(file, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return state, offset, nil
			}
			return diskState{}, offset, err
		}
		checksum := make([]byte, frameChecksum)
		if _, err := io.ReadFull(file, checksum); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return state, offset, nil
			}
			return diskState{}, offset, err
		}
		sum := sha256.Sum256(payload)
		if !bytes.Equal(checksum, sum[:]) {
			return diskState{}, offset, store.ErrCorrupt
		}
		var candidate diskState
		if err := canonicaljson.UnmarshalStrict(payload, &candidate); err != nil {
			return diskState{}, offset, fmt.Errorf("%w: %v", store.ErrCorrupt, err)
		}
		if err := validateState(candidate); err != nil {
			return diskState{}, offset, err
		}
		state = candidate
		offset += int64(frameHeader) + int64(length) + frameChecksum
	}
}

func validateState(state diskState) error {
	if state.Version != diskVersion || state.Aggregates == nil || state.Events == nil || state.Commands == nil || state.Intents == nil || state.Inbox == nil {
		return store.ErrCorrupt
	}
	for key, record := range state.Aggregates {
		if key != keyString(record.Key) || record.Revision == 0 {
			return store.ErrCorrupt
		}
		_, digest, err := normalizeRaw(record.Payload)
		if err != nil || digest != record.PayloadDigest {
			return store.ErrCorrupt
		}
	}
	for id, event := range state.Events {
		if id != event.EventID || validateEvent(event) != nil {
			return store.ErrCorrupt
		}
	}
	for id, command := range state.Commands {
		if !contracts.ValidIdentifier(id) || !contracts.ValidDigest(command.IdentityDigest) || !contracts.ValidDigest(command.OutcomeDigest) {
			return store.ErrCorrupt
		}
		_, digest, err := normalizeRaw(command.Outcome)
		if err != nil || digest != command.OutcomeDigest {
			return store.ErrCorrupt
		}
	}
	for id, intent := range state.Intents {
		if id != intent.Intent.Identity || intent.Revision == 0 || !validIntentKind(intent.Intent.Kind) {
			return store.ErrCorrupt
		}
		digest, err := canonicaljson.DigestValue(intent.Intent.Payload)
		if err != nil || digest != intent.Intent.PayloadDigest {
			return store.ErrCorrupt
		}
		if err := validateIntentRecord(intent); err != nil {
			return store.ErrCorrupt
		}
	}
	for key, inbox := range state.Inbox {
		if key != inbox.SourceRef+"\x00"+inbox.MessageID || !contracts.ValidIdentifier(inbox.SourceRef) ||
			!contracts.ValidIdentifier(inbox.MessageID) || !contracts.ValidDigest(inbox.PayloadDigest) || inbox.ObservedAt.IsZero() {
			return store.ErrCorrupt
		}
	}
	return nil
}

func validateIntentRecord(record store.IntentRecord) error {
	if record.AvailableAt.IsZero() || uint64(record.AttemptCount) > record.Revision {
		return store.ErrCorrupt
	}
	switch record.Status {
	case store.IntentPending:
		if record.LeaseOwner != "" || record.LeaseTokenHash != "" || record.LeaseExpiresAt != nil || record.CompletedAt != nil {
			return store.ErrCorrupt
		}
	case store.IntentLeased:
		if !contracts.ValidIdentifier(record.LeaseOwner) || !contracts.ValidDigest(record.LeaseTokenHash) || record.LeaseExpiresAt == nil || record.CompletedAt != nil {
			return store.ErrCorrupt
		}
	case store.IntentCompleted:
		if record.LeaseOwner != "" || record.LeaseTokenHash != "" || record.LeaseExpiresAt != nil || record.CompletedAt == nil || record.LastFailure != nil {
			return store.ErrCorrupt
		}
	case store.IntentDead:
		if record.LeaseOwner != "" || record.LeaseTokenHash != "" || record.LeaseExpiresAt != nil || record.CompletedAt == nil || record.LastFailure == nil {
			return store.ErrCorrupt
		}
	default:
		return store.ErrCorrupt
	}
	return nil
}

func emptyState() diskState {
	return diskState{
		Version: diskVersion, Aggregates: map[string]store.AggregateRecord{}, Events: map[string]contracts.DomainEvent{},
		Commands: map[string]commandRecord{}, Intents: map[string]store.IntentRecord{}, Inbox: map[string]store.InboxRecord{},
	}
}

func cloneState(state diskState) (diskState, error) {
	raw, err := canonicaljson.Marshal(state)
	if err != nil {
		return diskState{}, err
	}
	var clone diskState
	if err := canonicaljson.UnmarshalStrict(raw, &clone); err != nil {
		return diskState{}, err
	}
	return clone, nil
}

func normalizeRaw(raw json.RawMessage) (json.RawMessage, string, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	canonical, err := canonicaljson.Canonicalize(raw)
	if err != nil {
		return nil, "", err
	}
	digest, err := canonicaljson.Digest(canonical)
	return json.RawMessage(canonical), digest, err
}

func writeAll(writer io.Writer, raw []byte) error {
	for len(raw) > 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}

func (fileStore *Store) ensureOpen() error {
	if fileStore.closed {
		return os.ErrClosed
	}
	if fileStore.poisoned != nil {
		return fmt.Errorf("durable store write state is uncertain: %w", fileStore.poisoned)
	}
	return nil
}

func syncParent(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func cloneRaw(raw json.RawMessage) json.RawMessage { return append(json.RawMessage(nil), raw...) }

func cloneAggregate(record store.AggregateRecord) store.AggregateRecord {
	record.Payload = cloneRaw(record.Payload)
	return record
}

func cloneEvent(event contracts.DomainEvent) contracts.DomainEvent {
	event.Payload = cloneMap(event.Payload)
	return event
}

func cloneIntent(record store.IntentRecord) store.IntentRecord {
	record.Intent = cloneDurableIntent(record.Intent)
	record.LeaseExpiresAt = cloneTime(record.LeaseExpiresAt)
	record.CompletedAt = cloneTime(record.CompletedAt)
	record.LastFailure = cloneFailure(record.LastFailure)
	return record
}

func cloneDurableIntent(intent contracts.DurableIntent) contracts.DurableIntent {
	intent.Payload = cloneMap(intent.Payload)
	return intent
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	raw, _ := canonicaljson.Marshal(value)
	var clone map[string]any
	_ = json.Unmarshal(raw, &clone)
	return clone
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneFailure(value *contracts.StructuredFailure) *contracts.StructuredFailure {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
