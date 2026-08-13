// Package filestore is the standard-library durable Store reference adapter
// for a single local orchestration controller. State changes use committed,
// checksummed v2 snapshot frames. A stable sidecar, rather than the replaceable
// data inode, carries the process lock for the complete Store lifetime.
package filestore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	diskVersion          = 2
	frameFormatVersion   = 2
	frameHeader          = 96
	frameFooter          = 128
	frameHeaderCore      = frameHeader - sha256.Size
	frameFooterCore      = frameFooter - sha256.Size
	maxFrameBytes        = 64 << 20
	retainedFrameBudget  = 4
	maxEncodedFrameBytes = frameHeader + maxFrameBytes + frameFooter
	maxJournalBytes      = maxEncodedFrameBytes
	maxRecoveryBytes     = maxEncodedFrameBytes
	lockSuffix           = ".lock"
	frameFlagOffset      = 12
	framePayloadLength   = 16
	frameEncodedLength   = 24
	framePayloadDigest   = 32
	footerPayloadLength  = 16
	footerEncodedLength  = 24
	footerPayloadDigest  = 32
	footerHeaderDigest   = 64
)

var (
	frameMagic       = [8]byte{'X', 'G', 'C', '2', 'F', 'S', 'V', '2'}
	frameCommitMagic = [8]byte{'X', 'G', 'C', '2', 'C', 'M', 'V', '2'}
	errUnsafePath    = errors.New("filestore path authority changed or is unsafe")
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
	mu         sync.RWMutex
	file       *os.File
	lockFile   *os.File
	parent     *os.File
	parentPath string
	dataName   string
	lockName   string
	parentID   fileIdentity
	dataID     fileIdentity
	lockID     fileIdentity
	state      diskState
	frameCount int
	closed     bool
	poisoned   error
	hooks      *ioHooks
}

func Open(path string) (*Store, error) {
	return openWithHooks(path, nil)
}

// ioHooks is deliberately package-private. It makes every durability barrier
// faultable in deterministic tests without weakening the public Store port.
type ioHooks struct {
	write    func(string, *os.File, []byte) (int, error)
	sync     func(string, *os.File) error
	rename   func(*os.File, string, string) error
	flock    func(string, *os.File, int) error
	close    func(string, *os.File) error
	truncate func(*os.File, int64) error
}

func openWithHooks(path string, hooks *ioHooks) (_ *Store, returnedErr error) {
	parentPath, dataName, err := canonicalStorePath(path)
	if err != nil {
		return nil, err
	}
	parent, parentID, err := openCanonicalParent(parentPath)
	if err != nil {
		return nil, err
	}
	result := &Store{
		parent: parent, parentPath: parentPath, dataName: dataName,
		lockName: dataName + lockSuffix, parentID: parentID, hooks: hooks,
	}
	locked := false
	succeeded := false
	defer func() {
		if succeeded {
			return
		}
		var cleanup []error
		if result.file != nil {
			cleanup = append(cleanup, result.doClose("data-close", result.file))
		}
		if locked && result.lockFile != nil {
			cleanup = append(cleanup, result.doFlock("unlock", result.lockFile, syscall.LOCK_UN))
		}
		if result.lockFile != nil {
			cleanup = append(cleanup, result.doClose("lock-close", result.lockFile))
		}
		if result.parent != nil {
			cleanup = append(cleanup, result.doClose("parent-close", result.parent))
		}
		cleanup = append([]error{returnedErr}, cleanup...)
		returnedErr = errors.Join(cleanup...)
	}()

	lockFile, lockID, err := openRegularAt(parent, result.lockName)
	if err != nil {
		return nil, fmt.Errorf("open filestore lock: %w", err)
	}
	result.lockFile, result.lockID = lockFile, lockID
	if err := result.doFlock("lock", lockFile, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, store.ErrLocked
		}
		return nil, fmt.Errorf("lock filestore: %w", err)
	}
	locked = true
	if err := result.verifyParentAndLock(); err != nil {
		return nil, err
	}

	dataFile, dataID, err := openRegularAt(parent, dataName)
	if err != nil {
		return nil, fmt.Errorf("open filestore data: %w", err)
	}
	result.file, result.dataID = dataFile, dataID
	dataInfo, err := dataFile.Stat()
	if err != nil {
		return nil, err
	}
	initialSize := dataInfo.Size()
	state, recoveredOffset, frameCount, partialTail, err := load(dataFile)
	if err != nil {
		return nil, err
	}
	if initialSize > 0 && frameCount == 0 && !partialV2Prefix(dataFile, initialSize) {
		return nil, store.ErrCorrupt
	}
	result.state, result.frameCount = state, frameCount
	if partialTail {
		if err := result.doTruncate(dataFile, recoveredOffset); err != nil {
			return nil, fmt.Errorf("truncate incomplete filestore tail: %w", err)
		}
		if err := result.doSync("data-sync", dataFile); err != nil {
			return nil, fmt.Errorf("sync recovered filestore: %w", err)
		}
	}
	if frameCount > 1 {
		frame, frameErr := encodeFrame(state)
		if frameErr != nil {
			return nil, frameErr
		}
		if err := result.replaceFrame(frame); err != nil {
			return nil, err
		}
	}
	// Open never reports success until both persistent objects and the pinned,
	// canonical parent directory have crossed an fsync barrier.
	if err := result.doSync("lock-sync", result.lockFile); err != nil {
		return nil, fmt.Errorf("sync filestore lock: %w", err)
	}
	if err := result.doSync("data-sync", result.file); err != nil {
		return nil, fmt.Errorf("sync filestore data: %w", err)
	}
	if err := result.doSync("dir-sync", result.parent); err != nil {
		return nil, fmt.Errorf("sync filestore parent: %w", err)
	}
	if err := result.verifyAuthority(); err != nil {
		return nil, err
	}
	succeeded = true
	return result, nil
}

func (fileStore *Store) Close() error {
	fileStore.mu.Lock()
	defer fileStore.mu.Unlock()
	if fileStore.closed {
		return nil
	}
	fileStore.closed = true
	dataCloseErr := fileStore.doClose("data-close", fileStore.file)
	unlockErr := fileStore.doFlock("unlock", fileStore.lockFile, syscall.LOCK_UN)
	lockCloseErr := fileStore.doClose("lock-close", fileStore.lockFile)
	parentCloseErr := fileStore.doClose("parent-close", fileStore.parent)
	return errors.Join(unlockErr, dataCloseErr, lockCloseErr, parentCloseErr)
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
	case contracts.IntentOutbox, contracts.IntentReconcile, contracts.IntentCleanup, contracts.IntentWaitResolution, contracts.IntentChildResolution:
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
	frame, err := encodeFrame(state)
	if err != nil {
		return err
	}
	if err := fileStore.ensureOpen(); err != nil {
		return err
	}
	if err := fileStore.verifyAuthority(); err != nil {
		fileStore.poisoned = err
		return err
	}
	info, err := fileStore.file.Stat()
	if err != nil {
		fileStore.poisoned = err
		return err
	}
	if shouldCompact(info.Size(), int64(len(frame)), fileStore.frameCount) {
		err = fileStore.replaceFrame(frame)
		if err != nil {
			fileStore.poisoned = err
		}
		return err
	}
	if _, err := fileStore.file.Seek(0, io.SeekEnd); err != nil {
		fileStore.poisoned = err
		return err
	}
	if err := fileStore.writeAll("data-write", fileStore.file, frame); err != nil {
		fileStore.poisoned = err
		return err
	}
	if err := fileStore.doSync("data-sync", fileStore.file); err != nil {
		fileStore.poisoned = err
		return err
	}
	fileStore.frameCount++
	if err := fileStore.verifyAuthority(); err != nil {
		fileStore.poisoned = err
		return err
	}
	return nil
}

func encodeFrame(state diskState) ([]byte, error) {
	payload, err := canonicaljson.MarshalWithLimits(state, frameJSONLimits())
	if err != nil {
		return nil, err
	}
	if len(payload) > maxFrameBytes {
		return nil, fmt.Errorf("durable frame exceeds %d bytes", maxFrameBytes)
	}
	if len(payload) == 0 {
		return nil, errors.New("durable frame payload is empty")
	}
	encodedLength := frameHeader + len(payload) + frameFooter
	frame := make([]byte, encodedLength)
	copy(frame[:8], frameMagic[:])
	binary.BigEndian.PutUint16(frame[8:10], frameFormatVersion)
	binary.BigEndian.PutUint16(frame[10:12], frameHeader)
	binary.BigEndian.PutUint64(frame[framePayloadLength:framePayloadLength+8], uint64(len(payload)))
	binary.BigEndian.PutUint64(frame[frameEncodedLength:frameEncodedLength+8], uint64(encodedLength))
	payloadSum := sha256.Sum256(payload)
	copy(frame[framePayloadDigest:framePayloadDigest+sha256.Size], payloadSum[:])
	headerSum := sha256.Sum256(frame[:frameHeaderCore])
	copy(frame[frameHeaderCore:frameHeader], headerSum[:])
	copy(frame[frameHeader:], payload)
	footer := frame[frameHeader+len(payload):]
	copy(footer[:8], frameCommitMagic[:])
	binary.BigEndian.PutUint16(footer[8:10], frameFormatVersion)
	binary.BigEndian.PutUint16(footer[10:12], frameFooter)
	binary.BigEndian.PutUint64(footer[footerPayloadLength:footerPayloadLength+8], uint64(len(payload)))
	binary.BigEndian.PutUint64(footer[footerEncodedLength:footerEncodedLength+8], uint64(encodedLength))
	copy(footer[footerPayloadDigest:footerPayloadDigest+sha256.Size], payloadSum[:])
	copy(footer[footerHeaderDigest:footerHeaderDigest+sha256.Size], headerSum[:])
	footerSum := sha256.Sum256(footer[:frameFooterCore])
	copy(footer[frameFooterCore:frameFooter], footerSum[:])
	return frame, nil
}

func shouldCompact(currentBytes, nextFrameBytes int64, frameCount int) bool {
	return currentBytes+nextFrameBytes > maxJournalBytes ||
		frameCount >= retainedFrameBudget
}

// replaceFrame installs a complete snapshot while the independent sidecar lock
// remains held. All namespace operations are relative to the pinned parent fd,
// so replacing the path's parent cannot redirect an in-flight compaction.
func (fileStore *Store) replaceFrame(frame []byte) error {
	if err := fileStore.verifyAuthority(); err != nil {
		return err
	}
	stageName, stage, err := createStageAt(fileStore.parent, fileStore.dataName)
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		if installed {
			return
		}
		_ = fileStore.doClose("stage-close", stage)
		_ = syscall.Unlinkat(int(fileStore.parent.Fd()), stageName)
	}()
	if err := fileStore.writeAll("stage-write", stage, frame); err != nil {
		return err
	}
	if err := fileStore.doSync("data-sync", stage); err != nil {
		return err
	}
	dataID, err := regularIdentity(stage)
	if err != nil {
		return err
	}
	if err := fileStore.doRename(stageName, fileStore.dataName); err != nil {
		return err
	}
	installed = true
	prior := fileStore.file
	fileStore.file = stage
	fileStore.dataID = dataID
	fileStore.frameCount = 1
	directorySyncErr := fileStore.doSync("dir-sync", fileStore.parent)
	authorityErr := fileStore.verifyAuthority()
	priorCloseErr := fileStore.doClose("replaced-data-close", prior)
	return errors.Join(directorySyncErr, authorityErr, priorCloseErr)
}

type frameMetadata struct {
	payloadLength uint64
	encodedLength uint64
	payloadDigest [sha256.Size]byte
	headerDigest  [sha256.Size]byte
}

// load validates every committed frame within explicit byte and frame-count
// limits. Only a suffix that cannot contain a valid commit footer is a crash
// tail; any malformed committed frame fails closed instead of rolling back.
func load(file *os.File) (diskState, int64, int, bool, error) {
	info, err := file.Stat()
	if err != nil {
		return diskState{}, 0, 0, false, err
	}
	fileSize := info.Size()
	if fileSize < 0 || fileSize > maxRecoveryBytes {
		return diskState{}, 0, 0, false, store.ErrCorrupt
	}
	offset := int64(0)
	frameCount := 0
	latest := emptyState()
	header := make([]byte, frameHeader)
	for offset < fileSize {
		if frameCount >= retainedFrameBudget {
			return diskState{}, offset, frameCount, false, store.ErrCorrupt
		}
		remaining := fileSize - offset
		if remaining < frameHeader {
			return latest, offset, frameCount, true, nil
		}
		if _, err := file.ReadAt(header, offset); err != nil {
			return diskState{}, offset, frameCount, false, err
		}
		metadata, err := decodeHeader(header)
		if err != nil {
			return diskState{}, offset, frameCount, false, err
		}
		frameBytes := int64(metadata.encodedLength)
		if frameBytes > remaining {
			committed, footerErr := committedFooterInRange(file, offset, remaining)
			if footerErr != nil {
				return diskState{}, offset, frameCount, false, footerErr
			}
			if committed {
				return diskState{}, offset, frameCount, false, store.ErrCorrupt
			}
			return latest, offset, frameCount, true, nil
		}
		payload := make([]byte, int(metadata.payloadLength))
		if _, err := file.ReadAt(payload, offset+frameHeader); err != nil {
			return diskState{}, offset, frameCount, false, err
		}
		payloadSum := sha256.Sum256(payload)
		if !bytes.Equal(payloadSum[:], metadata.payloadDigest[:]) {
			return diskState{}, offset, frameCount, false, store.ErrCorrupt
		}
		footer := make([]byte, frameFooter)
		footerOffset := offset + frameHeader + int64(metadata.payloadLength)
		if _, err := file.ReadAt(footer, footerOffset); err != nil {
			return diskState{}, offset, frameCount, false, err
		}
		footerMetadata, err := decodeFooter(footer)
		if err != nil || footerMetadata != metadata {
			return diskState{}, offset, frameCount, false, store.ErrCorrupt
		}
		var state diskState
		if err := canonicaljson.UnmarshalStrictWithLimits(payload, &state, frameJSONLimits()); err != nil {
			return diskState{}, offset, frameCount, false, fmt.Errorf("%w: %v", store.ErrCorrupt, err)
		}
		if err := validateState(state); err != nil {
			return diskState{}, offset, frameCount, false, err
		}
		latest = state
		offset += frameBytes
		frameCount++
	}
	return latest, offset, frameCount, false, nil
}

func decodeHeader(header []byte) (frameMetadata, error) {
	if len(header) != frameHeader || !bytes.Equal(header[:8], frameMagic[:]) ||
		binary.BigEndian.Uint16(header[8:10]) != frameFormatVersion ||
		binary.BigEndian.Uint16(header[10:12]) != frameHeader ||
		binary.BigEndian.Uint32(header[frameFlagOffset:framePayloadLength]) != 0 {
		return frameMetadata{}, store.ErrCorrupt
	}
	headerSum := sha256.Sum256(header[:frameHeaderCore])
	if !bytes.Equal(header[frameHeaderCore:], headerSum[:]) {
		return frameMetadata{}, store.ErrCorrupt
	}
	metadata := frameMetadata{
		payloadLength: binary.BigEndian.Uint64(header[framePayloadLength : framePayloadLength+8]),
		encodedLength: binary.BigEndian.Uint64(header[frameEncodedLength : frameEncodedLength+8]),
	}
	copy(metadata.payloadDigest[:], header[framePayloadDigest:framePayloadDigest+sha256.Size])
	copy(metadata.headerDigest[:], header[frameHeaderCore:frameHeader])
	if metadata.payloadLength == 0 || metadata.payloadLength > maxFrameBytes ||
		metadata.encodedLength != frameHeader+metadata.payloadLength+frameFooter ||
		metadata.encodedLength > maxEncodedFrameBytes {
		return frameMetadata{}, store.ErrCorrupt
	}
	return metadata, nil
}

func decodeFooter(footer []byte) (frameMetadata, error) {
	if len(footer) != frameFooter || !bytes.Equal(footer[:8], frameCommitMagic[:]) ||
		binary.BigEndian.Uint16(footer[8:10]) != frameFormatVersion ||
		binary.BigEndian.Uint16(footer[10:12]) != frameFooter ||
		binary.BigEndian.Uint32(footer[frameFlagOffset:footerPayloadLength]) != 0 {
		return frameMetadata{}, store.ErrCorrupt
	}
	footerSum := sha256.Sum256(footer[:frameFooterCore])
	if !bytes.Equal(footer[frameFooterCore:], footerSum[:]) {
		return frameMetadata{}, store.ErrCorrupt
	}
	metadata := frameMetadata{
		payloadLength: binary.BigEndian.Uint64(footer[footerPayloadLength : footerPayloadLength+8]),
		encodedLength: binary.BigEndian.Uint64(footer[footerEncodedLength : footerEncodedLength+8]),
	}
	copy(metadata.payloadDigest[:], footer[footerPayloadDigest:footerPayloadDigest+sha256.Size])
	copy(metadata.headerDigest[:], footer[footerHeaderDigest:footerHeaderDigest+sha256.Size])
	if metadata.payloadLength == 0 || metadata.payloadLength > maxFrameBytes ||
		metadata.encodedLength != frameHeader+metadata.payloadLength+frameFooter ||
		metadata.encodedLength > maxEncodedFrameBytes {
		return frameMetadata{}, store.ErrCorrupt
	}
	return metadata, nil
}

// committedFooterInRange performs a bounded search for a self-validating commit
// record whose repeated encoded length places it at the current frame boundary.
// Its presence turns a header length mismatch into committed corruption.
func committedFooterInRange(file *os.File, offset, remaining int64) (bool, error) {
	if remaining < frameFooter {
		return false, nil
	}
	raw := make([]byte, int(remaining))
	if _, err := file.ReadAt(raw, offset); err != nil {
		return false, err
	}
	searchAt := 0
	for searchAt+frameFooter <= len(raw) {
		candidateAt := bytes.Index(raw[searchAt:], frameCommitMagic[:])
		if candidateAt < 0 {
			return false, nil
		}
		candidateAt += searchAt
		if candidateAt+frameFooter <= len(raw) {
			metadata, err := decodeFooter(raw[candidateAt : candidateAt+frameFooter])
			if err == nil && metadata.encodedLength == uint64(candidateAt+frameFooter) {
				return true, nil
			}
		}
		searchAt = candidateAt + 1
	}
	return false, nil
}

func partialV2Prefix(file *os.File, size int64) bool {
	if size <= 0 {
		return true
	}
	prefixLength := size
	if prefixLength > int64(len(frameMagic)) {
		prefixLength = int64(len(frameMagic))
	}
	prefix := make([]byte, int(prefixLength))
	if _, err := file.ReadAt(prefix, 0); err != nil {
		return false
	}
	return bytes.Equal(prefix, frameMagic[:len(prefix)])
}

type fileIdentity struct {
	device uint64
	inode  uint64
}

func identityFromInfo(info os.FileInfo) (fileIdentity, uint64, error) {
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fileIdentity{}, 0, errUnsafePath
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, 0, errUnsafePath
	}
	return fileIdentity{device: uint64(stat.Dev), inode: stat.Ino}, uint64(stat.Nlink), nil
}

func directoryIdentity(info os.FileInfo) (fileIdentity, error) {
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fileIdentity{}, errUnsafePath
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, errUnsafePath
	}
	return fileIdentity{device: uint64(stat.Dev), inode: stat.Ino}, nil
}

func canonicalStorePath(path string) (string, string, error) {
	if path == "" || strings.TrimSpace(path) != path || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", "", errors.New("filestore path must be absolute, clean, and canonical")
	}
	parentPath := filepath.Dir(path)
	dataName := filepath.Base(path)
	if dataName == "." || dataName == string(filepath.Separator) || dataName == "" {
		return "", "", errors.New("filestore path must name a file")
	}
	if info, err := os.Lstat(path); err == nil {
		if _, nlink, identityErr := identityFromInfo(info); identityErr != nil || nlink != 1 {
			return "", "", errUnsafePath
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	if info, err := os.Lstat(path + lockSuffix); err == nil {
		if _, nlink, identityErr := identityFromInfo(info); identityErr != nil || nlink != 1 {
			return "", "", errUnsafePath
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	return parentPath, dataName, nil
}

func openCanonicalParent(parentPath string) (*os.File, fileIdentity, error) {
	rootFD, err := syscall.Open(string(filepath.Separator), syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fileIdentity{}, err
	}
	parent := os.NewFile(uintptr(rootFD), string(filepath.Separator))
	relative := strings.TrimPrefix(parentPath, string(filepath.Separator))
	if relative != "" {
		for _, component := range strings.Split(relative, string(filepath.Separator)) {
			fd, openErr := syscall.Openat(int(parent.Fd()), component, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
			if openErr != nil {
				_ = parent.Close()
				return nil, fileIdentity{}, errors.Join(errUnsafePath, openErr)
			}
			next := os.NewFile(uintptr(fd), component)
			if closeErr := parent.Close(); closeErr != nil {
				_ = next.Close()
				return nil, fileIdentity{}, closeErr
			}
			parent = next
		}
	}
	info, err := parent.Stat()
	if err != nil {
		_ = parent.Close()
		return nil, fileIdentity{}, err
	}
	identity, err := directoryIdentity(info)
	if err != nil {
		_ = parent.Close()
		return nil, fileIdentity{}, err
	}
	return parent, identity, nil
}

func openRegularAt(parent *os.File, name string) (*os.File, fileIdentity, error) {
	fd, err := syscall.Openat(int(parent.Fd()), name, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fileIdentity{}, err
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fileIdentity{}, err
	}
	identity, nlink, err := identityFromInfo(info)
	if err != nil || nlink != 1 {
		_ = file.Close()
		return nil, fileIdentity{}, errUnsafePath
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fileIdentity{}, err
	}
	return file, identity, nil
}

func regularIdentity(file *os.File) (fileIdentity, error) {
	info, err := file.Stat()
	if err != nil {
		return fileIdentity{}, err
	}
	identity, nlink, err := identityFromInfo(info)
	if err != nil || nlink != 1 {
		return fileIdentity{}, errUnsafePath
	}
	return identity, nil
}

func createStageAt(parent *os.File, dataName string) (string, *os.File, error) {
	for attempt := 0; attempt < 32; attempt++ {
		var entropy [12]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return "", nil, err
		}
		name := "." + dataName + ".compact-" + hex.EncodeToString(entropy[:])
		fd, err := syscall.Openat(int(parent.Fd()), name, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0o600)
		if errors.Is(err, syscall.EEXIST) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		return name, os.NewFile(uintptr(fd), name), nil
	}
	return "", nil, errors.New("could not allocate filestore stage file")
}

func (fileStore *Store) verifyParentAndLock() error {
	return fileStore.verifyNamespace(false)
}

func (fileStore *Store) verifyAuthority() error {
	return fileStore.verifyNamespace(true)
}

// verifyNamespace re-walks every parent component without following symlinks.
// Comparing only the final parent inode is insufficient: an ancestor can be
// renamed and replaced by a symlink back to the same inode.
func (fileStore *Store) verifyNamespace(includeData bool) (returnedErr error) {
	currentParent, currentParentID, err := openCanonicalParent(fileStore.parentPath)
	if err != nil {
		return errors.Join(errUnsafePath, err)
	}
	defer func() {
		returnedErr = errors.Join(returnedErr, currentParent.Close())
	}()
	if currentParentID != fileStore.parentID {
		return errUnsafePath
	}
	lockID, lockLinks, err := existingRegularIdentityAt(currentParent, fileStore.lockName)
	if err != nil || lockLinks != 1 || lockID != fileStore.lockID {
		return errors.Join(errUnsafePath, err)
	}
	if !includeData {
		return nil
	}
	dataID, dataLinks, err := existingRegularIdentityAt(currentParent, fileStore.dataName)
	if err != nil || dataLinks != 1 || dataID != fileStore.dataID || dataID == lockID {
		return errors.Join(errUnsafePath, err)
	}
	return nil
}

func existingRegularIdentityAt(parent *os.File, name string) (_ fileIdentity, _ uint64, returnedErr error) {
	fd, err := syscall.Openat(int(parent.Fd()), name, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fileIdentity{}, 0, err
	}
	file := os.NewFile(uintptr(fd), name)
	defer func() {
		returnedErr = errors.Join(returnedErr, file.Close())
	}()
	info, err := file.Stat()
	if err != nil {
		return fileIdentity{}, 0, err
	}
	return identityFromInfo(info)
}

func (fileStore *Store) writeAll(operation string, file *os.File, raw []byte) error {
	for len(raw) > 0 {
		written, err := fileStore.doWrite(operation, file, raw)
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

func (fileStore *Store) doWrite(operation string, file *os.File, raw []byte) (int, error) {
	if fileStore.hooks != nil && fileStore.hooks.write != nil {
		return fileStore.hooks.write(operation, file, raw)
	}
	return file.Write(raw)
}

func (fileStore *Store) doSync(operation string, file *os.File) error {
	if fileStore.hooks != nil && fileStore.hooks.sync != nil {
		return fileStore.hooks.sync(operation, file)
	}
	return file.Sync()
}

func (fileStore *Store) doRename(oldName, newName string) error {
	if fileStore.hooks != nil && fileStore.hooks.rename != nil {
		return fileStore.hooks.rename(fileStore.parent, oldName, newName)
	}
	return syscall.Renameat(int(fileStore.parent.Fd()), oldName, int(fileStore.parent.Fd()), newName)
}

func (fileStore *Store) doFlock(operation string, file *os.File, how int) error {
	if fileStore.hooks != nil && fileStore.hooks.flock != nil {
		return fileStore.hooks.flock(operation, file, how)
	}
	return syscall.Flock(int(file.Fd()), how)
}

func (fileStore *Store) doClose(operation string, file *os.File) error {
	if file == nil {
		return nil
	}
	if fileStore.hooks != nil && fileStore.hooks.close != nil {
		return fileStore.hooks.close(operation, file)
	}
	return file.Close()
}

func (fileStore *Store) doTruncate(file *os.File, size int64) error {
	if fileStore.hooks != nil && fileStore.hooks.truncate != nil {
		return fileStore.hooks.truncate(file, size)
	}
	return file.Truncate(size)
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
	raw, err := canonicaljson.MarshalWithLimits(state, frameJSONLimits())
	if err != nil {
		return diskState{}, err
	}
	var clone diskState
	if err := canonicaljson.UnmarshalStrictWithLimits(raw, &clone, frameJSONLimits()); err != nil {
		return diskState{}, err
	}
	return clone, nil
}

func frameJSONLimits() canonicaljson.Limits {
	limits := canonicaljson.DefaultLimits()
	limits.MaxInputBytes = maxFrameBytes
	limits.MaxCanonicalBytes = maxFrameBytes
	limits.MaxNodes = 4_000_000
	return limits
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

func (fileStore *Store) ensureOpen() error {
	if fileStore.closed {
		return os.ErrClosed
	}
	if fileStore.poisoned != nil {
		return fmt.Errorf("durable store write state is uncertain: %w", fileStore.poisoned)
	}
	return fileStore.verifyAuthority()
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
