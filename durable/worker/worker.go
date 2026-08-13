// Package worker drains leased durable intents without embedding product or
// node logic. Handlers explicitly choose complete, retry, dead, or leave; the
// worker never guesses that an uncertain external mutation is safe to retry.
package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/durable/store"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type Disposition string

const (
	Complete Disposition = "complete"
	Retry    Disposition = "retry"
	Dead     Disposition = "dead"
	Leave    Disposition = "leave"
)

type Result struct {
	Disposition Disposition
	Failure     *contracts.StructuredFailure
	AvailableAt *time.Time
}

type Handler interface {
	Handle(context.Context, store.ClaimedIntent) Result
}

type HandlerFunc func(context.Context, store.ClaimedIntent) Result

func (function HandlerFunc) Handle(ctx context.Context, intent store.ClaimedIntent) Result {
	return function(ctx, intent)
}

type Worker struct {
	Store    store.Store
	OwnerRef string
	Handlers map[contracts.DurableIntentKind]Handler
}

type Batch struct {
	Kinds          []contracts.DurableIntentKind
	LeaseToken     string
	Now            time.Time
	LeaseExpiresAt time.Time
	Limit          int
}

type BatchResult struct {
	Claimed   int
	Completed int
	Retried   int
	Dead      int
	Left      int
}

func (worker Worker) RunOnce(ctx context.Context, batch Batch) (BatchResult, error) {
	if worker.Store == nil || worker.OwnerRef == "" {
		return BatchResult{}, errors.New("durable worker store and owner are required")
	}
	claimed, err := worker.Store.ClaimIntents(ctx, store.ClaimRequest{
		Kinds: batch.Kinds, OwnerRef: worker.OwnerRef, LeaseToken: batch.LeaseToken,
		Now: batch.Now, LeaseExpiresAt: batch.LeaseExpiresAt, Limit: batch.Limit,
	})
	if err != nil {
		return BatchResult{}, err
	}
	result := BatchResult{Claimed: len(claimed)}
	var failures []error
	for _, intent := range claimed {
		if err := ctx.Err(); err != nil {
			result.Left++
			continue
		}
		handler := worker.Handlers[intent.Record.Intent.Kind]
		if handler == nil {
			result.Left++
			failures = append(failures, fmt.Errorf("intent %s: no handler for %s", intent.Record.Intent.Identity, intent.Record.Intent.Kind))
			continue
		}
		handled := handler.Handle(ctx, intent)
		fence := store.IntentFence{
			IntentID: intent.Record.Intent.Identity, ExpectedRevision: intent.Record.Revision,
			OwnerRef: worker.OwnerRef, LeaseToken: intent.LeaseToken, At: batch.Now,
		}
		switch handled.Disposition {
		case Complete:
			if handled.Failure != nil || handled.AvailableAt != nil {
				failures = append(failures, fmt.Errorf("intent %s: complete result contains failure or retry time", fence.IntentID))
				result.Left++
				continue
			}
			if _, err := worker.Store.CompleteIntent(ctx, fence); err != nil {
				failures = append(failures, fmt.Errorf("intent %s complete: %w", fence.IntentID, err))
				result.Left++
				continue
			}
			result.Completed++
		case Retry:
			if handled.Failure == nil || handled.Failure.Class != contracts.FailureTransient || handled.AvailableAt == nil {
				failures = append(failures, fmt.Errorf("intent %s: retry requires transient failure and available time", fence.IntentID))
				result.Left++
				continue
			}
			if _, err := worker.Store.FailIntent(ctx, store.IntentFailure{Fence: fence, Failure: *handled.Failure, AvailableAt: handled.AvailableAt}); err != nil {
				failures = append(failures, fmt.Errorf("intent %s retry: %w", fence.IntentID, err))
				result.Left++
				continue
			}
			result.Retried++
		case Dead:
			if handled.Failure == nil || handled.AvailableAt != nil {
				failures = append(failures, fmt.Errorf("intent %s: dead result requires failure and no retry time", fence.IntentID))
				result.Left++
				continue
			}
			if _, err := worker.Store.FailIntent(ctx, store.IntentFailure{Fence: fence, Failure: *handled.Failure, Dead: true}); err != nil {
				failures = append(failures, fmt.Errorf("intent %s dead: %w", fence.IntentID, err))
				result.Left++
				continue
			}
			result.Dead++
		case Leave:
			result.Left++
		default:
			result.Left++
			failures = append(failures, fmt.Errorf("intent %s: invalid handler disposition %q", fence.IntentID, handled.Disposition))
		}
	}
	return result, errors.Join(failures...)
}
