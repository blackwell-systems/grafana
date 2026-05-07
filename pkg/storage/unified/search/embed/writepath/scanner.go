// Package writepath keeps the vector index in sync with ongoing dashboard
// writes. A periodic scanner reads StorageBackend.ListModifiedSince since
// the vector_latest_rv checkpoint, embeds new/modified dashboards, deletes
// vectors for tombstones, and advances the checkpoint.
//
// The scanner is the sole writer of vector_latest_rv. The backfiller has
// its own state (vector_backfill_jobs) and may run concurrently; both
// produce idempotent upserts on the same key shape, so a brief overlap is
// harmless.
package writepath

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/storage/unified/resource"
	"github.com/grafana/grafana/pkg/storage/unified/resourcepb"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed/embedder"
	"github.com/grafana/grafana/pkg/storage/unified/search/vector"
)

// DefaultPollInterval is how long the scanner sleeps between cycles when
// there is no work or the lock is held by another replica.
const DefaultPollInterval = 30 * time.Second

type Options struct {
	Storage       resource.StorageBackend
	VectorBackend vector.VectorBackend
	BatchEmbedder *embedder.BatchEmbedder
	Builders      []embed.Builder
	Model         string
	PollInterval  time.Duration
	Log           log.Logger
}

// Scanner is the write-path indexer. One per process; coordinated across
// replicas via vector.VectorBackend.TryAcquireScannerLock.
type Scanner struct {
	storage       resource.StorageBackend
	vectorBackend vector.VectorBackend
	batchEmbedder *embedder.BatchEmbedder
	builders      map[string]embed.Builder // keyed by resource
	model         string
	pollInterval  time.Duration
	log           log.Logger
}

func New(opts Options) (*Scanner, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("writepath: Storage is required")
	}
	if opts.VectorBackend == nil {
		return nil, fmt.Errorf("writepath: VectorBackend is required")
	}
	if opts.BatchEmbedder == nil {
		return nil, fmt.Errorf("writepath: BatchEmbedder is required")
	}
	if len(opts.Builders) == 0 {
		return nil, fmt.Errorf("writepath: at least one Builder is required")
	}
	if opts.Model == "" {
		return nil, fmt.Errorf("writepath: Model is required")
	}
	builders := make(map[string]embed.Builder, len(opts.Builders))
	for _, b := range opts.Builders {
		if _, dup := builders[b.Resource()]; dup {
			return nil, fmt.Errorf("writepath: duplicate builder for resource %q", b.Resource())
		}
		builders[b.Resource()] = b
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = DefaultPollInterval
	}
	if opts.Log == nil {
		opts.Log = log.New("writepath")
	}
	return &Scanner{
		storage:       opts.Storage,
		vectorBackend: opts.VectorBackend,
		batchEmbedder: opts.BatchEmbedder,
		builders:      builders,
		model:         opts.Model,
		pollInterval:  opts.PollInterval,
		log:           opts.Log,
	}, nil
}

// Run loops until ctx is cancelled. Each iteration tries to acquire the
// advisory lock; if held by another replica the cycle is skipped and we
// sleep until the next tick.
func (s *Scanner) Run(ctx context.Context) error {
	t := time.NewTicker(s.pollInterval)
	defer t.Stop()

	// Run one cycle immediately so a freshly-started replica picks up
	// pending work without waiting for the first tick.
	s.runOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s.runOnce(ctx)
		}
	}
}

func (s *Scanner) runOnce(ctx context.Context) {
	logger := s.log.FromContext(ctx)
	release, acquired, err := s.vectorBackend.TryAcquireScannerLock(ctx)
	if err != nil {
		logger.Error("writepath: acquire lock", "err", err)
		return
	}
	if !acquired {
		logger.Debug("writepath: lock held elsewhere; skipping cycle")
		return
	}
	defer release()

	for _, b := range s.builders {
		if ctx.Err() != nil {
			return
		}
		s.scanBuilder(ctx, b)
	}
}

// scanBuilder runs one cross-namespace ListModifiedSince + process pass
// for the given builder and advances vector_latest_rv on success.
func (s *Scanner) scanBuilder(ctx context.Context, builder embed.Builder) {
	logger := s.log.FromContext(ctx).New("group", builder.Group(), "resource", builder.Resource())

	sinceRv, err := s.vectorBackend.GetLatestRV(ctx)
	if err != nil {
		logger.Error("writepath: read checkpoint", "err", err)
		return
	}
	// ListModifiedSince treats sinceRv == 0 as an error; bump to 1 so
	// the very first scan covers all history.
	effectiveSince := sinceRv
	if effectiveSince <= 0 {
		effectiveSince = 1
	}

	key := resource.NamespacedResource{
		Group:    builder.Group(),
		Resource: builder.Resource(),
		// Namespace intentionally empty — cross-namespace scan.
	}
	latestRv, seq := s.storage.ListModifiedSince(ctx, key, effectiveSince, nil)

	lowestFailedRv := int64(math.MaxInt64)
	processed := 0
	for mr, iterErr := range seq {
		if ctx.Err() != nil {
			return
		}
		if iterErr != nil {
			logger.Error("writepath: iterator error", "err", iterErr)
			// Treat as a global failure: don't advance the checkpoint at all.
			lowestFailedRv = effectiveSince
			break
		}
		if mr == nil {
			continue
		}
		if perr := s.processOne(ctx, builder, mr); perr != nil {
			logger.Warn("writepath: process item",
				"namespace", mr.Key.Namespace,
				"name", mr.Key.Name,
				"rv", mr.ResourceVersion,
				"err", perr)
			if mr.ResourceVersion < lowestFailedRv {
				lowestFailedRv = mr.ResourceVersion
			}
			continue
		}
		processed++
	}

	target := chooseTarget(sinceRv, latestRv, lowestFailedRv)
	if target > sinceRv {
		if err := s.vectorBackend.SetLatestRV(ctx, target); err != nil {
			logger.Error("writepath: advance checkpoint", "err", err, "target", target)
			return
		}
	}
	if processed > 0 || target > sinceRv {
		logger.Debug("writepath: cycle complete",
			"processed", processed, "from", sinceRv, "to", target)
	}
}

// chooseTarget picks the highest checkpoint we can safely advance to:
//   - no failures: latestRv (everything in the window is durable);
//   - some failures: lowestFailedRv - 1 so the failed item is retried on
//     the next cycle (and items at lower RVs aren't reprocessed forever).
//   - the lowest failure was at sinceRv+1 (nothing safely processed):
//     return sinceRv so we don't advance.
func chooseTarget(sinceRv, latestRv, lowestFailedRv int64) int64 {
	if lowestFailedRv == math.MaxInt64 {
		return latestRv
	}
	candidate := lowestFailedRv - 1
	if candidate < sinceRv {
		return sinceRv
	}
	return candidate
}

func (s *Scanner) processOne(ctx context.Context, builder embed.Builder, mr *resource.ModifiedResource) error {
	switch mr.Action {
	case resourcepb.WatchEvent_DELETED:
		// Drop every subresource for this dashboard.
		if err := s.vectorBackend.Delete(ctx, mr.Key.Namespace, s.model, builder.Resource(), mr.Key.Name); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
		return nil
	case resourcepb.WatchEvent_ADDED, resourcepb.WatchEvent_MODIFIED:
		return s.embedAndUpsert(ctx, builder, mr)
	default:
		return fmt.Errorf("unknown action %v", mr.Action)
	}
}

func (s *Scanner) embedAndUpsert(ctx context.Context, builder embed.Builder, mr *resource.ModifiedResource) error {
	if len(mr.Value) == 0 {
		// Empty payload on a non-delete event is treated as nothing to embed.
		return nil
	}
	key := &resourcepb.ResourceKey{
		Group:     builder.Group(),
		Resource:  builder.Resource(),
		Namespace: mr.Key.Namespace,
		Name:      mr.Key.Name,
	}
	items, err := builder.Extract(ctx, key, mr.Value, "")
	if err != nil {
		return fmt.Errorf("extract: %w", err)
	}
	if maxItems := builder.MaxItemsPerResource(); maxItems > 0 && len(items) > maxItems {
		items = items[:maxItems]
	}

	// Drop any panel embeddings that are no longer present. We do this
	// before upsert so partial failure leaves the dashboard with the old
	// embeddings rather than orphan rows.
	if err := s.cleanupStaleSubresources(ctx, builder, mr.Key.Namespace, mr.Key.Name, items); err != nil {
		return fmt.Errorf("cleanup stale subresources: %w", err)
	}

	if len(items) == 0 {
		return nil
	}

	vectors, err := s.batchEmbedder.Embed(ctx, mr.Key.Namespace, builder.Resource(), mr.ResourceVersion, items)
	if err != nil {
		return fmt.Errorf("embed: %w", err)
	}
	if len(vectors) == 0 {
		return nil
	}
	if err := s.vectorBackend.Upsert(ctx, vectors); err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	return nil
}

// cleanupStaleSubresources deletes any stored subresource embeddings whose
// keys aren't represented in the latest extract. A panel that was removed
// from the dashboard would otherwise stick around in search results.
func (s *Scanner) cleanupStaleSubresources(ctx context.Context, builder embed.Builder, namespace, uid string, items []embed.Item) error {
	stored, err := s.vectorBackend.GetSubresourceContent(ctx, namespace, s.model, builder.Resource(), uid)
	if err != nil {
		return err
	}
	if len(stored) == 0 {
		return nil
	}
	keep := make(map[string]struct{}, len(items))
	for _, it := range items {
		keep[it.Subresource] = struct{}{}
	}
	var stale []string
	for sub := range stored {
		if _, ok := keep[sub]; !ok {
			stale = append(stale, sub)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	if err := s.vectorBackend.DeleteSubresources(ctx, namespace, s.model, builder.Resource(), uid, stale); err != nil {
		return fmt.Errorf("delete stale: %w", err)
	}
	return nil
}

