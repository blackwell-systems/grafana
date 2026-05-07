// Package writepath keeps the vector index in sync with ongoing dashboard
// writes. A periodic scanner enumerates active namespaces via
// GetResourceStats, reads StorageBackend.ListModifiedSince per namespace
// since the vector_latest_rv checkpoint, and aggregates every panel that
// needs embedding into a single pooled EmbedText call followed by a
// single Upsert. Provider-side chunking (e.g. Vertex's 250-text limit)
// happens inside EmbedText, so the scanner doesn't need to know about it.
//
// The per-namespace fan-out is intentional: ListModifiedSince's existing
// contract requires a non-empty namespace, so the scanner does the
// fan-out itself rather than push cross-namespace semantics into the
// storage backend. With a 30s cycle and a few-thousand namespaces ceiling
// at Grafana scale, the extra round-trips per cycle are cheap. If that
// stops being true, swap to a single cross-namespace ListModifiedSince
// (see commit history for an earlier draft).
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
	Embedder      *embedder.Embedder
	Builders      []embed.Builder
	PollInterval  time.Duration
	Log           log.Logger
}

// Scanner is the write-path indexer. One per process; coordinated across
// replicas via vector.VectorBackend.TryAcquireScannerLock.
type Scanner struct {
	storage       resource.StorageBackend
	vectorBackend vector.VectorBackend
	embedder      *embedder.Embedder
	builders      map[string]embed.Builder // keyed by resource
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
	if opts.Embedder == nil {
		return nil, fmt.Errorf("writepath: Embedder is required")
	}
	if opts.Embedder.Model == "" {
		return nil, fmt.Errorf("writepath: Embedder.Model is required")
	}
	if len(opts.Builders) == 0 {
		return nil, fmt.Errorf("writepath: at least one Builder is required")
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
		embedder:      opts.Embedder,
		builders:      builders,
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

// pendingEmbed pairs a partially-built vector with the text that still
// needs an embedding. Pooling these across all namespaces lets the
// scanner make a single EmbedText call per cycle even when many
// dashboards changed in different tenants.
type pendingEmbed struct {
	proto vector.Vector // every field set except Embedding
	text  string
}

// scanBuilder enumerates active namespaces, pools every panel that
// needs embedding into one call, and advances vector_latest_rv based on
// the global outcome. Deletes execute inline because they don't need
// the embedder.
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

	stats, err := s.storage.GetResourceStats(ctx, resource.NamespacedResource{
		Group:    builder.Group(),
		Resource: builder.Resource(),
		// Namespace empty = enumerate all.
	}, 0)
	if err != nil {
		logger.Error("writepath: enumerate namespaces", "err", err)
		return
	}

	// Aggregate across namespaces. vector_latest_rv is a single global
	// cursor and storage RVs are globally monotonic, so we collapse
	// per-namespace progress into one (sinceRv, latestRv, lowestFailedRv)
	// triple and feed it to chooseTarget once.
	var (
		pending        []pendingEmbed
		lowestFailedRv = int64(math.MaxInt64)
		maxLatestRv    int64
		processed      int // counts inline-processed work (deletes); pooled embeds counted later
	)
	for _, st := range stats {
		if ctx.Err() != nil {
			return
		}
		if st.Namespace == "" {
			continue
		}
		nsLatest, nsProcessed, nsLowestFailed := s.collectNamespace(ctx, builder, st.Namespace, effectiveSince, &pending, logger)
		if nsLatest > maxLatestRv {
			maxLatestRv = nsLatest
		}
		if nsLowestFailed < lowestFailedRv {
			lowestFailedRv = nsLowestFailed
		}
		processed += nsProcessed
	}

	if len(pending) > 0 {
		if err := s.embedAndUpsertPooled(ctx, pending); err != nil {
			logger.Error("writepath: pooled embed/upsert",
				"items", len(pending), "err", err)
			// Treat the whole batch as failed: pin lowestFailedRv to the
			// minimum RV among pending items so we retry the whole window.
			for _, p := range pending {
				if p.proto.ResourceVersion < lowestFailedRv {
					lowestFailedRv = p.proto.ResourceVersion
				}
			}
		} else {
			processed += len(pending)
		}
	}

	target := chooseTarget(sinceRv, maxLatestRv, lowestFailedRv)
	if target > sinceRv {
		if err := s.vectorBackend.SetLatestRV(ctx, target); err != nil {
			logger.Error("writepath: advance checkpoint", "err", err, "target", target)
			return
		}
	}
	if processed > 0 || target > sinceRv {
		logger.Debug("writepath: cycle complete",
			"namespaces", len(stats),
			"processed", processed,
			"pooled_items", len(pending),
			"from", sinceRv, "to", target)
	}
}

// collectNamespace processes one namespace's slice of changes. Deletes
// execute inline; updates queue items into `pending` for the pooled
// embed step. Returns the latestRv reported by the backend, the count
// of inline-completed items (deletes only), and the lowest RV that
// failed inline (math.MaxInt64 if none).
func (s *Scanner) collectNamespace(ctx context.Context, builder embed.Builder, namespace string, sinceRv int64, pending *[]pendingEmbed, logger log.Logger) (int64, int, int64) {
	key := resource.NamespacedResource{
		Namespace: namespace,
		Group:     builder.Group(),
		Resource:  builder.Resource(),
	}
	latestRv, seq := s.storage.ListModifiedSince(ctx, key, sinceRv, nil)

	lowestFailedRv := int64(math.MaxInt64)
	processed := 0
	for mr, iterErr := range seq {
		if ctx.Err() != nil {
			return latestRv, processed, lowestFailedRv
		}
		if iterErr != nil {
			logger.Error("writepath: iterator error", "namespace", namespace, "err", iterErr)
			// Treat the whole namespace as failed at sinceRv so the
			// global advance stays put.
			return latestRv, processed, sinceRv
		}
		if mr == nil {
			continue
		}
		switch mr.Action {
		case resourcepb.WatchEvent_DELETED:
			if err := s.vectorBackend.Delete(ctx, mr.Key.Namespace, s.embedder.Model, builder.Resource(), mr.Key.Name); err != nil {
				logger.Warn("writepath: delete vector",
					"namespace", mr.Key.Namespace, "name", mr.Key.Name,
					"rv", mr.ResourceVersion, "err", err)
				if mr.ResourceVersion < lowestFailedRv {
					lowestFailedRv = mr.ResourceVersion
				}
				continue
			}
			processed++
		case resourcepb.WatchEvent_ADDED, resourcepb.WatchEvent_MODIFIED:
			if err := s.collectForUpsert(ctx, builder, mr, pending); err != nil {
				logger.Warn("writepath: collect item",
					"namespace", mr.Key.Namespace, "name", mr.Key.Name,
					"rv", mr.ResourceVersion, "err", err)
				if mr.ResourceVersion < lowestFailedRv {
					lowestFailedRv = mr.ResourceVersion
				}
			}
		default:
			logger.Warn("writepath: unknown action",
				"namespace", mr.Key.Namespace, "name", mr.Key.Name,
				"rv", mr.ResourceVersion, "action", mr.Action)
			if mr.ResourceVersion < lowestFailedRv {
				lowestFailedRv = mr.ResourceVersion
			}
		}
	}
	return latestRv, processed, lowestFailedRv
}

// collectForUpsert extracts items from a single dashboard, runs
// stale-subresource cleanup immediately, and queues each item with its
// owning resource metadata for the pooled embed step.
func (s *Scanner) collectForUpsert(ctx context.Context, builder embed.Builder, mr *resource.ModifiedResource, pending *[]pendingEmbed) error {
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
	// inline (not in the pooled phase) so each dashboard's stale rows
	// are gone before its fresh embeddings land — partial cycle failure
	// then leaves the dashboard in a self-consistent state.
	if err := s.cleanupStaleSubresources(ctx, builder, mr.Key.Namespace, mr.Key.Name, items); err != nil {
		return fmt.Errorf("cleanup stale subresources: %w", err)
	}

	for _, it := range items {
		if it.Content == "" {
			continue
		}
		*pending = append(*pending, pendingEmbed{
			proto: vector.Vector{
				Namespace:       mr.Key.Namespace,
				Resource:        builder.Resource(),
				UID:             it.UID,
				Title:           it.Title,
				Subresource:     it.Subresource,
				ResourceVersion: mr.ResourceVersion,
				Folder:          it.Folder,
				Content:         it.Content,
				Metadata:        it.Metadata,
				Model:           s.embedder.Model,
			},
			text: it.Content,
		})
	}
	return nil
}

// embedAndUpsertPooled submits every queued text in one EmbedText call
// (the provider chunks internally to fit its per-call limit) and writes
// every resulting vector in a single Upsert transaction. Failure is
// all-or-nothing for the cycle: caller marks the whole batch as failed.
func (s *Scanner) embedAndUpsertPooled(ctx context.Context, pending []pendingEmbed) error {
	texts := make([]string, len(pending))
	for i, p := range pending {
		texts[i] = p.text
	}
	out, err := s.embedder.EmbedText(ctx, embedder.EmbedTextInput{
		Texts:     texts,
		Normalize: s.embedder.ShouldNormalize(),
		Task:      embedder.TaskRetrievalDocument,
	})
	if err != nil {
		return fmt.Errorf("embed: %w", err)
	}
	if len(out.Embeddings) != len(pending) {
		return fmt.Errorf("embed returned %d embeddings for %d texts", len(out.Embeddings), len(pending))
	}
	vectors := make([]vector.Vector, len(pending))
	for i, p := range pending {
		v := p.proto
		v.Embedding = out.Embeddings[i].Dense
		vectors[i] = v
	}
	if err := s.vectorBackend.Upsert(ctx, vectors); err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	return nil
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

// cleanupStaleSubresources deletes any stored subresource embeddings whose
// keys aren't represented in the latest extract. A panel that was removed
// from the dashboard would otherwise stick around in search results.
func (s *Scanner) cleanupStaleSubresources(ctx context.Context, builder embed.Builder, namespace, uid string, items []embed.Item) error {
	stored, err := s.vectorBackend.GetSubresourceContent(ctx, namespace, s.embedder.Model, builder.Resource(), uid)
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
	if err := s.vectorBackend.DeleteSubresources(ctx, namespace, s.embedder.Model, builder.Resource(), uid, stale); err != nil {
		return fmt.Errorf("delete stale: %w", err)
	}
	return nil
}

