// Package writepath keeps the vector index in sync with ongoing dashboard
// writes. The scanner combines two signals:
//
//  1. WatchWriteEvents — a long-lived subscription that streams every
//     dashboard write across the cluster. Each event adds the affected
//     namespace to a "needs scan" set; the watch is purely a hint, never
//     the source of truth for the embedded value.
//
//  2. ListModifiedSince per namespace — the cycle, every PollInterval,
//     drains the set, calls ListModifiedSince(ns, vector_latest_rv) for
//     each, dedups events whose RV ≤ checkpoint, and upserts the rest.
//     The cursor is the only dedup mechanism: replayed watch events for
//     already-processed RVs are filtered by the SQL query for free.
//
// At startup the scanner bootstraps the set so anything that committed
// while the process was down (and isn't replayed by the watch) still
// gets picked up. Discovery prefers the cheap NamespaceLister capability
// (SQL backend), falling back to GetResourceStats for backends that
// don't implement it.
//
// All embedding work for a cycle is pooled: one EmbedText call covers
// every panel from every flagged namespace, then one Upsert writes them
// in a single transaction. Provider-side chunking (e.g. Vertex's 250-text
// limit) lives inside EmbedText.
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
	"sync"
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

// NamespaceLister is an optional capability backends may implement to
// answer "which namespaces have any change since RV X?" cheaply. Used
// at scanner bootstrap so we don't have to walk every namespace via
// GetResourceStats. Backends that don't implement it fall back to
// GetResourceStats — the scanner still works, just less efficiently on
// first start.
type NamespaceLister interface {
	ListNamespacesModifiedSince(ctx context.Context, group, resource string, sinceRv int64) ([]string, error)
}

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

	// nsToScan holds namespaces flagged by the watch loop or bootstrap.
	// The cycle drains it, runs ListModifiedSince per namespace, and
	// re-adds entries that didn't fully advance.
	mu       sync.Mutex
	nsToScan map[string]struct{}
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
		nsToScan:      make(map[string]struct{}),
	}, nil
}

// flagNamespace marks a namespace for scanning. Empty strings are
// dropped — they would mean a cluster-scoped event, which dashboards
// don't produce.
func (s *Scanner) flagNamespace(ns string) {
	if ns == "" {
		return
	}
	s.mu.Lock()
	s.nsToScan[ns] = struct{}{}
	s.mu.Unlock()
}

// drainNamespaces returns and clears the current set. Watch events that
// arrive after this call go into the next cycle; failures during the
// current cycle re-add specific namespaces via flagNamespace.
func (s *Scanner) drainNamespaces() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.nsToScan) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.nsToScan))
	for ns := range s.nsToScan {
		out = append(out, ns)
	}
	s.nsToScan = make(map[string]struct{})
	return out
}

// Run subscribes to write events, bootstraps the namespace set, then
// runs the periodic drain-and-scan loop until ctx is cancelled.
func (s *Scanner) Run(ctx context.Context) error {
	logger := s.log.FromContext(ctx)

	// Subscribe early so events flagged during bootstrap aren't lost.
	ch, err := s.storage.WatchWriteEvents(ctx)
	if err != nil {
		logger.Error("writepath: subscribe to write events", "err", err)
		// Watch failure isn't fatal; the periodic poll alone still works
		// (with the bootstrap path repeating each cycle, which is wasteful
		// but correct). Carry on without the watch.
	} else {
		go s.consumeWatchEvents(ctx, ch)
	}

	// Bootstrap fills nsToScan with everything that has activity past the
	// checkpoint, covering writes that committed during downtime / before
	// the watch became active.
	s.bootstrap(ctx)

	t := time.NewTicker(s.pollInterval)
	defer t.Stop()

	// First cycle runs immediately so a freshly-started replica picks up
	// bootstrap work without waiting for the tick.
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

// consumeWatchEvents flags namespaces of dashboard writes for the next
// cycle. Events for unsupported groups/resources are ignored; events
// whose RV is already past vector_latest_rv get filtered cheaply by
// ListModifiedSince when the cycle runs, so we don't dedup here.
func (s *Scanner) consumeWatchEvents(ctx context.Context, ch <-chan *resource.WrittenEvent) {
	logger := s.log.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				logger.Warn("writepath: watch channel closed")
				return
			}
			if ev == nil || ev.Key == nil {
				continue
			}
			if _, ok := s.builders[ev.Key.Resource]; !ok {
				continue
			}
			s.flagNamespace(ev.Key.Namespace)
		}
	}
}

// bootstrap discovers namespaces with activity past the current
// checkpoint and flags them. Prefers the cheap NamespaceLister
// capability; falls back to GetResourceStats.
func (s *Scanner) bootstrap(ctx context.Context) {
	logger := s.log.FromContext(ctx)
	sinceRv, err := s.vectorBackend.GetLatestRV(ctx)
	if err != nil {
		logger.Error("writepath: bootstrap read checkpoint", "err", err)
		return
	}
	for _, b := range s.builders {
		s.bootstrapBuilder(ctx, b, sinceRv, logger)
	}
}

func (s *Scanner) bootstrapBuilder(ctx context.Context, builder embed.Builder, sinceRv int64, logger log.Logger) {
	if nl, ok := s.storage.(NamespaceLister); ok {
		nss, err := nl.ListNamespacesModifiedSince(ctx, builder.Group(), builder.Resource(), sinceRv)
		if err == nil {
			for _, ns := range nss {
				s.flagNamespace(ns)
			}
			return
		}
		// Fall through to GetResourceStats on error so a misconfigured
		// optimization path doesn't break boot.
		logger.Warn("writepath: NamespaceLister failed, falling back to GetResourceStats",
			"group", builder.Group(), "resource", builder.Resource(), "err", err)
	}
	stats, err := s.storage.GetResourceStats(ctx, resource.NamespacedResource{
		Group:    builder.Group(),
		Resource: builder.Resource(),
	}, 0)
	if err != nil {
		logger.Error("writepath: bootstrap GetResourceStats", "err", err)
		return
	}
	for _, st := range stats {
		s.flagNamespace(st.Namespace)
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

// scanBuilder drains the namespace set, fans out per-namespace
// ListModifiedSince calls, pools every panel that needs embedding into
// one call, and advances vector_latest_rv based on the global outcome.
// Deletes execute inline because they don't need the embedder. On
// failure (any namespace partially processed, or pooled embed/upsert
// errored), namespaces with unfinished work are re-flagged so the next
// cycle retries them.
func (s *Scanner) scanBuilder(ctx context.Context, builder embed.Builder) {
	logger := s.log.FromContext(ctx).New("group", builder.Group(), "resource", builder.Resource())

	namespaces := s.drainNamespaces()
	if len(namespaces) == 0 {
		return
	}

	sinceRv, err := s.vectorBackend.GetLatestRV(ctx)
	if err != nil {
		logger.Error("writepath: read checkpoint", "err", err)
		// Re-flag so a transient checkpoint read failure doesn't drop work.
		for _, ns := range namespaces {
			s.flagNamespace(ns)
		}
		return
	}
	// ListModifiedSince treats sinceRv == 0 as an error; bump to 1 so
	// the very first scan covers all history.
	effectiveSince := sinceRv
	if effectiveSince <= 0 {
		effectiveSince = 1
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
	for _, ns := range namespaces {
		if ctx.Err() != nil {
			// Re-flag remaining work so we resume next cycle.
			for _, ns := range namespaces {
				s.flagNamespace(ns)
			}
			return
		}
		nsLatest, nsProcessed, nsLowestFailed := s.collectNamespace(ctx, builder, ns, effectiveSince, &pending, logger)
		if nsLatest > maxLatestRv {
			maxLatestRv = nsLatest
		}
		if nsLowestFailed < lowestFailedRv {
			lowestFailedRv = nsLowestFailed
		}
		processed += nsProcessed
	}

	pooledFailed := false
	if len(pending) > 0 {
		if err := s.embedAndUpsertPooled(ctx, pending); err != nil {
			pooledFailed = true
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
			// Couldn't persist the cursor; re-flag so we don't lose work.
			for _, ns := range namespaces {
				s.flagNamespace(ns)
			}
			return
		}
	}

	// Re-flag namespaces that didn't fully advance. With pooled all-or-
	// nothing semantics, that means "everything we processed" on any
	// failure, since we can't tell which namespace owned the failing item.
	if pooledFailed || lowestFailedRv != math.MaxInt64 {
		for _, ns := range namespaces {
			s.flagNamespace(ns)
		}
	}

	if processed > 0 || target > sinceRv {
		logger.Debug("writepath: cycle complete",
			"namespaces", len(namespaces),
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

