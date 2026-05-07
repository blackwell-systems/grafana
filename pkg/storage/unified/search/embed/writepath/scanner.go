// Package writepath keeps the vector index in sync with ongoing dashboard
// writes. The scanner combines two signals into a single in-memory queue
// keyed by (group, resource, namespace, name) and dedup'd by RV: an event
// is only kept if its RV is strictly greater than whatever the queue
// already holds for that resource. Older RVs are silently dropped at
// enqueue time, which removes the "older event overwrites newer" race
// when bootstrap and watch both surface the same dashboard.
//
// Two producers feed the queue:
//
//  1. WatchWriteEvents — a long-lived subscription that streams every
//     dashboard write across the cluster. The event payload (its Value)
//     is enqueued directly so the cycle never has to fetch it again.
//
//  2. Bootstrap (at startup) — uses the NamespaceLister capability to
//     enumerate namespaces with activity past vector_latest_rv, then
//     calls ListModifiedSince per namespace and enqueues each event.
//     Catches anything that committed while the process was down and
//     isn't replayed by the new watch subscription.
//
// Each cycle drains the queue, drops events whose RV ≤ checkpoint
// (cursor-level dedup of replayed history), executes deletes inline,
// and pools every panel that needs embedding into one EmbedText call
// followed by one Upsert. Provider-side chunking (e.g. Vertex's 250-text
// limit) lives inside EmbedText.
//
// On any per-cycle failure the affected events are re-enqueued so the
// next cycle retries them. The scanner is the sole writer of
// vector_latest_rv. The backfiller has its own state
// (vector_backfill_jobs) and may run concurrently; both produce
// idempotent upserts on the same key shape, so brief overlap is
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

// NamespaceLister is the capability backends must implement so the
// scanner can bootstrap without walking every namespace. Both the SQL
// and KV backends implement it; the scanner refuses to recover missed
// writes when a backend doesn't.
type NamespaceLister interface {
	ListNamespacesModifiedSince(ctx context.Context, group, resource string, sinceRv int64) ([]string, error)
}

// pendingEvent is one queued change waiting to be embedded/upserted/
// deleted. Fields are flattened (rather than holding a *ResourceKey)
// because resourcepb.ResourceKey embeds protoimpl.MessageState which
// wraps a sync.Mutex — copying it triggers go vet's `copylocks`
// warning. We don't need the protobuf shape for in-memory queueing.
type pendingEvent struct {
	action    resourcepb.WatchEvent_Type
	group     string
	resource  string
	namespace string
	name      string
	value     []byte // payload for upserts; nil/empty for deletes
	rv        int64
}

// eventQueueKey is the dedup key — one pending event per resource at a
// time, regardless of how many writes it received.
func eventQueueKey(group, resource, namespace, name string) string {
	return group + "/" + resource + "/" + namespace + "/" + name
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

	// queue holds at most one pendingEvent per resource. Enqueue keeps
	// the highest RV; the cycle drains it under lock, processes, and
	// re-enqueues anything that didn't make it.
	queueMu sync.Mutex
	queue   map[string]*pendingEvent
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
		queue:         make(map[string]*pendingEvent),
	}, nil
}

// enqueue adds an event to the queue if its RV is strictly greater than
// any pending event for the same resource. Older RVs are silently
// dropped — the dedup is the whole point of using a map keyed by
// resource identity. Events for resources without a registered builder
// or with an empty namespace (cluster-scoped) are ignored.
func (s *Scanner) enqueue(ev *pendingEvent) {
	if ev == nil || ev.namespace == "" {
		return
	}
	builder, ok := s.builders[ev.resource]
	if !ok || builder.Group() != ev.group {
		return
	}
	k := eventQueueKey(ev.group, ev.resource, ev.namespace, ev.name)
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if existing, ok := s.queue[k]; ok && existing.rv >= ev.rv {
		return
	}
	s.queue[k] = ev
}

// drainQueue returns and clears every pending event. Events that arrive
// after this call go into the next cycle; per-cycle failures re-enqueue
// specific events.
func (s *Scanner) drainQueue() []*pendingEvent {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if len(s.queue) == 0 {
		return nil
	}
	out := make([]*pendingEvent, 0, len(s.queue))
	for _, ev := range s.queue {
		out = append(out, ev)
	}
	s.queue = make(map[string]*pendingEvent)
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

// consumeWatchEvents enqueues every dashboard write the watch surfaces.
// Events for unsupported groups/resources are dropped; events with an
// older RV than what we already have queued for the same resource are
// dropped at enqueue time.
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
			s.enqueue(&pendingEvent{
				action:    ev.Type,
				group:     ev.Key.Group,
				resource:  ev.Key.Resource,
				namespace: ev.Key.Namespace,
				name:      ev.Key.Name,
				value:     ev.Value,
				rv:        ev.ResourceVersion,
			})
		}
	}
}

// bootstrap discovers events committed past the current checkpoint and
// enqueues them. Discovery is two-phase per builder: NamespaceLister
// returns the active namespaces, then ListModifiedSince(ns, sinceRv)
// gives us each event with its payload.
func (s *Scanner) bootstrap(ctx context.Context) {
	logger := s.log.FromContext(ctx)
	sinceRv, err := s.vectorBackend.GetLatestRV(ctx)
	if err != nil {
		logger.Error("writepath: bootstrap read checkpoint", "err", err)
		return
	}
	effective := sinceRv
	if effective <= 0 {
		effective = 1
	}
	for _, b := range s.builders {
		s.bootstrapBuilder(ctx, b, effective, logger)
	}
}

func (s *Scanner) bootstrapBuilder(ctx context.Context, builder embed.Builder, sinceRv int64, logger log.Logger) {
	nl, ok := s.storage.(NamespaceLister)
	if !ok {
		logger.Warn("writepath: storage doesn't implement NamespaceLister; cannot recover missed writes",
			"group", builder.Group(), "resource", builder.Resource())
		return
	}
	nss, err := nl.ListNamespacesModifiedSince(ctx, builder.Group(), builder.Resource(), sinceRv)
	if err != nil {
		logger.Error("writepath: NamespaceLister failed",
			"group", builder.Group(), "resource", builder.Resource(), "err", err)
		return
	}
	for _, ns := range nss {
		if ctx.Err() != nil {
			return
		}
		s.bootstrapNamespace(ctx, builder, ns, sinceRv, logger)
	}
}

// bootstrapNamespace pulls every event past sinceRv for one namespace
// and enqueues it. ListModifiedSince already collapses repeated writes
// to the same resource down to the latest RV, but the queue's
// RV-keyed dedup makes that a defence-in-depth.
func (s *Scanner) bootstrapNamespace(ctx context.Context, builder embed.Builder, namespace string, sinceRv int64, logger log.Logger) {
	key := resource.NamespacedResource{
		Namespace: namespace,
		Group:     builder.Group(),
		Resource:  builder.Resource(),
	}
	_, seq := s.storage.ListModifiedSince(ctx, key, sinceRv, nil)
	for mr, err := range seq {
		if err != nil {
			logger.Warn("writepath: bootstrap iterator error",
				"namespace", namespace, "err", err)
			return
		}
		if mr == nil {
			continue
		}
		s.enqueue(&pendingEvent{
			action:    mr.Action,
			group:     mr.Key.Group,
			resource:  mr.Key.Resource,
			namespace: mr.Key.Namespace,
			name:      mr.Key.Name,
			value:     mr.Value,
			rv:        mr.ResourceVersion,
		})
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

	s.processQueue(ctx)
}

// pendingEmbed pairs a partially-built vector with the text that still
// needs an embedding. Pooling these across resources lets the scanner
// make a single EmbedText call per cycle even when many dashboards
// changed across different tenants.
type pendingEmbed struct {
	proto vector.Vector // every field set except Embedding
	text  string
}

// processQueue drains the pending-event queue, executes deletes inline,
// pools every panel that needs embedding into one EmbedText/Upsert
// pair, and advances vector_latest_rv based on the global outcome.
// Events whose RV is already past vector_latest_rv (replayed history)
// are dropped at the cursor check. On any failure, the affected events
// are re-enqueued so the next cycle retries them.
func (s *Scanner) processQueue(ctx context.Context) {
	logger := s.log.FromContext(ctx)

	pending := s.drainQueue()
	if len(pending) == 0 {
		return
	}

	sinceRv, err := s.vectorBackend.GetLatestRV(ctx)
	if err != nil {
		logger.Error("writepath: read checkpoint", "err", err)
		s.requeue(pending)
		return
	}

	// Track per-event outcome so we know what to re-enqueue. The lowest
	// failed RV pins the global advance below it, ensuring failed work
	// is retried before the cursor moves past.
	var (
		pooled         []pendingEmbed
		pooledOwners   []*pendingEvent // 1:1 alignment is impossible (multi-panel), so we re-enqueue source events on pooled failure separately
		failed         []*pendingEvent
		successes      []*pendingEvent
		lowestFailedRv = int64(math.MaxInt64)
		maxRv          = sinceRv
	)
	_ = pooledOwners // see comment below; we track owners as a slice of source events

	// Source events that contributed to `pooled` so we can re-enqueue
	// them on pooled failure.
	var pooledSources []*pendingEvent

	for _, ev := range pending {
		if ctx.Err() != nil {
			s.requeue(pending)
			return
		}
		// Cursor-level dedup: anything ≤ checkpoint was already
		// processed by a prior cycle (or is replayed history from the
		// watch). Drop it without touching state.
		if ev.rv <= sinceRv {
			continue
		}
		builder, ok := s.builders[ev.resource]
		if !ok {
			continue
		}

		switch ev.action {
		case resourcepb.WatchEvent_DELETED:
			if err := s.vectorBackend.Delete(ctx, ev.namespace, s.embedder.Model, builder.Resource(), ev.name); err != nil {
				logger.Warn("writepath: delete vector",
					"namespace", ev.namespace, "name", ev.name,
					"rv", ev.rv, "err", err)
				failed = append(failed, ev)
				if ev.rv < lowestFailedRv {
					lowestFailedRv = ev.rv
				}
				continue
			}
			successes = append(successes, ev)

		case resourcepb.WatchEvent_ADDED, resourcepb.WatchEvent_MODIFIED:
			added, err := s.collectUpsertEvent(ctx, builder, ev, &pooled)
			if err != nil {
				logger.Warn("writepath: collect event",
					"namespace", ev.namespace, "name", ev.name,
					"rv", ev.rv, "err", err)
				failed = append(failed, ev)
				if ev.rv < lowestFailedRv {
					lowestFailedRv = ev.rv
				}
				continue
			}
			if added {
				pooledSources = append(pooledSources, ev)
			} else {
				// Empty extract / no-content: nothing to embed; treat as success.
				successes = append(successes, ev)
			}

		default:
			logger.Warn("writepath: unknown action",
				"namespace", ev.namespace, "name", ev.name,
				"rv", ev.rv, "action", ev.action)
			failed = append(failed, ev)
			if ev.rv < lowestFailedRv {
				lowestFailedRv = ev.rv
			}
			continue
		}
		if ev.rv > maxRv {
			maxRv = ev.rv
		}
	}

	// Single pooled embed + upsert. Failure here invalidates every
	// source event that contributed; re-enqueue the lot.
	if len(pooled) > 0 {
		if err := s.embedAndUpsertPooled(ctx, pooled); err != nil {
			logger.Error("writepath: pooled embed/upsert",
				"items", len(pooled), "sources", len(pooledSources), "err", err)
			for _, ev := range pooledSources {
				failed = append(failed, ev)
				if ev.rv < lowestFailedRv {
					lowestFailedRv = ev.rv
				}
			}
		} else {
			successes = append(successes, pooledSources...)
		}
	}

	target := chooseTarget(sinceRv, maxRv, lowestFailedRv)
	if target > sinceRv {
		if err := s.vectorBackend.SetLatestRV(ctx, target); err != nil {
			logger.Error("writepath: advance checkpoint", "err", err, "target", target)
			// Cursor write failed; we can't tell what's persisted.
			// Re-enqueue everything we drained so nothing is dropped.
			s.requeue(pending)
			return
		}
	}

	// Re-enqueue events that didn't make the cut. The cursor advance
	// guarantees `target ≥ ev.rv` for every successful event, so
	// re-queueing failures alone is sufficient — the cursor filter on
	// the next cycle handles everything else.
	for _, ev := range failed {
		s.enqueue(ev)
	}

	if len(successes) > 0 || target > sinceRv {
		logger.Debug("writepath: cycle complete",
			"drained", len(pending),
			"succeeded", len(successes),
			"failed", len(failed),
			"pooled_items", len(pooled),
			"from", sinceRv, "to", target)
	}
}

// requeue puts every event back. Used when we can't tell what's
// persisted (e.g. cursor write failed) and have to assume the worst.
func (s *Scanner) requeue(events []*pendingEvent) {
	for _, ev := range events {
		s.enqueue(ev)
	}
}

// collectUpsertEvent extracts items from one event's payload, runs
// stale-subresource cleanup, and appends every item with content into
// `pooled`. Returns true when at least one item was queued for
// embedding.
func (s *Scanner) collectUpsertEvent(ctx context.Context, builder embed.Builder, ev *pendingEvent, pooled *[]pendingEmbed) (bool, error) {
	if len(ev.value) == 0 {
		return false, nil
	}
	key := &resourcepb.ResourceKey{
		Group:     builder.Group(),
		Resource:  builder.Resource(),
		Namespace: ev.namespace,
		Name:      ev.name,
	}
	items, err := builder.Extract(ctx, key, ev.value, "")
	if err != nil {
		return false, fmt.Errorf("extract: %w", err)
	}
	if maxItems := builder.MaxItemsPerResource(); maxItems > 0 && len(items) > maxItems {
		items = items[:maxItems]
	}

	// Drop any panel embeddings that are no longer present. We do this
	// inline (per dashboard, before the pooled phase) so each
	// dashboard's stale rows are gone before its fresh embeddings land.
	// Partial cycle failure leaves the dashboard in a self-consistent
	// state.
	if err := s.cleanupStaleSubresources(ctx, builder, ev.namespace, ev.name, items); err != nil {
		return false, fmt.Errorf("cleanup stale subresources: %w", err)
	}

	added := false
	for _, it := range items {
		if it.Content == "" {
			continue
		}
		*pooled = append(*pooled, pendingEmbed{
			proto: vector.Vector{
				Namespace:       ev.namespace,
				Resource:        builder.Resource(),
				UID:             it.UID,
				Title:           it.Title,
				Subresource:     it.Subresource,
				ResourceVersion: ev.rv,
				Folder:          it.Folder,
				Content:         it.Content,
				Metadata:        it.Metadata,
				Model:           s.embedder.Model,
			},
			text: it.Content,
		})
		added = true
	}
	return added, nil
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
