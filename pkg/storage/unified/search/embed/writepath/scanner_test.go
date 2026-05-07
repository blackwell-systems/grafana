package writepath

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/grafana/pkg/storage/unified/resource"
	"github.com/grafana/grafana/pkg/storage/unified/resourcepb"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed/dashboard"
	"github.com/grafana/grafana/pkg/storage/unified/search/vector"
)

const dashGroup = "dashboard.grafana.app"
const dashRes = "dashboards"
const testModel = "test-model"

// minimalDashboard returns a single-panel dashboard payload that the
// dashboard extractor will turn into one embed.Item.
func minimalDashboard(uid, title string) []byte {
	body, _ := json.Marshal(map[string]any{
		"uid":   uid,
		"title": title,
		"panels": []any{
			map[string]any{"id": 1, "title": "CPU", "description": "CPU usage"},
		},
	})
	return body
}

// multiPanelDashboard returns a dashboard with N panels — used to verify
// pooling collapses a fan of items into a single embed call.
func multiPanelDashboard(uid, title string, n int) []byte {
	panels := make([]any, n)
	for i := 0; i < n; i++ {
		panels[i] = map[string]any{"id": i + 1, "title": uid, "description": "panel"}
	}
	body, _ := json.Marshal(map[string]any{"uid": uid, "title": title, "panels": panels})
	return body
}

// newScanner builds a Scanner and runs bootstrap synchronously, so
// runOnce() picks up everything the test pre-loaded into st.changes.
// Tests that want to observe pre-bootstrap state should use newScannerNoBootstrap.
func newScanner(t *testing.T, st *fakeStorage, vec *fakeVector) (*Scanner, *fakeText) {
	t.Helper()
	s, text := newScannerNoBootstrap(t, st, vec)
	s.bootstrap(context.Background())
	return s, text
}

func newScannerNoBootstrap(t *testing.T, st *fakeStorage, vec *fakeVector) (*Scanner, *fakeText) {
	t.Helper()
	text := &fakeText{dim: 4}
	// Default Subscribe stub: hands the test's fakeStorage watch channel
	// back. Tests that don't drive the watch path leave the channel
	// unused; tests that do call st.emit() directly.
	subscribe := func(ctx context.Context, _ string) (<-chan *resource.WrittenEvent, func(), error) {
		ch, err := st.WatchWriteEvents(ctx)
		if err != nil {
			return nil, nil, err
		}
		return ch, func() {}, nil
	}
	s, err := New(Options{
		Storage:       st,
		VectorBackend: vec,
		Embedder:      newFakeEmbedder(text),
		Builders:      []embed.Builder{dashboard.New()},
		Subscribe:     subscribe,
		PollInterval:  time.Hour,
	})
	require.NoError(t, err)
	return s, text
}

// dashEvent builds a pendingEvent with the dashboard group/resource pre-filled.
func dashEvent(action resourcepb.WatchEvent_Type, ns, name string, rv int64, value []byte) *pendingEvent {
	return &pendingEvent{
		action:    action,
		group:     dashGroup,
		resource:  dashRes,
		namespace: ns,
		name:      name,
		value:     value,
		rv:        rv,
	}
}

func dashChange(action resourcepb.WatchEvent_Type, ns, name string, rv int64, value []byte) *resource.ModifiedResource {
	return &resource.ModifiedResource{
		Action: action,
		Key: resourcepb.ResourceKey{
			Group: dashGroup, Resource: dashRes, Namespace: ns, Name: name,
		},
		ResourceVersion: rv,
		Value:           value,
	}
}

func TestScanner_NewValidatesInputs(t *testing.T) {
	noopSubscribe := func(context.Context, string) (<-chan *resource.WrittenEvent, func(), error) {
		return nil, func() {}, nil
	}
	cases := []struct {
		name string
		mod  func(*Options)
	}{
		{"missing storage", func(o *Options) { o.Storage = nil }},
		{"missing vector", func(o *Options) { o.VectorBackend = nil }},
		{"missing embedder", func(o *Options) { o.Embedder = nil }},
		{"missing builders", func(o *Options) { o.Builders = nil }},
		{"missing subscribe", func(o *Options) { o.Subscribe = nil }},
		{"missing embedder model", func(o *Options) {
			e := *o.Embedder
			e.Model = ""
			o.Embedder = &e
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				Storage:       &fakeStorage{},
				VectorBackend: newFakeVector(),
				Embedder:      newFakeEmbedder(&fakeText{dim: 4}),
				Builders:      []embed.Builder{dashboard.New()},
				Subscribe:     noopSubscribe,
			}
			tc.mod(&opts)
			_, err := New(opts)
			require.Error(t, err)
		})
	}
}

func TestScanner_NoChanges_AdvancesToLatestRV(t *testing.T) {
	st := &fakeStorage{}
	vec := newFakeVector()
	s, text := newScanner(t, st, vec)

	s.runOnce(context.Background())

	// Empty change set: no embed call, no upsert, checkpoint stays at 0.
	assert.Equal(t, int64(0), vec.latestRV)
	assert.Empty(t, vec.upserts)
	assert.Empty(t, vec.deletes)
	assert.Equal(t, 0, text.calls, "no embed call on an empty cycle")
}

func TestScanner_HappyPath_PoolsEmbedAndUpsertAcrossNamespaces(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
		dashChange(resourcepb.WatchEvent_MODIFIED, "ns-b", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")),
	}
	vec := newFakeVector()
	s, text := newScanner(t, st, vec)

	s.runOnce(context.Background())

	// One pooled EmbedText call covers items from both namespaces.
	assert.Equal(t, 1, text.calls, "pooled embed call across all namespaces")
	require.Len(t, vec.upserts, 1, "single Upsert wraps every pooled vector")
	assert.Len(t, vec.upserts[0], 2, "two dashboards = two vectors in the pooled upsert")
	assert.Equal(t, int64(200), vec.latestRV)
	assert.Equal(t, 1, vec.lockAttempts)
	assert.Equal(t, 1, vec.lockReleases)
}

func TestScanner_HappyPath_PoolsManyPanelsIntoOneEmbedCall(t *testing.T) {
	// One dashboard with many panels + one dashboard with one panel.
	// Pooling must produce a single EmbedText call regardless of how the
	// panels are distributed across dashboards.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "big", 100, multiPanelDashboard("big", "Big Dash", 12)),
		dashChange(resourcepb.WatchEvent_ADDED, "ns-b", "small", 200, minimalDashboard("small", "Small Dash")),
	}
	vec := newFakeVector()
	s, text := newScanner(t, st, vec)

	s.runOnce(context.Background())

	assert.Equal(t, 1, text.calls)
	require.Len(t, vec.upserts, 1)
	assert.Len(t, vec.upserts[0], 13, "12 panels + 1 panel = 13 pooled vectors")
}

func TestScanner_DeleteEvent_CallsVectorDeleteInline(t *testing.T) {
	// Deletes don't need embeddings, so they execute inline in the
	// per-namespace loop — not in the pooled phase.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_DELETED, "ns", "dash-x", 50, nil),
	}
	vec := newFakeVector()
	s, text := newScanner(t, st, vec)

	s.runOnce(context.Background())

	require.Len(t, vec.deletes, 1)
	assert.Equal(t, deleteCall{Namespace: "ns", Model: testModel, Resource: dashRes, UID: "dash-x"}, vec.deletes[0])
	assert.Equal(t, int64(50), vec.latestRV)
	assert.Equal(t, 0, text.calls, "delete-only cycle does not call the embedder")
}

func TestScanner_LockUnavailable_NoWork(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	vec := newFakeVector()
	vec.lockUnavailable = true
	s, text := newScanner(t, st, vec)

	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Equal(t, int64(0), vec.latestRV)
	assert.Equal(t, 1, vec.lockAttempts)
	assert.Equal(t, 0, vec.lockReleases)
	assert.Equal(t, 0, text.calls)
}

func TestScanner_PooledUpsertFailure_BlocksAdvanceAtLowestRV(t *testing.T) {
	// Pooling makes the failure mode all-or-nothing: a single Upsert
	// covers every dashboard's vectors, so any failure inside the
	// upsert pins the global advance to (lowestRvInBatch - 1).
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "first", 100, minimalDashboard("first", "First")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "second", 200, minimalDashboard("second", "Second")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "third", 300, minimalDashboard("third", "Third")),
	}
	vec := newFakeVector()
	vec.upsertErr = errBoom
	s, _ := newScanner(t, st, vec)
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts, "Upsert failed; nothing was recorded")
	assert.Equal(t, int64(99), vec.latestRV, "checkpoint pinned to lowest pending RV - 1")
}

func TestScanner_PooledEmbedFailure_BlocksAdvanceAtLowestRV(t *testing.T) {
	// Same property as the upsert-failure test, but driven by an embedder error.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "alpha", 100, minimalDashboard("alpha", "Alpha")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "beta", 200, minimalDashboard("beta", "Beta")),
	}
	vec := newFakeVector()
	s, text := newScanner(t, st, vec)
	text.failNext = errBoom

	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Equal(t, int64(99), vec.latestRV)
}

func TestScanner_IteratorError_DoesNotAdvance(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	st.itemErr = errBoom
	st.itemErrI = 0
	vec := newFakeVector()
	s, _ := newScanner(t, st, vec)

	s.runOnce(context.Background())

	assert.Equal(t, int64(0), vec.latestRV, "iterator-level error must not advance the checkpoint")
}

func TestScanner_StaleSubresources_AreDeletedBeforeUpsert(t *testing.T) {
	// Pre-seed two stored panels under one dashboard, then drive an
	// update whose extract only contains panel/1. Cleanup runs inline
	// during collection (per-dashboard), so it lands before the pooled
	// upsert.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_MODIFIED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	vec := newFakeVector()
	k := subsKey("ns", testModel, dashRes, "dash-1")
	vec.storedSubs[k] = map[string]string{
		"panel/1": "old content",
		"panel/2": "stale panel that should be deleted",
	}

	s, _ := newScanner(t, st, vec)
	s.runOnce(context.Background())

	require.Len(t, vec.delsubs, 1)
	assert.ElementsMatch(t, []string{"panel/2"}, vec.delsubs[0].Subresources)
	require.Len(t, vec.upserts, 1)
}

func TestScanner_MonotonicCheckpoint(t *testing.T) {
	// Two cycles. Each cycle issues at most one pooled embed/upsert.
	// After cycle 1 the checkpoint is at 100; cycle 2 must not
	// reprocess RV 100 and should advance to 200.
	vec := newFakeVector()
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	s, text := newScanner(t, st, vec)
	s.runOnce(context.Background())
	require.Len(t, vec.upserts, 1)
	require.Equal(t, int64(100), vec.latestRV)
	require.Equal(t, 1, text.calls)

	// Simulate a watch event for the new write — the scanner enqueues
	// the event with its payload, so cycle 2 doesn't need to re-list.
	st.changes = append(st.changes, dashChange(resourcepb.WatchEvent_MODIFIED, "ns", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")))
	s.enqueue(dashEvent(resourcepb.WatchEvent_MODIFIED, "ns", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")))
	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 2, "second cycle adds one more pooled upsert")
	assert.Len(t, vec.upserts[1], 1, "only the unseen RV 200 event ended up in cycle 2's batch")
	require.Equal(t, int64(200), vec.latestRV)
	require.Equal(t, 2, text.calls, "one embed call per non-empty cycle")
}

func TestScanner_UnknownAction_BlocksAdvance(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		{
			Action: resourcepb.WatchEvent_BOOKMARK,
			Key: resourcepb.ResourceKey{
				Group: dashGroup, Resource: dashRes, Namespace: "ns", Name: "weird",
			},
			ResourceVersion: 50,
		},
	}
	vec := newFakeVector()
	s, text := newScanner(t, st, vec)
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Empty(t, vec.deletes)
	assert.Equal(t, 0, text.calls)
	assert.Equal(t, int64(49), vec.latestRV, "checkpoint stops at (failed - 1)")
}

func TestScanner_MultiNamespace_FailureBlocksGlobalAdvance(t *testing.T) {
	// Pooling makes any per-cycle Upsert failure global. Even if only
	// one namespace has a problem, the checkpoint stops at the lowest
	// pending RV minus one.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "boom", 100, minimalDashboard("boom", "Boom")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns-b", "ok", 200, minimalDashboard("ok", "OK")),
	}
	vec := newFakeVector()
	vec.upsertErrFn = func(vs []vector.Vector) error {
		for _, v := range vs {
			if v.UID == "boom" {
				return errBoom
			}
		}
		return nil
	}
	s, _ := newScanner(t, st, vec)
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts, "pooled upsert containing 'boom' fails wholesale")
	assert.Equal(t, int64(99), vec.latestRV, "global advance stops at lowest pending RV - 1")
}

func TestScanner_NoNamespacesActive_NoOp(t *testing.T) {
	st := &fakeStorage{}
	vec := newFakeVector()
	s, text := newScanner(t, st, vec)

	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Empty(t, vec.deletes)
	assert.Equal(t, 0, text.calls)
	assert.Equal(t, int64(0), vec.latestRV)
}

func TestScanner_Bootstrap_PrefersNamespaceListerCapability(t *testing.T) {
	// fakeStorage advertises the NamespaceLister capability by default.
	// Bootstrap should hit it instead of GetResourceStats.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns-b", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")),
	}
	// A non-nil override forces this exact set, proving the scanner used
	// the capability rather than deriving from changes via GetResourceStats.
	st.namespaceListerNamespaces = []string{"ns-a", "ns-b"}

	vec := newFakeVector()
	s, _ := newScanner(t, st, vec) // bootstraps inline
	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 1)
	assert.Len(t, vec.upserts[0], 2)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_Bootstrap_NamespaceListerError_NoRecovery(t *testing.T) {
	// NamespaceLister error means bootstrap can't recover missed
	// writes. The scanner logs and proceeds; only the watch can
	// surface new activity from this point on.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	st.nsListerErr = errBoom
	vec := newFakeVector()
	s, _ := newScanner(t, st, vec)
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts, "no recovery without NamespaceLister")
	assert.Equal(t, int64(0), vec.latestRV, "checkpoint stays put")
}

func TestScanner_WatchEvent_DrivesNextCycle(t *testing.T) {
	// Storage starts empty → bootstrap finds nothing → cycle 1 is a no-op.
	// A watch event arrives carrying the payload directly; cycle 2 embeds it
	// without re-listing storage.
	st := &fakeStorage{}
	vec := newFakeVector()
	s, text := newScannerNoBootstrap(t, st, vec)
	s.bootstrap(context.Background())

	s.runOnce(context.Background())
	require.Empty(t, vec.upserts)
	require.Equal(t, 0, text.calls, "no work to do on cycle 1")

	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns-x", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")))

	s.runOnce(context.Background())
	require.Len(t, vec.upserts, 1, "cycle 2 picks up the watch event")
	assert.Equal(t, 1, text.calls)
	assert.Equal(t, int64(100), vec.latestRV)
}

func TestScanner_EnqueueDedup_KeepsHighestRV(t *testing.T) {
	// Two events for the same dashboard at different RVs. Only the
	// highest RV's content should be embedded.
	st := &fakeStorage{}
	vec := newFakeVector()
	s, text := newScannerNoBootstrap(t, st, vec)

	// Older RV first.
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash", 100, minimalDashboard("dash", "Old Title")))
	// Newer RV — wins.
	s.enqueue(dashEvent(resourcepb.WatchEvent_MODIFIED, "ns", "dash", 200, minimalDashboard("dash", "New Title")))
	// Older RV arriving after newer (replay scenario) — dropped.
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash", 100, minimalDashboard("dash", "Old Again")))

	s.runOnce(context.Background())
	assert.Equal(t, 1, text.calls, "single embed call")
	require.Len(t, vec.upserts, 1)
	require.Len(t, vec.upserts[0], 1, "only the highest-RV event was embedded")
	assert.Equal(t, int64(200), vec.upserts[0][0].ResourceVersion)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_EnqueueDedup_DeleteOverridesOlderUpsert(t *testing.T) {
	// A delete at a higher RV must beat an earlier upsert for the same resource.
	st := &fakeStorage{}
	vec := newFakeVector()
	s, _ := newScannerNoBootstrap(t, st, vec)

	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "dash", 100, minimalDashboard("dash", "Title")))
	s.enqueue(dashEvent(resourcepb.WatchEvent_DELETED, "ns", "dash", 200, nil))

	s.runOnce(context.Background())
	assert.Empty(t, vec.upserts, "older upsert was overridden by the newer delete")
	require.Len(t, vec.deletes, 1)
	assert.Equal(t, "dash", vec.deletes[0].UID)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_CursorFiltersAlreadyProcessedEvents(t *testing.T) {
	// Pre-set the checkpoint to 150. Any event with RV ≤ 150 must be
	// dropped at the cursor check.
	st := &fakeStorage{}
	vec := newFakeVector()
	vec.latestRV = 150
	s, text := newScannerNoBootstrap(t, st, vec)

	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "old", 100, minimalDashboard("old", "Old"))) // already processed
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns", "new", 200, minimalDashboard("new", "New")))

	s.runOnce(context.Background())
	assert.Equal(t, 1, text.calls)
	require.Len(t, vec.upserts, 1)
	require.Len(t, vec.upserts[0], 1, "only the post-cursor event embedded")
	assert.Equal(t, "new", vec.upserts[0][0].UID)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_WatchConsumer_IgnoresUnrelatedResources(t *testing.T) {
	// The consumer goroutine should drop events whose resource isn't
	// configured (e.g. folders, when only the dashboards builder is registered).
	st := &fakeStorage{}
	vec := newFakeVector()
	s, _ := newScannerNoBootstrap(t, st, vec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := st.WatchWriteEvents(ctx)
	require.NoError(t, err)
	go s.consumeWatchEvents(ctx, ch)

	st.emit(&resource.WrittenEvent{
		Type: resourcepb.WatchEvent_ADDED,
		Key: &resourcepb.ResourceKey{
			Group: "folder.grafana.app", Resource: "folders", Namespace: "ns-x", Name: "f1",
		},
		ResourceVersion: 50,
	})
	st.emit(&resource.WrittenEvent{
		Type: resourcepb.WatchEvent_ADDED,
		Key: &resourcepb.ResourceKey{
			Group: dashGroup, Resource: dashRes, Namespace: "ns-y", Name: "d1",
		},
		Value:           minimalDashboard("d1", "Dash 1"),
		ResourceVersion: 60,
	})

	dashKey := eventQueueKey(dashGroup, dashRes, "ns-y", "d1")
	folderKey := eventQueueKey("folder.grafana.app", "folders", "ns-x", "f1")

	require.Eventually(t, func() bool {
		s.queueMu.Lock()
		defer s.queueMu.Unlock()
		_, dashboardQueued := s.queue[dashKey]
		_, folderQueued := s.queue[folderKey]
		return dashboardQueued && !folderQueued
	}, time.Second, 10*time.Millisecond)
}

func TestScanner_PooledFailure_ReEnqueuesSourceEvents(t *testing.T) {
	// On a pooled failure, every source event that contributed must
	// land back on the queue so the next cycle retries it.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns-b", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")),
	}
	vec := newFakeVector()
	vec.upsertErr = errBoom
	s, _ := newScanner(t, st, vec)
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Equal(t, int64(99), vec.latestRV, "pooled failure pins target to lowest pending RV - 1")

	keyA := eventQueueKey(dashGroup, dashRes, "ns-a", "dash-1")
	keyB := eventQueueKey(dashGroup, dashRes, "ns-b", "dash-2")

	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	_, hasA := s.queue[keyA]
	_, hasB := s.queue[keyB]
	assert.True(t, hasA, "dash-1 re-enqueued after pooled failure")
	assert.True(t, hasB, "dash-2 re-enqueued after pooled failure")
}

func TestScanner_BackfillInProgress_BootstrapSkipped(t *testing.T) {
	// An incomplete backfill job for our resource means bootstrap should
	// not list/enqueue anything. The backfill's CompleteBackfillJob
	// hand-off will move vector_latest_rv forward when it's done.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	vec := newFakeVector()
	vec.jobs = []vector.BackfillJob{{
		ID: 1, Model: testModel, Resource: dashRes, StoppingRV: 1000,
	}}
	s, _ := newScanner(t, st, vec) // bootstraps inline
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts, "backfill in progress; nothing should be embedded")
	assert.Equal(t, int64(0), vec.latestRV)
}

func TestScanner_BackfillInProgress_WatchEventsDeferred(t *testing.T) {
	// Watch events arriving while backfill runs are queued but not
	// processed. They drain on the next cycle after backfill completes.
	st := &fakeStorage{}
	vec := newFakeVector()
	vec.jobs = []vector.BackfillJob{{
		ID: 1, Model: testModel, Resource: dashRes, StoppingRV: 1000,
	}}

	s, text := newScannerNoBootstrap(t, st, vec)

	// Live event arrives during backfill.
	s.enqueue(dashEvent(resourcepb.WatchEvent_ADDED, "ns-x", "dash-x", 1500, minimalDashboard("dash-x", "Dash X")))
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts, "deferred while backfill is in flight")
	assert.Equal(t, 0, text.calls)

	// Backfill completes (test simulates handoff).
	vec.jobs = nil
	vec.latestRV = 1000

	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 1, "deferred event drains once backfill clears")
	assert.Equal(t, "dash-x", vec.upserts[0][0].UID)
	assert.Equal(t, int64(1500), vec.latestRV)
}

func TestScanner_BackfillForDifferentResource_DoesNotBlock(t *testing.T) {
	// A backfill job for a model/resource the scanner doesn't handle
	// (different model in this case) must not gate dashboard work.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	vec := newFakeVector()
	vec.jobs = []vector.BackfillJob{{
		ID: 1, Model: "some-other-model", Resource: dashRes, StoppingRV: 1000,
	}}
	s, _ := newScanner(t, st, vec)
	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 1, "different model = different vector space; not blocked")
	assert.Equal(t, int64(100), vec.latestRV)
}

func TestChooseTarget(t *testing.T) {
	const noFail = int64(1<<63 - 1)
	cases := []struct {
		name                                    string
		sinceRv, latestRv, lowestFailedRv, want int64
	}{
		{"no failures advances to latest", 50, 200, noFail, 200},
		{"failure advances to fail-1", 50, 200, 120, 119},
		{"failure at sinceRv+1 stays put", 50, 200, 51, 50},
		{"failure at sinceRv stays put", 50, 200, 50, 50},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := chooseTarget(tc.sinceRv, tc.latestRv, tc.lowestFailedRv)
			require.Equal(t, tc.want, got)
		})
	}
}
