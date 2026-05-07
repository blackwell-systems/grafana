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
	s, err := New(Options{
		Storage:       st,
		VectorBackend: vec,
		Embedder:      newFakeEmbedder(text),
		Builders:      []embed.Builder{dashboard.New()},
		PollInterval:  time.Hour,
	})
	require.NoError(t, err)
	return s, text
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
	cases := []struct {
		name string
		mod  func(*Options)
	}{
		{"missing storage", func(o *Options) { o.Storage = nil }},
		{"missing vector", func(o *Options) { o.VectorBackend = nil }},
		{"missing embedder", func(o *Options) { o.Embedder = nil }},
		{"missing builders", func(o *Options) { o.Builders = nil }},
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

	st.changes = append(st.changes, dashChange(resourcepb.WatchEvent_MODIFIED, "ns", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")))
	// Simulate a watch event for the new write, which flags ns for the next cycle.
	s.flagNamespace("ns")
	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 2, "second cycle adds one more pooled upsert")
	assert.Len(t, vec.upserts[1], 1, "only the unseen RV 200 dashboard ended up in cycle 2's batch")
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

func TestScanner_Bootstrap_FallsBackOnCapabilityError(t *testing.T) {
	// NamespaceLister errors → scanner falls back to GetResourceStats,
	// which derives namespaces from `changes` in the fake.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	st.nsListerErr = errBoom
	vec := newFakeVector()
	s, _ := newScanner(t, st, vec)
	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 1)
	assert.Equal(t, int64(100), vec.latestRV)
}

func TestScanner_WatchEvent_FlagsNamespaceAndScansNextCycle(t *testing.T) {
	// Storage starts empty → bootstrap finds nothing → cycle 1 is a no-op.
	// A watch event arrives announcing activity in ns-x; cycle 2 picks it up.
	st := &fakeStorage{}
	vec := newFakeVector()
	s, text := newScannerNoBootstrap(t, st, vec)
	s.bootstrap(context.Background())

	s.runOnce(context.Background())
	require.Empty(t, vec.upserts)
	require.Equal(t, 0, text.calls, "no work to do on cycle 1")

	// Now activity happens. The change is added to storage AND a watch
	// event flags the namespace.
	st.mu.Lock()
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-x", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	st.mu.Unlock()
	s.flagNamespace("ns-x")

	s.runOnce(context.Background())
	require.Len(t, vec.upserts, 1, "cycle 2 finds the watch-flagged namespace")
	assert.Equal(t, 1, text.calls)
	assert.Equal(t, int64(100), vec.latestRV)
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

	// Push two events: one for folders (ignored), one for dashboards (flagged).
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
		ResourceVersion: 60,
	})
	// Tiny synchronous wait by polling the set instead of sleeping.
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, ok := s.nsToScan["ns-y"]
		_, dropped := s.nsToScan["ns-x"]
		return ok && !dropped
	}, time.Second, 10*time.Millisecond)
}

func TestScanner_PooledFailure_ReFlagsNamespaces(t *testing.T) {
	// On a pooled failure, the scanner must re-flag the drained
	// namespaces so the next cycle retries them.
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
	assert.Equal(t, int64(99), vec.latestRV, "pooled failure pins target to lowest RV - 1")

	// Both namespaces should be back in the set, awaiting retry.
	s.mu.Lock()
	defer s.mu.Unlock()
	_, hasA := s.nsToScan["ns-a"]
	_, hasB := s.nsToScan["ns-b"]
	assert.True(t, hasA, "ns-a re-flagged after pooled failure")
	assert.True(t, hasB, "ns-b re-flagged after pooled failure")
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
