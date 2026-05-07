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

func newScanner(t *testing.T, st *fakeStorage, vec *fakeVector) *Scanner {
	t.Helper()
	s, err := New(Options{
		Storage:       st,
		VectorBackend: vec,
		BatchEmbedder: newFakeBatchEmbedder(),
		Builders:      []embed.Builder{dashboard.New()},
		Model:         testModel,
		PollInterval:  time.Hour,
	})
	require.NoError(t, err)
	return s
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
		{"missing batch embedder", func(o *Options) { o.BatchEmbedder = nil }},
		{"missing builders", func(o *Options) { o.Builders = nil }},
		{"missing model", func(o *Options) { o.Model = "" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				Storage:       &fakeStorage{},
				VectorBackend: newFakeVector(),
				BatchEmbedder: newFakeBatchEmbedder(),
				Builders:      []embed.Builder{dashboard.New()},
				Model:         testModel,
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
	s := newScanner(t, st, vec)

	s.runOnce(context.Background())

	// Empty change set + latestRv == 0 means no-op; checkpoint stays at 0.
	assert.Equal(t, int64(0), vec.latestRV)
	assert.Empty(t, vec.upserts)
	assert.Empty(t, vec.deletes)
}

func TestScanner_HappyPath_UpsertsAndAdvances(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
		dashChange(resourcepb.WatchEvent_MODIFIED, "ns-b", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")),
	}
	vec := newFakeVector()
	s := newScanner(t, st, vec)

	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 2)
	assert.Equal(t, int64(200), vec.latestRV, "advances to latestRv on success")
	assert.Equal(t, 1, vec.lockAttempts)
	assert.Equal(t, 1, vec.lockReleases)
}

func TestScanner_DeleteEvent_CallsVectorDelete(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_DELETED, "ns", "dash-x", 50, nil),
	}
	vec := newFakeVector()
	s := newScanner(t, st, vec)

	s.runOnce(context.Background())

	require.Len(t, vec.deletes, 1)
	assert.Equal(t, deleteCall{Namespace: "ns", Model: testModel, Resource: dashRes, UID: "dash-x"}, vec.deletes[0])
	assert.Equal(t, int64(50), vec.latestRV)
}

func TestScanner_LockUnavailable_NoWork(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	vec := newFakeVector()
	vec.lockUnavailable = true
	s := newScanner(t, st, vec)

	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Equal(t, int64(0), vec.latestRV)
	assert.Equal(t, 1, vec.lockAttempts)
	assert.Equal(t, 0, vec.lockReleases, "lock should not be released since it wasn't acquired")
}

func TestScanner_PartialFailure_AdvancesToLowestFailureMinusOne(t *testing.T) {
	// Three dashboards: two succeed, one fails. The checkpoint should
	// stop short of the failure so it gets retried.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "ok-a", 100, minimalDashboard("ok-a", "OK A")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "boom", 200, minimalDashboard("boom", "Boom")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "ok-b", 300, minimalDashboard("ok-b", "OK B")),
	}
	vec := newFakeVector()
	// Per-call hook: fail upserts whose UID is "boom".
	vec.upsertErrFn = func(vs []vector.Vector) error {
		for _, v := range vs {
			if v.UID == "boom" {
				return errBoom
			}
		}
		return nil
	}
	s := newScanner(t, st, vec)
	s.runOnce(context.Background())

	// Successful upserts only — boom dropped.
	require.Len(t, vec.upserts, 2)
	assert.Equal(t, int64(199), vec.latestRV, "should advance to (lowestFailedRv - 1)")
}

func TestScanner_IteratorError_DoesNotAdvance(t *testing.T) {
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	st.itemErr = errBoom
	st.itemErrI = 0
	vec := newFakeVector()
	s := newScanner(t, st, vec)

	s.runOnce(context.Background())

	assert.Equal(t, int64(0), vec.latestRV, "iterator-level error must not advance the checkpoint")
}

func TestScanner_StaleSubresources_AreDeletedBeforeUpsert(t *testing.T) {
	// Pre-seed the vector store with two stored panels under the same UID,
	// then drive an update that only contains panel/1. The scanner must
	// delete panel/2 before upserting.
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

	s := newScanner(t, st, vec)
	s.runOnce(context.Background())

	require.Len(t, vec.delsubs, 1)
	assert.ElementsMatch(t, []string{"panel/2"}, vec.delsubs[0].Subresources)
	require.Len(t, vec.upserts, 1)
}

func TestScanner_MonotonicCheckpoint(t *testing.T) {
	// Run two cycles. After the first the checkpoint is at 100; second
	// cycle should not reprocess the same resource and should advance to 200.
	vec := newFakeVector()
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns", "dash-1", 100, minimalDashboard("dash-1", "Dash 1")),
	}
	s := newScanner(t, st, vec)
	s.runOnce(context.Background())
	require.Len(t, vec.upserts, 1)
	require.Equal(t, int64(100), vec.latestRV)

	st.changes = append(st.changes, dashChange(resourcepb.WatchEvent_MODIFIED, "ns", "dash-2", 200, minimalDashboard("dash-2", "Dash 2")))
	s.runOnce(context.Background())

	// Upserts grew by exactly one — the previously-seen RV 100 was filtered.
	require.Len(t, vec.upserts, 2)
	require.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_UnknownAction_TreatedAsFailure(t *testing.T) {
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
	s := newScanner(t, st, vec)
	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Empty(t, vec.deletes)
	// Unknown action is a per-item failure; checkpoint stops at (failed - 1)
	// so the same item is retried next cycle (it'll fail again unless the
	// payload changes, but operators can investigate via logs).
	assert.Equal(t, int64(49), vec.latestRV)
}

func TestScanner_MultiNamespace_ProcessesEachAndAdvancesToMaxRV(t *testing.T) {
	// Two namespaces, one resource each. Fan-out must produce two
	// upserts and advance to the highest RV seen.
	st := &fakeStorage{}
	st.changes = []*resource.ModifiedResource{
		dashChange(resourcepb.WatchEvent_ADDED, "ns-a", "dash-1", 100, minimalDashboard("dash-1", "Dash A")),
		dashChange(resourcepb.WatchEvent_ADDED, "ns-b", "dash-2", 200, minimalDashboard("dash-2", "Dash B")),
	}
	vec := newFakeVector()
	s := newScanner(t, st, vec)

	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 2)
	assert.Equal(t, int64(200), vec.latestRV)
}

func TestScanner_MultiNamespace_FailureInOneNamespaceBlocksGlobalAdvance(t *testing.T) {
	// Namespace A has a failing dashboard at RV 100. Namespace B is
	// healthy with RV 200. Global checkpoint must stop at 99 so A's
	// failure is retried, even though everything in B succeeded.
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
	s := newScanner(t, st, vec)
	s.runOnce(context.Background())

	require.Len(t, vec.upserts, 1, "ns-b succeeds; ns-a fails")
	assert.Equal(t, int64(99), vec.latestRV, "global advance stops at lowest failure - 1")
}

func TestScanner_NoNamespacesActive_NoOp(t *testing.T) {
	// GetResourceStats returns empty (no dashboards anywhere yet); the
	// scanner should run cleanly without making list calls.
	st := &fakeStorage{}
	vec := newFakeVector()
	s := newScanner(t, st, vec)

	s.runOnce(context.Background())

	assert.Empty(t, vec.upserts)
	assert.Empty(t, vec.deletes)
	assert.Equal(t, int64(0), vec.latestRV)
}

func TestChooseTarget(t *testing.T) {
	const noFail = int64(1<<63 - 1)
	cases := []struct {
		name                                 string
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
