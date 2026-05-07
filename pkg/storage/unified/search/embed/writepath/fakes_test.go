package writepath

import (
	"context"
	"errors"
	"iter"
	"sync"
	"time"

	"github.com/grafana/grafana/pkg/storage/unified/resource"
	"github.com/grafana/grafana/pkg/storage/unified/resourcepb"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed/embedder"
	"github.com/grafana/grafana/pkg/storage/unified/search/vector"
)

// fakeStorage stubs the bits of resource.StorageBackend the scanner uses.
// ListModifiedSince returns the configured changes (filtered by sinceRv)
// and a latestRv equal to the highest RV in the slice.
type fakeStorage struct {
	mu       sync.Mutex
	changes  []*resource.ModifiedResource
	listErr  error
	itemErr  error // returned from the iterator partway through
	itemErrI int   // index after which to inject itemErr
}

func (f *fakeStorage) WriteEvent(context.Context, resource.WriteEvent) (int64, error) {
	panic("not implemented")
}
func (f *fakeStorage) ReadResource(context.Context, *resourcepb.ReadRequest) *resource.BackendReadResponse {
	panic("not implemented")
}
func (f *fakeStorage) ListIterator(context.Context, *resourcepb.ListRequest, func(resource.ListIterator) error) (int64, error) {
	panic("not implemented")
}
func (f *fakeStorage) ListHistory(context.Context, *resourcepb.ListRequest, func(resource.ListIterator) error) (int64, error) {
	panic("not implemented")
}
func (f *fakeStorage) WatchWriteEvents(context.Context) (<-chan *resource.WrittenEvent, error) {
	panic("not implemented")
}
func (f *fakeStorage) GetResourceStats(context.Context, resource.NamespacedResource, int) ([]resource.ResourceStats, error) {
	panic("not implemented")
}
func (f *fakeStorage) GetResourceLastImportTimes(context.Context) iter.Seq2[resource.ResourceLastImportTime, error] {
	panic("not implemented")
}

func (f *fakeStorage) ListModifiedSince(_ context.Context, key resource.NamespacedResource, sinceRv int64, _ *time.Time) (int64, iter.Seq2[*resource.ModifiedResource, error]) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		err := f.listErr
		return 0, func(yield func(*resource.ModifiedResource, error) bool) {
			yield(nil, err)
		}
	}
	// Snapshot the slice + per-iteration error config so the iterator
	// closes over a stable view. The scanner runs the iter outside the
	// lock, and the test may mutate state afterwards.
	matches := make([]*resource.ModifiedResource, 0, len(f.changes))
	var latestRv int64
	for _, c := range f.changes {
		if c.Key.Group != key.Group || c.Key.Resource != key.Resource {
			continue
		}
		if c.ResourceVersion <= sinceRv {
			continue
		}
		matches = append(matches, c)
		if c.ResourceVersion > latestRv {
			latestRv = c.ResourceVersion
		}
	}
	itemErr := f.itemErr
	itemErrI := f.itemErrI
	return latestRv, func(yield func(*resource.ModifiedResource, error) bool) {
		for i, c := range matches {
			if itemErr != nil && i == itemErrI {
				if !yield(nil, itemErr) {
					return
				}
				continue
			}
			if !yield(c, nil) {
				return
			}
		}
	}
}

// fakeVector records calls and lets tests inspect upserts/deletes/checkpoint advancement.
type fakeVector struct {
	mu sync.Mutex

	latestRV    int64
	upserts     [][]vector.Vector
	deletes     []deleteCall
	delsubs     []deleteSubsCall
	storedSubs  map[string]map[string]string // ns|model|res|uid -> sub -> content
	upsertErr   error
	upsertErrFn func(vs []vector.Vector) error // dynamic error decision
	deleteErr   error

	lockUnavailable bool
	lockAttempts    int
	lockReleases    int
}

type deleteCall struct{ Namespace, Model, Resource, UID string }
type deleteSubsCall struct {
	Namespace, Model, Resource, UID string
	Subresources                    []string
}

func newFakeVector() *fakeVector {
	return &fakeVector{storedSubs: map[string]map[string]string{}}
}

func subsKey(ns, model, res, uid string) string { return ns + "|" + model + "|" + res + "|" + uid }

func (f *fakeVector) Search(context.Context, string, string, string, []float32, int, ...vector.SearchFilter) ([]vector.VectorSearchResult, error) {
	return nil, nil
}
func (f *fakeVector) Upsert(_ context.Context, vs []vector.Vector) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.upsertErrFn != nil {
		if err := f.upsertErrFn(vs); err != nil {
			return err
		}
	}
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.upserts = append(f.upserts, vs)
	for _, v := range vs {
		k := subsKey(v.Namespace, v.Model, v.Resource, v.UID)
		if f.storedSubs[k] == nil {
			f.storedSubs[k] = map[string]string{}
		}
		f.storedSubs[k][v.Subresource] = v.Content
	}
	return nil
}
func (f *fakeVector) Delete(_ context.Context, ns, model, res, uid string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deletes = append(f.deletes, deleteCall{ns, model, res, uid})
	delete(f.storedSubs, subsKey(ns, model, res, uid))
	return nil
}
func (f *fakeVector) DeleteSubresources(_ context.Context, ns, model, res, uid string, subs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delsubs = append(f.delsubs, deleteSubsCall{ns, model, res, uid, subs})
	if m := f.storedSubs[subsKey(ns, model, res, uid)]; m != nil {
		for _, s := range subs {
			delete(m, s)
		}
	}
	return nil
}
func (f *fakeVector) GetSubresourceContent(_ context.Context, ns, model, res, uid string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for k, v := range f.storedSubs[subsKey(ns, model, res, uid)] {
		out[k] = v
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}
func (f *fakeVector) Exists(context.Context, string, string, string, string) (bool, error) {
	return false, nil
}
func (f *fakeVector) GetLatestRV(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.latestRV, nil
}
func (f *fakeVector) SetLatestRV(_ context.Context, rv int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rv > f.latestRV {
		f.latestRV = rv
	}
	return nil
}
func (f *fakeVector) ListIncompleteBackfillJobs(context.Context) ([]vector.BackfillJob, error) {
	return nil, nil
}
func (f *fakeVector) UpdateBackfillJobCheckpoint(context.Context, int64, string, string) error {
	return nil
}
func (f *fakeVector) MarkBackfillJobError(context.Context, int64, string) error { return nil }
func (f *fakeVector) CompleteBackfillJob(context.Context, int64) error          { return nil }
func (f *fakeVector) TryAcquireBackfillLock(context.Context) (func(), bool, error) {
	return func() {}, true, nil
}
func (f *fakeVector) TryAcquireScannerLock(context.Context) (func(), bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lockAttempts++
	if f.lockUnavailable {
		return nil, false, nil
	}
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.lockReleases++
	}, true, nil
}

// fakeText is a deterministic embedder used by the BatchEmbedder.
type fakeText struct{ dim int }

func (f *fakeText) EmbedText(_ context.Context, in embedder.EmbedTextInput) (embedder.EmbedTextOutput, error) {
	out := embedder.EmbedTextOutput{Embeddings: make([]embedder.Embedding, len(in.Texts))}
	for i := range in.Texts {
		dense := make([]float32, f.dim)
		for j := range dense {
			dense[j] = 0.5
		}
		out.Embeddings[i] = embedder.Embedding{Dense: dense}
	}
	return out, nil
}

func newFakeBatchEmbedder() *embedder.BatchEmbedder {
	e := embedder.Embedder{
		TextEmbedder: &fakeText{dim: 4},
		Model:        "test-model",
		VectorType:   embedder.VectorTypeDense,
		Metric:       embedder.CosineDistance,
		Dimensions:   4,
	}
	return embedder.NewBatchEmbedder(e)
}

var errBoom = errors.New("boom")
