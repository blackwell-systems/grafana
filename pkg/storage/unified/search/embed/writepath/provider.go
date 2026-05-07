package writepath

import (
	"github.com/grafana/grafana/pkg/infra/log"
	"github.com/grafana/grafana/pkg/setting"
	"github.com/grafana/grafana/pkg/storage/unified/resource"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed/dashboard"
	"github.com/grafana/grafana/pkg/storage/unified/search/embed/embedder"
	"github.com/grafana/grafana/pkg/storage/unified/search/vector"
)

// ProvideScanner constructs the write-path scanner. Returns (nil, nil)
// when the feature is disabled or any required dep is missing — same
// pattern as backfill.ProvideVectorBackfiller, so callers must tolerate
// a nil result.
func ProvideScanner(
	cfg *setting.Cfg,
	storage resource.StorageBackend,
	vb vector.VectorBackend,
	emb *embedder.Embedder,
) (*Scanner, error) {
	if cfg == nil || !cfg.VectorBackfillerEnabled {
		return nil, nil
	}
	if cfg.EmbeddingProvider == "" {
		return nil, nil
	}
	if storage == nil || vb == nil || emb == nil {
		return nil, nil
	}
	return New(Options{
		Storage:       storage,
		VectorBackend: vb,
		Embedder:      emb,
		Builders:      []embed.Builder{dashboard.New()},
		Log:           log.New("writepath"),
	})
}
