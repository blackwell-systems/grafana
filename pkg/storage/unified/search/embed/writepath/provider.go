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
//
// `subscribe` is the write-event source. The standard wiring resolves
// it to the resource server's broadcaster lazily so the server has had
// a chance to initialise before the scanner subscribes.
func ProvideScanner(
	cfg *setting.Cfg,
	storage resource.StorageBackend,
	vb vector.VectorBackend,
	emb *embedder.Embedder,
	subscribe SubscribeFunc,
) (*Scanner, error) {
	logger := log.New("writepath")
	switch {
	case cfg == nil || !cfg.VectorBackfillerEnabled:
		logger.Info("writepath: disabled (vector_backfiller not enabled)")
		return nil, nil
	case cfg.EmbeddingProvider == "":
		logger.Info("writepath: disabled (no embedding provider configured)")
		return nil, nil
	case storage == nil:
		logger.Info("writepath: disabled (no storage backend)")
		return nil, nil
	case vb == nil:
		logger.Info("writepath: disabled (no vector backend)")
		return nil, nil
	case emb == nil:
		logger.Info("writepath: disabled (no embedder)")
		return nil, nil
	case subscribe == nil:
		logger.Info("writepath: disabled (no write-event subscriber wired)")
		return nil, nil
	}
	return New(Options{
		Storage:       storage,
		VectorBackend: vb,
		Embedder:      emb,
		Builders:      []embed.Builder{dashboard.New()},
		Subscribe:     subscribe,
		Log:           logger,
	})
}
