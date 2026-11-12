package metrics

// this is for the data represntation/visualization in the grafna
import (
	"context"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"time"
)

var (
	TotalRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "minikv_requests_total",
			Help: "Total requests received",
		},
		[]string{"node"},
	)

	SetRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "minikv_set_requests_total",
			Help: "Total SET requests",
		},
		[]string{"node"},
	)

	GetRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "minikv_get_requests_total",
			Help: "Total GET requests",
		},
		[]string{"node"},
	)

	DelRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_del_requests_total",
			Help: "Total DEL requests",
		},
	)

	ForwardedRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_forwarded_requests_total",
			Help: "Total forwarded requests",
		},
	)

	ReplicationRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_replication_requests_total",
			Help: "Total replication operations",
		},
	)
	RequestLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "minikv_request_latency_seconds",
			Help:    "Latency of requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)
	ReadRepairs = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_read_repairs_total",
			Help: "Total read repairs performed",
		},
	)

	ClusterSyncs = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_cluster_syncs_total",
			Help: "Total cluster synchronization operations",
		},
	)

	SnapshotsCreated = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_snapshots_created_total",
			Help: "Total snapshots created",
		},
	)

	WALCompactions = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_wal_compactions_total",
			Help: "Total WAL compactions",
		},
	)

	NodeRecoveries = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_node_recoveries_total",
			Help: "Total node recovery events detected",
		},
	)
	AntiEntropyRepairs = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_anti_entropy_repairs_total",
			Help: "Total anti entropy repairs",
		},
	)
)
var AliveNodes = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "minikv_alive_nodes",
		Help: "Number of alive nodes",
	},
)

var DeadNodes = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "minikv_dead_nodes",
		Help: "Number of dead nodes",
	},
)

func Init() {

	prometheus.MustRegister(
		TotalRequests,
		SetRequests,
		GetRequests,
		DelRequests,
		ForwardedRequests,
		ReplicationRequests,
		RequestLatency,
		AliveNodes,
		DeadNodes,
		ReadRepairs,
		ClusterSyncs,
		SnapshotsCreated,
		WALCompactions,
		NodeRecoveries,
		AntiEntropyRepairs,
	)
}
func StartServer(ctx context.Context, address string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return err
		}
		err := <-serveErr
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
