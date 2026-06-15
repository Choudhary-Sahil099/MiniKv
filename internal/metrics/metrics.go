package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

var (
	TotalRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_requests_total",
			Help: "Total requests received",
		},
	)

	SetRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_set_requests_total",
			Help: "Total SET requests",
		},
	)

	GetRequests = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "minikv_get_requests_total",
			Help: "Total GET requests",
		},
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
func StartServer() {

	http.Handle(
		"/metrics",
		promhttp.Handler(),
	)

	http.ListenAndServe(
		":2112",
		nil,
	)
}
