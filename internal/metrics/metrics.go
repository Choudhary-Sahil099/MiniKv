package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
)
func Init() {

	prometheus.MustRegister(
		TotalRequests,
		SetRequests,
		GetRequests,
		DelRequests,
		ForwardedRequests,
		ReplicationRequests,
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
var RequestLatency = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Name:    "minikv_request_latency_seconds",
		Help:    "Latency of requests in seconds",
		Buckets: prometheus.DefBuckets,
	},
)