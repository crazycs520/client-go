package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	TiKVFFIMicroSecHistogramVec     *prometheus.HistogramVec
	TiKVFFICallMicroSecHistogramVec *prometheus.HistogramVec
	TiKVFFIWaitMicroSecHistogramVec *prometheus.HistogramVec
)

func initMetrics4PkDB(namespace, subsystem string, constLabels prometheus.Labels) {
	TiKVFFIMicroSecHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "ffi_duration_microsecond",
			Help:        "The duration in microsecond of FFI functions, including rust cost and go cost",
			Buckets:     prometheus.ExponentialBucketsRange(10, 10_000, 20), // 10us ~ 10ms
			ConstLabels: constLabels,
		}, []string{"name"})

	TiKVFFICallMicroSecHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "ffi_call_duration_microsecond",
			Help:        "The duration in microsecond of FFI call request",
			Buckets:     prometheus.ExponentialBucketsRange(10, 10_000, 20), // 10us ~ 10ms
			ConstLabels: constLabels,
		}, []string{"name"})

	TiKVFFIWaitMicroSecHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   namespace,
			Subsystem:   subsystem,
			Name:        "ffi_wait_duration_microsecond",
			Help:        "The duration in microsecond of FFI wait response",
			Buckets:     prometheus.ExponentialBucketsRange(10, 10_000, 20), // 10us ~ 10ms
			ConstLabels: constLabels,
		}, []string{"name"})
}

func registerMetrics4PkDB() {
	prometheus.MustRegister(TiKVFFIMicroSecHistogramVec)
	prometheus.MustRegister(TiKVFFICallMicroSecHistogramVec)
	prometheus.MustRegister(TiKVFFIWaitMicroSecHistogramVec)
}
