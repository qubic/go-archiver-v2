package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ProcessingMetrics struct {
	prometheusRegistry     *prometheus.Registry
	lastProcessedTickGauge prometheus.Gauge
	currentEpochGauge      prometheus.Gauge
}

func NewProcessingMetrics(registry *prometheus.Registry, namespace string) *ProcessingMetrics {

	factory := promauto.With(registry)
	metrics := ProcessingMetrics{
		prometheusRegistry: registry,
		lastProcessedTickGauge: factory.NewGauge(prometheus.GaugeOpts{
			Name: namespace + "_last_processed_tick",
			Help: "The last processed tick.",
		}),
		currentEpochGauge: factory.NewGauge(prometheus.GaugeOpts{
			Name: namespace + "_current_epoch",
			Help: "Current epoch.",
		}),
	}
	return &metrics
}

func (m *ProcessingMetrics) SetLastProcessedTick(tickNumber uint32) {
	m.lastProcessedTickGauge.Set(float64(tickNumber))
}

func (m *ProcessingMetrics) SetCurrentEpoch(epoch uint32) {
	m.currentEpochGauge.Set(float64(epoch))
}
