package internal

import (
	"context"
	"fmt"
	"strings"

	"gopkg.in/inf.v0"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"sigs.k8s.io/custom-metrics-apiserver/pkg/provider"
)

const QUEUE_PREFIX = "al-service-queue-length-"

type alProvider struct {
	values *QueueMonitor
}

func NewAlProvider(qm *QueueMonitor) provider.ExternalMetricsProvider {
	provider := &alProvider{
		values: qm,
	}
	return provider
}

func (p *alProvider) GetExternalMetric(ctx context.Context, namespace string, metricSelector labels.Selector, info provider.ExternalMetricInfo) (*external_metrics.ExternalMetricValueList, error) {
	snapshot := p.values.read()

	service_name := strings.ToLower(strings.TrimPrefix(info.Metric, QUEUE_PREFIX))
	// if we can't remove the prefix, the string won't change
	if service_name == info.Metric {
		return nil, fmt.Errorf("Metric not found (%s != %s)", service_name, info.Metric)
	}

	queue_length, ok := snapshot.lengths[service_name]
	if !ok {
		return nil, fmt.Errorf("Metric not found (%s)", service_name)
	}

	matchingMetrics := []external_metrics.ExternalMetricValue{}
	matchingMetrics = append(matchingMetrics, external_metrics.ExternalMetricValue{
		MetricName: info.Metric,
		Timestamp:  snapshot.timestamp,
		Value:      *resource.NewDecimalQuantity(*inf.NewDec(queue_length, 0), resource.DecimalSI),
	})

	return &external_metrics.ExternalMetricValueList{
		Items: matchingMetrics,
	}, nil

}

func (p *alProvider) ListAllExternalMetrics() []provider.ExternalMetricInfo {
	snapshot := p.values.read()
	metric_list := make([]provider.ExternalMetricInfo, 0)
	for service_name := range snapshot.lengths {
		metric_list = append(metric_list, provider.ExternalMetricInfo{
			Metric: QUEUE_PREFIX + service_name,
		})
	}
	return metric_list
}
