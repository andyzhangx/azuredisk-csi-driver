/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package azureutils

import (
	"context"
	"net/url"
	"time"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/tools/metrics"
)

// kubeRateLimitWaitThreshold is the minimum client-side rate-limiter wait
// duration worth recording as a span event. Shorter waits are normal
// token-bucket jitter and would only add noise to traces.
const kubeRateLimitWaitThreshold = 50 * time.Millisecond

type tracingRateLimiterLatency struct {
	metrics.LatencyMetric
}

// RegisterRateLimiterTracing adds span events to client-go's existing wait-time
// observer without changing limiters or suppressing metrics. Call it at startup,
// after metrics registration and before starting any Kubernetes clients.
// Repeated calls before clients start are safe.
func RegisterRateLimiterTracing() {
	if _, ok := metrics.RateLimiterLatency.(*tracingRateLimiterLatency); ok {
		return
	}
	metrics.RateLimiterLatency = &tracingRateLimiterLatency{LatencyMetric: metrics.RateLimiterLatency}
}

// Observe records time spent waiting, including canceled/failed waits: client-go's
// metric hook provides duration but not the outcome of the wait.
func (t *tracingRateLimiterLatency) Observe(ctx context.Context, verb string, u url.URL, waited time.Duration) {
	t.LatencyMetric.Observe(ctx, verb, u, waited)
	if waited >= kubeRateLimitWaitThreshold {
		if span := oteltrace.SpanFromContext(ctx); span.IsRecording() {
			span.AddEvent("kube_client_ratelimited", oteltrace.WithAttributes(
				attribute.Int64("wait_ms", waited.Milliseconds()),
			))
		}
	}
}
