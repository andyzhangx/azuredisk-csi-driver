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
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/metrics"
)

type rateLimiterLatencyObservation struct {
	ctx     context.Context
	verb    string
	u       url.URL
	latency time.Duration
	calls   int
}

func (o *rateLimiterLatencyObservation) Observe(ctx context.Context, verb string, u url.URL, latency time.Duration) {
	o.ctx, o.verb, o.u, o.latency = ctx, verb, u, latency
	o.calls++
}

type rateLimiterSpanExporter struct {
	spans []trace.ReadOnlySpan
}

func (e *rateLimiterSpanExporter) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	e.spans = append(e.spans, spans...)
	return nil
}

func (*rateLimiterSpanExporter) Shutdown(context.Context) error { return nil }

func TestRateLimiterLatencyTracing(t *testing.T) {
	for _, test := range []struct {
		name      string
		waited    time.Duration
		recording bool
		wantEvent bool
	}{
		{name: "short wait", waited: kubeRateLimitWaitThreshold - time.Millisecond, recording: true},
		{name: "threshold wait", waited: kubeRateLimitWaitThreshold, recording: true, wantEvent: true},
		{name: "not recording", waited: 2 * kubeRateLimitWaitThreshold},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := &rateLimiterSpanExporter{}
			sampler := trace.NeverSample()
			if test.recording {
				sampler = trace.AlwaysSample()
			}
			provider := trace.NewTracerProvider(trace.WithSyncer(recorder), trace.WithSampler(sampler))
			defer func() { _ = provider.Shutdown(context.Background()) }()
			ctx, span := provider.Tracer("test").Start(context.Background(), "GetNode")
			original := &rateLimiterLatencyObservation{}
			observer := &tracingRateLimiterLatency{LatencyMetric: original}
			u := url.URL{Scheme: "https", Host: "kubernetes", Path: "/api/v1/nodes/node"}

			observer.Observe(ctx, "GET", u, test.waited)
			span.End()

			if original.calls != 1 || original.ctx != ctx || original.verb != "GET" || original.u != u || original.latency != test.waited {
				t.Fatalf("original metrics observer did not receive the unchanged observation: %+v", original)
			}
			spans := recorder.spans
			if !test.recording {
				if len(spans) != 0 {
					t.Fatal("non-recording span was exported")
				}
				return
			}
			if len(spans) != 1 {
				t.Fatalf("got %d spans, want 1", len(spans))
			}
			events := spans[0].Events()
			if !test.wantEvent {
				if len(events) != 0 {
					t.Fatalf("short wait produced events: %v", events)
				}
				return
			}
			if len(events) != 1 || events[0].Name != "kube_client_ratelimited" {
				t.Fatalf("expected one rate-limiter event, got %v", events)
			}
			attrs := events[0].Attributes
			if len(attrs) != 1 || string(attrs[0].Key) != "wait_ms" || attrs[0].Value.AsInt64() != test.waited.Milliseconds() {
				t.Fatalf("incorrect wait attributes: %v", attrs)
			}
		})
	}
}

func TestRegisterRateLimiterTracingPreservesLimiters(t *testing.T) {
	previous := metrics.RateLimiterLatency
	t.Cleanup(func() { metrics.RateLimiterLatency = previous })
	original := &rateLimiterLatencyObservation{}
	metrics.RateLimiterLatency = original
	RegisterRateLimiterTracing()
	wrapped := metrics.RateLimiterLatency
	RegisterRateLimiterTracing()
	if metrics.RateLimiterLatency != wrapped {
		t.Fatal("metrics observer was wrapped more than once")
	}

	client, err := kubernetes.NewForConfig(&rest.Config{Host: "https://kubernetes"})
	if err != nil {
		t.Fatal(err)
	}
	coreLimiter := client.CoreV1().RESTClient().GetRateLimiter()
	storageLimiter := client.StorageV1().RESTClient().GetRateLimiter()
	if coreLimiter == nil || storageLimiter == nil || coreLimiter == storageLimiter {
		t.Fatal("default CoreV1 and StorageV1 limiters must be present and independent")
	}
	for i := 0; i < rest.DefaultBurst; i++ {
		if !coreLimiter.TryAccept() {
			t.Fatalf("CoreV1 burst exhausted after %d requests", i)
		}
	}
	if !storageLimiter.TryAccept() {
		t.Fatal("CoreV1 exhausted StorageV1's independent budget")
	}
}
