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

package azuredisk

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/policy/retryrepectthrottled"
)

func TestARMThrottleRetryAfter(t *testing.T) {
	t.Setenv(tracesExporterEnv, "klog")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	t.Setenv(spanVerbosityEnv, "2")
	buf, restore := captureKlogAtSpanVerbosity(t)
	defer restore()
	previousProvider := otel.GetTracerProvider()
	defer otel.SetTracerProvider(previousProvider)
	provider, err := InitOtelTracing()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Shutdown(context.Background()) }()
	previousTransport := azclient.DefaultResourceClientTransport
	defer func() { azclient.DefaultResourceClientTransport = previousTransport }()

	for _, test := range []struct {
		name       string
		status     int
		retryAfter string
		wantDelay  string
	}{
		{name: "uncapped seconds", status: http.StatusTooManyRequests, retryAfter: "1800", wantDelay: "1800s"},
		{name: "HTTP date", status: http.StatusTooManyRequests, retryAfter: "Thu, 29 Oct 2026 14:00:00 GMT", wantDelay: "Thu, 29 Oct 2026 14:00:00 GMT"},
		{name: "missing header", status: http.StatusTooManyRequests},
		{name: "success", status: http.StatusOK, retryAfter: "60"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := &http.Response{
				StatusCode: test.status,
				Header:     http.Header{"Retry-After": []string{test.retryAfter}},
				Body:       io.NopCloser(strings.NewReader("{}")),
			}
			inner := testRoundTripper(func(req *http.Request) (*http.Response, error) {
				response.Request = req
				return response, nil
			})
			client := &http.Client{Transport: inner, Timeout: time.Minute}
			azclient.DefaultResourceClientTransport = client
			RegisterARMThrottleTracing()
			wrapped := azclient.DefaultResourceClientTransport
			RegisterARMThrottleTracing()
			if azclient.DefaultResourceClientTransport != wrapped || wrapped.Timeout != client.Timeout {
				t.Fatal("registration changed client settings or wrapped twice")
			}
			options, err := azclient.GetDefaultResourceClientOption(nil)
			if err != nil {
				t.Fatal(err)
			}
			options.Retry.MaxRetries = -1
			pipeline := runtime.NewPipeline("tracing-test", "v1.0.0", runtime.PipelineOptions{}, &options.ClientOptions)
			ctx, span := startSpan(context.Background(), test.name)
			req, err := runtime.NewRequest(ctx, http.MethodGet, "https://management.azure.com/test")
			if err != nil {
				t.Fatal(err)
			}
			resp, requestErr := pipeline.Do(req)
			if resp != response {
				t.Fatal("response was replaced")
			}
			if test.status == http.StatusTooManyRequests && !errors.Is(requestErr, retryrepectthrottled.ErrTooManyRequest) {
				t.Fatalf("throttling error changed: %v", requestErr)
			}
			if test.status == http.StatusOK && requestErr != nil {
				t.Fatal(requestErr)
			}
			if resp != nil && resp.Body != nil {
				defer resp.Body.Close()
			}
			recordThrottleIfThrottled(ctx, requestErr)
			span.End()
			if err := provider.ForceFlush(context.Background()); err != nil {
				t.Fatal(err)
			}
			klog.Flush()
			out := buf.String()
			buf.Reset()
			if test.status == http.StatusOK {
				if strings.Contains(out, eventThrottled) {
					t.Fatalf("successful request recorded throttling: %s", out)
				}
				return
			}
			wantEvents := eventARMHTTPThrottled
			if test.wantDelay != "" {
				wantEvents += " retry_after=" + test.wantDelay
			}
			wantEvents += "; " + eventThrottled
			if !strings.Contains(out, `events="`+wantEvents+`"`) {
				t.Fatalf("want distinct HTTP and operation events %q: %s", wantEvents, out)
			}
		})
	}
}

// klogFlagsOnce ensures klog flags are registered on the default flag set only
// once across all tests in this file (registering twice panics).
var klogFlagsOnce sync.Once

// captureKlogAtSpanVerbosity redirects klog output to a buffer and raises the
// verbosity so span log lines (emitted at klogSpanVerbosity) are captured. It
// returns the buffer and a restore func the caller should defer.
func captureKlogAtSpanVerbosity(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	klogFlagsOnce.Do(func() {
		klog.InitFlags(nil)
	})
	if err := flag.Set("logtostderr", "false"); err != nil {
		t.Fatal(err)
	}
	if err := flag.Set("v", fmt.Sprintf("%d", klogSpanVerbosity)); err != nil {
		t.Fatal(err)
	}
	flag.Parse()

	buf := new(bytes.Buffer)
	klog.SetOutput(buf)

	return buf, func() {
		klog.Flush()
		// Restore logtostderr so later tests don't fall through to klog's
		// file-creation path and write logs to temp files.
		_ = flag.Set("logtostderr", "true")
		klog.SetOutput(nil)
	}
}

// TestTracingSpansVisibleInLogs is an end-to-end check that, with tracing
// enabled, a span produced via startSpan (with attributes, an event and a
// status) is exported to the container logs by the klogSpanExporter.
func TestTracingSpansVisibleInLogs(t *testing.T) {
	t.Setenv(tracesExporterEnv, "klog")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	t.Setenv("OTEL_SERVICE_NAME", "csi-azuredisk-controller")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=staging")
	buf, restore := captureKlogAtSpanVerbosity(t)
	defer restore()

	// InitOtelTracing mutates the global tracer provider; restore it so other
	// tests (e.g. TestTracingDisabledEmitsNoSpans) still see the default no-op.
	prevTP := otel.GetTracerProvider()
	defer otel.SetTracerProvider(prevTP)

	tp, err := InitOtelTracing()
	if err != nil {
		t.Fatalf("InitOtelTracing failed: %v", err)
	}
	defer func() {
		_ = tp.Shutdown(context.Background())
	}()

	// Simulate a handler: start a span, label it, record a throttle event,
	// then finish it successfully.
	ctx, span := startSpan(context.Background(), "AttachDisk",
		attribute.String(attrDiskName, "pvc-123"),
		attribute.String(attrNode, "aks-node-42"))
	recordThrottleEvent(ctx, eventThrottled, "30s")
	recordSpanResult(span, nil)
	span.End()

	// Batch span processor exports asynchronously; force a synchronous flush.
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush failed: %v", err)
	}
	klog.Flush()

	out := buf.String()
	t.Logf("captured klog output:\n%s", out)

	for _, want := range []string{
		"otel trace span",
		`span="AttachDisk"`,
		`service.name="csi-azuredisk-controller"`,
		`deployment.environment.name="staging"`,
		`disk.name="pvc-123"`,
		`node.name="aks-node-42"`,
		eventThrottled,
		"retry_after=30s",
		"traceID=",
		"durationMs=",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected span log output to contain %q, got:\n%s", want, out)
		}
	}
}

// TestTracingDisabledEmitsNoSpans verifies that when InitOtelTracing has not
// been called (default no-op tracer), startSpan produces no log output, so
// tracing has zero impact when disabled.
func TestTracingDisabledEmitsNoSpans(t *testing.T) {
	buf, restore := captureKlogAtSpanVerbosity(t)
	defer restore()

	_, span := startSpan(context.Background(), "AttachDisk")
	span.End()
	klog.Flush()

	if strings.Contains(buf.String(), "otel trace span") {
		t.Errorf("expected no span output when tracing is disabled, got:\n%s", buf.String())
	}
}

// TestRecordThrottleIfThrottled verifies that recordThrottleIfThrottled records
// a "throttled" event carrying the parsed Retry-After back-off when the error
// is an Azure ARM throttling error, and records nothing for nil/non-throttling
// errors.
func TestRecordThrottleIfThrottled(t *testing.T) {
	buf, restore := captureKlogAtSpanVerbosity(t)
	defer restore()

	// InitOtelTracing mutates the global tracer provider; restore it so other
	// tests (e.g. TestTracingDisabledEmitsNoSpans) still see the default no-op.
	prevTP := otel.GetTracerProvider()
	defer otel.SetTracerProvider(prevTP)

	tp, err := InitOtelTracing()
	if err != nil {
		t.Fatalf("InitOtelTracing failed: %v", err)
	}
	defer func() {
		_ = tp.Shutdown(context.Background())
	}()

	throttlingErr := errors.New("could not list storage accounts for account type Premium_LRS: Retriable: true, RetryAfter: 217s, HTTPStatusCode: 429, RawError: azure cloud provider throttled for operation StorageAccountListByResourceGroup with reason \"TooManyRequests\"")

	// Throttling error: expect a "throttled" event with retry_after=217s.
	ctx, span := startSpan(context.Background(), "AttachDisk")
	recordThrottleIfThrottled(ctx, throttlingErr)
	recordSpanResult(span, nil)
	span.End()

	// Non-throttling error and nil error: expect no throttle event.
	ctx2, span2 := startSpan(context.Background(), "DetachDisk")
	recordThrottleIfThrottled(ctx2, errors.New("some transient network error"))
	recordThrottleIfThrottled(ctx2, nil)
	recordSpanResult(span2, nil)
	span2.End()

	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush failed: %v", err)
	}
	klog.Flush()

	out := buf.String()
	t.Logf("captured klog output:\n%s", out)

	for _, want := range []string{
		`span="AttachDisk"`,
		eventThrottled,
		"retry_after=217s",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected throttled span log to contain %q, got:\n%s", want, out)
		}
	}

	// The DetachDisk span must not carry a throttle event.
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, `span="DetachDisk"`) && strings.Contains(line, eventThrottled) {
			t.Errorf("did not expect a throttle event on DetachDisk span, got:\n%s", line)
		}
	}
}

// TestDiskCorrelationPropagatesToChildSpans verifies that withDiskCorrelation
// labels the root span and, via baggage, auto-stamps the same disk.name onto
// child sub-spans started with startSpan without the caller passing it.
func TestDiskCorrelationPropagatesToChildSpans(t *testing.T) {
	buf, restore := captureKlogAtSpanVerbosity(t)
	defer restore()

	prevTP := otel.GetTracerProvider()
	defer otel.SetTracerProvider(prevTP)

	tp, err := InitOtelTracing()
	if err != nil {
		t.Fatalf("InitOtelTracing failed: %v", err)
	}
	defer func() {
		_ = tp.Shutdown(context.Background())
	}()

	// Root span for the RPC, then stamp the canonical correlation key.
	ctx, root := startSpan(context.Background(), "csi.v1.Controller/CreateVolume")
	ctx = withDiskCorrelation(ctx, "pvc-abc123")

	// A child sub-span started without passing disk.name explicitly.
	_, child := startSpan(ctx, "CreateManagedDisk")
	child.End()
	root.End()

	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush failed: %v", err)
	}
	klog.Flush()

	out := buf.String()
	t.Logf("captured klog output:\n%s", out)

	var childHasDiskName bool
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, `span="CreateManagedDisk"`) && strings.Contains(line, `disk.name="pvc-abc123"`) {
			childHasDiskName = true
		}
	}
	if !childHasDiskName {
		t.Errorf("expected child span CreateManagedDisk to inherit disk.name=pvc-abc123 via baggage, got:\n%s", out)
	}
}

func TestParseTracesExporters(t *testing.T) {
	tests := []struct {
		name           string
		value          string
		endpoint       string
		tracesEndpoint string
		wantKlog       bool
		wantOTLP       bool
	}{
		{name: "default without endpoint", wantKlog: true},
		{name: "whitespace without endpoint", value: "  ", wantKlog: true},
		{name: "default with endpoint", endpoint: "http://collector:4317", wantKlog: true, wantOTLP: true},
		{name: "default with traces endpoint", tracesEndpoint: "http://collector:4317", wantKlog: true, wantOTLP: true},
		{name: "whitespace with endpoint", value: "  ", endpoint: "http://collector:4317", wantKlog: true, wantOTLP: true},
		{name: "blank endpoints", endpoint: "  ", tracesEndpoint: "  ", wantKlog: true},
		{name: "klog", value: "klog", wantKlog: true},
		{name: "klog with endpoint", value: "klog", endpoint: "http://collector:4317", wantKlog: true},
		{name: "otlp", value: "otlp", wantOTLP: true},
		{name: "otlp with endpoint", value: "otlp", endpoint: "http://collector:4317", wantOTLP: true},
		{name: "otlp with traces endpoint", value: "otlp", tracesEndpoint: "http://collector:4317", wantOTLP: true},
		{name: "both", value: " KLOG, OTLP ", wantKlog: true, wantOTLP: true},
		{name: "none", value: "none"},
		{name: "none with endpoint", value: "none", endpoint: "http://collector:4317"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", test.endpoint)
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", test.tracesEndpoint)
			gotKlog, gotOTLP := parseTracesExporters(test.value)
			if gotKlog != test.wantKlog || gotOTLP != test.wantOTLP {
				t.Fatalf("parseTracesExporters(%q) = (%v, %v), want (%v, %v)", test.value, gotKlog, gotOTLP, test.wantKlog, test.wantOTLP)
			}
		})
	}
}

func TestInitOtelTracingHonorsSamplerEnvironment(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	t.Setenv(tracesExporterEnv, "none")

	previous := otel.GetTracerProvider()
	defer otel.SetTracerProvider(previous)

	provider, err := InitOtelTracing()
	if err != nil {
		t.Fatalf("InitOtelTracing failed: %v", err)
	}
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	_, span := startSpan(context.Background(), "unsampled")
	defer span.End()
	if span.IsRecording() {
		t.Fatal("span is recording with OTEL_TRACES_SAMPLER=always_off")
	}
}
