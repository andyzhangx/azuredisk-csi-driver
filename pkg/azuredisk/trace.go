/*
Copyright 2023 The Kubernetes Authors.

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
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"k8s.io/klog/v2"
	azureutils "sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
)

type armThrottleTracingTransport struct {
	http.RoundTripper
}

// RegisterARMThrottleTracing observes ARM responses before the SDK's throttling
// policy discards their headers. Call only at startup, before creating clients.
func RegisterARMThrottleTracing() {
	client := azclient.DefaultResourceClientTransport
	if _, ok := client.Transport.(*armThrottleTracingTransport); ok {
		return
	}
	tracingClient := *client
	tracingClient.Transport = &armThrottleTracingTransport{RoundTripper: client.Transport}
	azclient.DefaultResourceClientTransport = &tracingClient
}

func (t *armThrottleTracingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.RoundTripper.RoundTrip(req)
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After"))
		// Preserve HTTP-date values; label numeric delays as seconds without a sleep-policy cap.
		if _, parseErr := strconv.ParseUint(retryAfter, 10, 64); parseErr == nil {
			retryAfter += "s"
		}
		recordThrottleEvent(req.Context(), eventARMHTTPThrottled, retryAfter)
	}
	return resp, err
}

const (
	// tracerName is attached to every span as the instrumentation scope so
	// spans produced by this driver can be distinguished from library spans.
	tracerName = "sigs.k8s.io/azuredisk-csi-driver"

	// spanVerbosityEnv overrides the klog verbosity at which span lines are
	// written. Operators who want span lines at a different level than the
	// default can set it (e.g. "4") without changing the driver's global -v.
	spanVerbosityEnv = "OTEL_KLOG_SPAN_VERBOSITY"

	// tracesExporterEnv selects which span exporters are enabled.
	tracesExporterEnv = "OTEL_TRACES_EXPORTER"

	// defaultKlogSpanVerbosity is the default klog verbosity at which spans are
	// logged. It is deliberately low (V(2)) so that enabling tracing produces
	// usable span lines at normal production verbosity, without requiring the
	// whole driver to run at V(4).
	defaultKlogSpanVerbosity klog.Level = 2
)

// klogSpanVerbosity is the klog verbosity level at which spans are logged. It
// defaults to defaultKlogSpanVerbosity and can be overridden at startup via the
// spanVerbosityEnv environment variable (see InitOtelTracing).
var klogSpanVerbosity = defaultKlogSpanVerbosity

// Span attribute keys used across handlers so root-span labeling stays
// consistent and traces are easy to correlate by disk, node or volume.
const (
	attrDiskURI    = "disk.uri"
	attrDiskName   = "disk.name"
	attrNode       = "node.name"
	attrVolumeID   = "volume.id"
	attrRetryAfter = "retry_after"

	// eventThrottled marks an ARM/library throttling back-off inside a span.
	eventThrottled = "throttled"

	// eventARMHTTPThrottled identifies an HTTP 429 response, not the operation outcome.
	eventARMHTTPThrottled = "arm_http_throttled"

	// correlationBaggageKey is the OTel baggage key under which the canonical
	// per-request correlation value (the disk name, e.g. pvc-<uid>) is carried
	// so that startSpan can stamp it onto every child sub-span of an RPC.
	correlationBaggageKey = attrDiskName
)

// noisySpanNames are health-check and capability-discovery RPCs that the
// kubelet and liveness-probe sidecar call on a timer. They carry no useful
// tracing signal and would otherwise flood the logs, so they are not exported
// to klog. Real operations (CreateVolume, ControllerPublishVolume, etc.) are
// unaffected.
var noisySpanNames = map[string]struct{}{
	"csi.v1.Identity/Probe":                       {},
	"csi.v1.Identity/GetPluginInfo":               {},
	"csi.v1.Identity/GetPluginCapabilities":       {},
	"csi.v1.Node/NodeGetCapabilities":             {},
	"csi.v1.Node/NodeGetVolumeStats":              {},
	"csi.v1.Controller/ControllerGetCapabilities": {},
}

// tracer returns the driver's tracer. Until InitOtelTracing installs a real
// TracerProvider this resolves to the global no-op tracer, so calling startSpan
// before (or when tracing is disabled) is safe and free.
func tracer() oteltrace.Tracer {
	return otel.Tracer(tracerName)
}

// startSpan starts a child span named name off of ctx and returns the derived
// context (carrying the new span) together with the span itself. Callers must
// call span.End() when the traced work completes, typically via defer.
//
// When tracing is disabled the global provider is a no-op, so the returned span
// does nothing and the call is effectively free. This lets handlers instrument
// I/O boundaries unconditionally without guarding every call site.
func startSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	ctx, span := tracer().Start(ctx, name)
	// Inherit the request's canonical correlation key (disk name) from baggage
	// so every sub-span groups under the same disk without each call site
	// having to pass it explicitly.
	if v := baggage.FromContext(ctx).Member(correlationBaggageKey).Value(); v != "" {
		span.SetAttributes(attribute.String(attrDiskName, v))
	}
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
	return ctx, span
}

// withDiskCorrelation stamps diskName as the canonical correlation key for the
// whole request. It labels the current root span and stores the value in OTel
// baggage so startSpan copies it onto every child sub-span. It returns the
// enriched context; callers must use the returned context for downstream work.
// An empty diskName returns ctx unchanged.
func withDiskCorrelation(ctx context.Context, diskName string) context.Context {
	if diskName == "" {
		return ctx
	}
	// Label the root span of this RPC.
	oteltrace.SpanFromContext(ctx).SetAttributes(attribute.String(attrDiskName, diskName))
	// Propagate onto child sub-spans via baggage.
	if member, err := baggage.NewMember(correlationBaggageKey, diskName); err == nil {
		if bag, err := baggage.FromContext(ctx).SetMember(member); err == nil {
			ctx = baggage.ContextWithBaggage(ctx, bag)
		}
	}
	return ctx
}

// recordSpanResult sets the span status from err. On error it also records the
// error on the span. It is safe to call on a no-op span.
func recordSpanResult(span oteltrace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return
	}
	span.SetStatus(otelcodes.Ok, "")
}

// recordThrottleEvent records a library-level throttling/back-off delay as an
// event on the span currently associated with ctx, so waits caused by ARM
// "Retry-After" or client-side rate limiting are visible inside the trace.
func recordThrottleEvent(ctx context.Context, eventName, retryAfter string) {
	span := oteltrace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attrs := []attribute.KeyValue{}
	if retryAfter != "" {
		attrs = append(attrs, attribute.String(attrRetryAfter, retryAfter))
	}
	span.AddEvent(eventName, oteltrace.WithAttributes(attrs...))
}

// recordThrottleIfThrottled inspects err and, when it is an Azure ARM
// throttling error, records a "throttled" event on the span in ctx together
// with the parsed Retry-After back-off. It is a no-op for nil/non-throttling
// errors and when tracing is disabled (non-recording span).
func recordThrottleIfThrottled(ctx context.Context, err error) {
	if err == nil || !azureutils.IsThrottlingError(err) {
		return
	}
	retryAfter := ""
	if s := azureutils.GetRetryAfterSeconds(err); s > 0 {
		retryAfter = fmt.Sprintf("%ds", s)
	}
	recordThrottleEvent(ctx, eventThrottled, retryAfter)
}

// klogSpanExporter is a trace.SpanExporter that writes finished spans to klog.
// This makes traces visible through standard container-log collection without
// requiring an OTLP collector to be deployed.
type klogSpanExporter struct{}

var _ trace.SpanExporter = (*klogSpanExporter)(nil)

// ExportSpans formats each finished span as a single structured klog line
// describing its name, duration, identifiers, resource and span attributes,
// events and status.
func (e *klogSpanExporter) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	logger := klog.V(klogSpanVerbosity)
	if !logger.Enabled() {
		return nil
	}
	for _, span := range spans {
		if _, noisy := noisySpanNames[span.Name()]; noisy {
			continue
		}
		logger.InfoS("otel trace span", spanKeysAndValues(span)...)
	}
	return nil
}

// Shutdown is a no-op; there is nothing to flush or close for klog output.
func (e *klogSpanExporter) Shutdown(_ context.Context) error { return nil }

// spanKeysAndValues renders a ReadOnlySpan into a flat list of key/value pairs
// suitable for klog structured logging.
func spanKeysAndValues(span trace.ReadOnlySpan) []interface{} {
	sc := span.SpanContext()
	kv := []interface{}{
		"span", span.Name(),
		"traceID", sc.TraceID().String(),
		"spanID", sc.SpanID().String(),
	}
	if parent := span.Parent(); parent.HasSpanID() {
		kv = append(kv, "parentSpanID", parent.SpanID().String())
	}
	kv = append(kv, "durationMs", span.EndTime().Sub(span.StartTime()).Milliseconds())

	for _, attr := range span.Resource().Attributes() {
		kv = append(kv, string(attr.Key), attr.Value.Emit())
	}

	for _, attr := range span.Attributes() {
		kv = append(kv, string(attr.Key), attr.Value.Emit())
	}

	if events := span.Events(); len(events) > 0 {
		var b strings.Builder
		for i, ev := range events {
			if i > 0 {
				b.WriteString("; ")
			}
			b.WriteString(ev.Name)
			for _, attr := range ev.Attributes {
				fmt.Fprintf(&b, " %s=%v", attr.Key, attr.Value.Emit())
			}
		}
		kv = append(kv, "events", b.String())
	}

	if status := span.Status(); status.Code != 0 {
		kv = append(kv, "status", status.Code.String())
		if status.Description != "" {
			kv = append(kv, "statusMessage", status.Description)
		}
	}
	return kv
}

// InitOtelTracing initializes and registers a global OpenTelemetry
// TracerProvider for the driver. Spans are exported to the container logs via
// klog by default and additionally over OTLP/gRPC when an endpoint is configured.
// OTEL_TRACES_EXPORTER overrides this selection; explicit OTLP selection uses
// the SDK's endpoint defaults when no endpoint is configured. The returned
// TracerProvider must be shut down on exit to flush any buffered spans. It is
// only called when tracing is enabled, so there is zero cost when tracing is
// disabled.
func InitOtelTracing() (*trace.TracerProvider, error) {
	ctx := context.Background()

	// Allow operators to override the span log verbosity without changing the
	// driver's global -v level.
	if v := strings.TrimSpace(os.Getenv(spanVerbosityEnv)); v != "" {
		if lvl, err := strconv.Atoi(v); err == nil && lvl >= 0 && lvl <= math.MaxInt32 {
			klogSpanVerbosity = klog.Level(lvl)
		} else {
			klog.Warningf("otel tracing: ignoring invalid %s=%q", spanVerbosityEnv, v)
		}
	}

	// Resource will auto populate spans with common attributes.
	res, err := resource.New(ctx,
		resource.WithFromEnv(), // pull attributes from OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME environment variables
		resource.WithProcess(),
		resource.WithOS(),
		resource.WithContainer(),
		resource.WithHost(),
	)
	if err != nil {
		klog.ErrorS(err, "Failed to create the otel resource, spans will lack some metadata")
	}

	opts := []trace.TracerProviderOption{
		trace.WithResource(res),
	}

	enableKlog, enableOTLP := parseTracesExporters(os.Getenv(tracesExporterEnv))
	if enableKlog {
		opts = append(opts, trace.WithBatcher(&klogSpanExporter{}))
	}
	if enableOTLP {
		exporter, exporterErr := otlptrace.New(ctx, otlptracegrpc.NewClient())
		if exporterErr != nil {
			klog.ErrorS(exporterErr, "otel tracing: failed to create OTLP exporter; continuing without it")
		} else {
			opts = append(opts, trace.WithBatcher(exporter))
		}
	}

	traceProvider := trace.NewTracerProvider(opts...)

	// Register the trace provider as global.
	otel.SetTracerProvider(traceProvider)

	return traceProvider, nil
}

func parseTracesExporters(value string) (klogEnabled, otlpEnabled bool) {
	if strings.TrimSpace(value) == "" {
		return true, strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != "" ||
			strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")) != ""
	}
	for _, exporter := range strings.Split(value, ",") {
		switch strings.ToLower(strings.TrimSpace(exporter)) {
		case "klog":
			klogEnabled = true
		case "otlp":
			otlpEnabled = true
		case "none":
			return false, false
		}
	}
	return
}
