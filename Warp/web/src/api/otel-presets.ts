// Real, documented OTLP destination presets for the Observability page's Export config generator
// -- NOT config Warp writes or applies anywhere. WARP_OTEL_PROTOCOL/ENDPOINT/HEADERS are env-var
// only (see WarpTelemetry.fromEnv()); there is no persisted-config write path for them the way
// otlpEnabled/prometheusEnabled have via PATCH /api/observability. This module only generates the
// text an operator would set and restart with -- endpoints/header names are real, documented
// values from each vendor's own OTLP-ingestion docs; API keys/instance IDs are always placeholders
// the operator fills in themselves, never a real credential.

export type PresetMaturity = 'available' | 'collector' | 'planned'
export type PresetKind = 'otlp' | 'prometheus-scrape'

export interface OtelPreset {
  id: string
  label: string
  sub: string
  maturity: PresetMaturity
  kind: PresetKind
  /** Only for kind: 'otlp'. */
  protocol?: 'grpc' | 'http'
  endpointPlaceholder?: string
  headerPlaceholder?: string
  /** Real, disclosed context: why this maturity, what Warp capability it relies on. */
  notes: string
}

export const OTEL_PRESETS: OtelPreset[] = [
  {
    id: 'otel-collector', label: 'OTel Collector', sub: 'recommended · fan-out', maturity: 'available', kind: 'otlp',
    protocol: 'grpc', endpointPlaceholder: 'http://otel-collector:4317', headerPlaceholder: '',
    notes: "A generic OTLP receiver -- exactly what WarpTelemetry's exporter already speaks. The Collector then fans out to any backend its own exporters support (Datadog, CloudWatch, Azure, GCP, ...), which is why this is the recommended path for anything Warp can't reach directly.",
  },
  {
    id: 'grafana-cloud', label: 'Grafana Cloud', sub: 'direct OTLP', maturity: 'available', kind: 'otlp',
    protocol: 'http', endpointPlaceholder: 'https://otlp-gateway-<region>.grafana.net/otlp', headerPlaceholder: 'Authorization=Basic <base64(instanceID:apiKey)>',
    notes: 'Grafana Cloud accepts OTLP directly over HTTP with a Basic-auth header -- find your exact endpoint and instance ID under your stack’s "OTLP Endpoint" page in the Grafana Cloud portal; the values above are placeholders, not real credentials.',
  },
  {
    id: 'datadog', label: 'Datadog', sub: 'direct OTLP intake', maturity: 'available', kind: 'otlp',
    protocol: 'grpc', endpointPlaceholder: 'https://otlp.datadoghq.com:4317', headerPlaceholder: 'DD-API-KEY=<your Datadog API key>',
    notes: "Datadog's OTLP intake accepts a direct export with just an API-key header -- no Agent or Collector required for metrics. Use datadoghq.eu (or your region's intake host) if your Datadog org isn't on the US1 site.",
  },
  {
    id: 'new-relic', label: 'New Relic', sub: 'direct OTLP', maturity: 'available', kind: 'otlp',
    protocol: 'grpc', endpointPlaceholder: 'https://otlp.nr-data.net:4317', headerPlaceholder: 'api-key=<your New Relic license key>',
    notes: "New Relic's OTLP intake takes a license key as a plain header -- this is the exact SaaS pattern WarpTelemetry's own WARP_OTEL_HEADERS support was built for.",
  },
  {
    id: 'aws-cloudwatch', label: 'AWS CloudWatch', sub: 'ADOT + awsemf exporter', maturity: 'collector', kind: 'otlp',
    notes: "CloudWatch's native metric format (awsemf) isn't something raw OTLP can reach directly -- route through an OTel Collector (the AWS Distro for OpenTelemetry, ADOT) configured with the awsemf exporter. Point WARP_OTEL_ENDPOINT at that Collector, not at AWS.",
  },
  {
    id: 'azure-monitor', label: 'Azure Monitor', sub: 'Collector + azuremonitor exporter', maturity: 'collector', kind: 'otlp',
    notes: "Same shape as CloudWatch: Azure Monitor's ingestion format needs an OTel Collector with the azuremonitor exporter translating in between. Warp's OTLP export can't speak this format directly.",
  },
  {
    id: 'gcp-monitoring', label: 'GCP Monitoring', sub: 'Collector + googlecloud exporter', maturity: 'collector', kind: 'otlp',
    notes: "Same shape again: Google Cloud Monitoring needs an OTel Collector with the googlecloud exporter. Warp's generic OTLP export has no direct path to GCP's ingestion API.",
  },
  {
    id: 'splunk', label: 'Splunk Observability', sub: 'OTLP via Collector', maturity: 'planned', kind: 'otlp',
    notes: 'Not yet supported by Warp -- shown to illustrate how a future vendor addition would appear in this list, not a real destination today.',
  },
  {
    id: 'prometheus', label: 'Prometheus', sub: 'scrape Warp directly', maturity: 'available', kind: 'prometheus-scrape',
    notes: "No OTLP export needed at all -- Warp's own /metrics endpoint is a real, always-available (unless an admin disabled it) Prometheus exposition target. Point your Prometheus server's own scrape_configs at it.",
  },
]
