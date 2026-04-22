use opentelemetry::global;
use opentelemetry_otlp::{MetricExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
};
use url::Url;

pub async fn otel_metrics(
    metrics_endpoint: &Url,
    resource: &Resource,
) -> Result<SdkMeterProvider, Box<dyn std::error::Error>> {
    if let Err(err) = reqwest::Client::new()
        .get(metrics_endpoint.as_str())
        .send()
        .await
    {
        tracing::warn!(
            error = %err,
            endpoint = %metrics_endpoint,
            "OTLP metrics endpoint is unreachable; export attempts will still be configured"
        );
    } else {
        tracing::debug!(
            endpoint = %metrics_endpoint,
            "Successfully reached OTLP metrics endpoint"
        );
    }

    let exporter = MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .with_endpoint(metrics_endpoint.clone())
        .build()?;

    let reader = PeriodicReader::builder(exporter).build();

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource.clone())
        .with_reader(reader)
        .build();

    global::set_meter_provider(meter_provider.clone());

    return Ok(meter_provider);
}

pub async fn shutdown_otel_metrics(meter_provider: SdkMeterProvider) {
    if let Err(err) = meter_provider.shutdown() {
        tracing::error!(
            error = %err,
            "Failed to shut down metrics provider"
        );
    } else {
        tracing::debug!("Metrics provider shut down successfully");
    }
}
