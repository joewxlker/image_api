use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge as OtelBridge;
use opentelemetry_otlp::{LogExporter, Protocol, WithExportConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{BatchConfig, BatchLogProcessor, SdkLoggerProvider},
};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

use crate::services::log_service::ENV_FILTER;

pub async fn otel_logger(
    logs_endpoint: &Url,
    resource: &Resource,
) -> Result<SdkLoggerProvider, Box<dyn std::error::Error>> {
    if let Err(err) = reqwest::Client::new()
        .get(logs_endpoint.as_str())
        .send()
        .await
    {
        tracing::warn!(
            error = %err,
            endpoint = %logs_endpoint,
            "OTLP logs endpoint is unreachable; logs will still be buffered/export attempts will continue"
        );
    } else {
        tracing::debug!(
            endpoint = %logs_endpoint,
            "Successfully reached OTLP logs endpoint"
        );
    }

    let exporter = LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpJson)
        .with_endpoint(logs_endpoint.clone())
        .build()?;

    let processor = BatchLogProcessor::builder(exporter)
        .with_batch_config(BatchConfig::default())
        .build();

    let provider = SdkLoggerProvider::builder()
        .with_log_processor(processor)
        .with_resource(resource.clone())
        .build();

    let filter = ENV_FILTER.clone();

    tracing_subscriber::registry()
        .with(OtelBridge::new(&provider).with_filter(filter))
        .init();

    return Ok(provider);
}

pub async fn shutdown_otel_logging(logger_provider: SdkLoggerProvider) {
    if let Err(err) = logger_provider.shutdown() {
        tracing::error!(
            error = %err,
            "Failed to shut down logger provider"
        );
    } else {
        tracing::debug!("Logger provider shut down successfully");
    }
}
