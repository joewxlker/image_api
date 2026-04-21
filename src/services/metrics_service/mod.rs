use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};

use crate::{config::Config, services::metrics_service::otel::otel_metrics};

pub mod otel;

pub async fn initialize_metrics(
    config: &Config,
) -> Result<Option<SdkMeterProvider>, Box<dyn std::error::Error>> {
    if config.otlp.collect_metrics && config.otlp.collector_endpoint.is_some() {
        let collector_endpoint = config.otlp.collector_endpoint.clone().unwrap();

        let metrics_endpoint = collector_endpoint.join("/v1/metrics")?;
        let resource = Resource::from(config);

        tracing::info!(
            endpoint = %metrics_endpoint,
            "OTLP metrics enabled; initializing exporter"
        );

        let provider = otel_metrics(metrics_endpoint.clone(), resource).await?;

        tracing::info!(
            endpoint = %metrics_endpoint,
            "OTLP metrics initialized successfully"
        );

        return Ok(Some(provider));
    } else if config.otlp.collect_metrics && config.otlp.collector_endpoint.is_none() {
        tracing::warn!(
            "OTLP metrics export is enabled, but no collector endpoint is configured; metrics will not be exported"
        );
    } else {
        tracing::info!("OTLP metrics export is disabled by configuration");
    }

    Ok(None)
}