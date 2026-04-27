use std::sync::{Arc, LazyLock};

use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    filter::Filtered,
    fmt::format::{Compact, DefaultFields},
    layer::{Layered, SubscriberExt},
    util::SubscriberInitExt,
};

use crate::services::log_service::ENV_FILTER;

pub type SimpleLogger = Arc<
    Layered<
        Filtered<
            tracing_subscriber::fmt::Layer<
                Registry,
                DefaultFields,
                tracing_subscriber::fmt::format::Format<Compact>,
            >,
            EnvFilter,
            Registry,
        >,
        Registry,
    >,
>;

/// Holds a cloneable stdout logger provider with the filter [ENV_FILTER]
/// 
/// ## Examples
/// ```rust
/// async fn some_async_task() {
///     tracing::info!("This will log to the terminal");
/// }
/// 
/// // Async
/// some_async_task()
///     .with_subscriber(STDOUT_LOGGER.clone())
///     .await;
/// 
/// // Sync
/// with_default(registry, || {
///     tracing::info!("This will log to the terminal")
/// });
/// 
/// ```
pub static STDOUT_LOGGER: LazyLock<SimpleLogger> = LazyLock::new(|| {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(ENV_FILTER.clone());

    let registry = Registry::default().with(fmt_layer);

    Arc::new(registry)
});

/// Sets the global log subscriber to stdout with the filter [ENV_FILTER]
/// 
/// # Panics
/// Calling multiple 'global-subscriber' setting functions will panic 
pub fn set_stdout_logger() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(ENV_FILTER.clone());

    Registry::default().with(fmt_layer).init();
}
