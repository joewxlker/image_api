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

pub static STDOUT_LOGGER: LazyLock<SimpleLogger> = LazyLock::new(|| {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(ENV_FILTER.clone());

    let registry = Registry::default().with(fmt_layer);

    Arc::new(registry)
});

pub fn stdout_logger() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(ENV_FILTER.clone());

    Registry::default().with(fmt_layer).init();
}
