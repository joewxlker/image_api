use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use rocket::fairing::{Fairing, Info, Kind};
use rocket::{Data, Request, Response};
use std::time::Instant;

const TIME_BOUNDARIES: &[f64] = &[
    0.001, 0.002, 0.005, 0.01, 0.02,
    0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 
    3.0, 5.0, 8.0, 10.0, 15.0, 20.0,
];

pub struct RequestMetricsFairing {
    requests_total: Counter<u64>,
    request_duration: Histogram<f64>,
    requests_errors: Counter<u64>,
}

impl RequestMetricsFairing {
    pub fn new() -> Self {
        let meter = global::meter("image_api");

        Self {
            requests_total: meter
                .u64_counter("http.requests.total")
                .with_description("Total number of HTTP requests")
                .build(),

            request_duration: meter
                .f64_histogram("http.request.duration.seconds")
                .with_unit("s")
                .with_boundaries(TIME_BOUNDARIES.into())
                .with_description("Duration of HTTP requests in seconds")
                .build(),

            requests_errors: meter
                .u64_counter("http.requests.errors")
                .with_description("Number of HTTP error responses (4xx and 5xx)")
                .build(),
        }
    }
}

#[rocket::async_trait]
impl Fairing for RequestMetricsFairing {
    fn info(&self) -> Info {
        Info {
            name: "Request Metrics Middleware",
            kind: Kind::Request | Kind::Response,
        }
    }

    async fn on_request(&self, request: &mut Request<'_>, _data: &mut Data<'_>) {
        request.local_cache(|| Instant::now());
    }

    async fn on_response<'r>(&self, request: &'r Request<'_>, response: &mut Response<'r>) {
        let start = request.local_cache(|| Instant::now());
        let duration = start.elapsed().as_secs_f64();

        let method = request.method().as_str().to_string();
        let route = request
            .route()
            .map(|r| r.uri.path().to_string())
            .unwrap_or_else(|| request.uri().path().to_string());

        let status = response.status().code;

        self.requests_total.add(
            1,
            &[
                KeyValue::new("method", method.clone()),
                KeyValue::new("route", route.clone()),
                KeyValue::new("status", status.to_string()),
                KeyValue::new("handler", get_handler_name(request)),
            ],
        );

        self.request_duration.record(
            duration,
            &[
                KeyValue::new("method", method.clone()),
                KeyValue::new("route", route.clone()),
                KeyValue::new("status", status.to_string()),
            ],
        );

        if status >= 400 {
            self.requests_errors.add(
                1,
                &[
                    KeyValue::new("method", method),
                    KeyValue::new("route", route),
                    KeyValue::new("status", status.to_string()),
                ],
            );
        }
    }
}

fn get_handler_name(request: &Request<'_>) -> String {
    request
        .route()
        .and_then(|r| r.name.clone())
        .map(|name| name.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
