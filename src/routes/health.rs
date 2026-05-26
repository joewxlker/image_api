use rocket::{Route, routes, serde::json::Json};
use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReadyState {
    pub status: String
}

#[rocket::get("/ready")]
pub async fn get_ready_state() -> Json<ReadyState> {
    Json(ReadyState { status: String::from("ready") })
}

pub fn health_routes() -> Vec<Route> {
    routes![get_ready_state]
}