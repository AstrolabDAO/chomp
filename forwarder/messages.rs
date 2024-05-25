use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Deserialize)]
pub struct WsRequest {
  pub method: String,
  pub resources: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct WsResponse {
  pub status: String,
  pub message: String,
}

#[derive(Clone)]
pub struct Client {
  pub id: String,
  pub ip: String,
  pub subscriptions: HashSet<String>,
}
