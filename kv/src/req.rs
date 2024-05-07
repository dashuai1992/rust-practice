use serde::{Deserialize, Serialize};

use crate::kv::command::Command;

#[derive(Debug, Deserialize, Serialize)]
pub struct Request {
  pub command: Command,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
  pub result: Result<Option<String>, String>
}