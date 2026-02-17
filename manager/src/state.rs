use tokio::sync::Mutex;

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Default, Clone)]
pub struct State {
    inner: Arc<InnerState>,
}

impl Deref for State {
    type Target = InnerState;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Default)]
pub struct InnerState {
    pub workers: Mutex<Vec<Worker>>,
    pub requests: Mutex<HashMap<uuid::Uuid, Arc<Mutex<Crack>>>>,
}

#[derive(Clone, Debug)]
pub struct Worker {
    pub address: String,
    pub client: crate::worker::Client,
}

#[derive(Debug)]
pub struct CrackWorker {
    pub last_update: tokio::time::Instant,
    pub worker: Worker,
    pub start: usize,
    pub curr: usize,
    pub end: usize,
}

pub struct Crack {
    pub hash: String,
    pub alphabet: String,
    pub max_length: usize,
    pub total_count: usize,
    pub data: Vec<String>,
    pub workers: Vec<CrackWorker>,
}
