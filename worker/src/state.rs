use std::{ops::Deref, sync::Arc};

#[derive(Clone)]
pub struct State {
    inner: Arc<InnerState>,
}

impl Deref for State {
    type Target = InnerState;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl State {
    pub fn new(client: crate::manager::Client) -> Self {
        Self {
            inner: Arc::new(InnerState { client }),
        }
    }
}

pub struct InnerState {
    pub client: crate::manager::Client,
}
