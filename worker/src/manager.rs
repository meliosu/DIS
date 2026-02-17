use reqwest::IntoUrl;
use reqwest::Url;

use common::constants::{MANAGER_REGISTER_PATH, MANAGER_UPDATE_TASK_PATH};
use common::types::{RegisterRequest, UpdateTaskQuery, UpdateTaskRequest};

pub struct Client {
    base: Url,
    client: reqwest::Client,
}

impl Client {
    pub fn new<U: IntoUrl>(addr: U) -> anyhow::Result<Self> {
        let url = addr.into_url()?;
        let client = reqwest::Client::builder()
            .tls_danger_accept_invalid_certs(true)
            .build()?;

        Ok(Self { client, base: url })
    }

    pub async fn register(&self, r: &RegisterRequest) -> anyhow::Result<()> {
        let url = self.base.join(MANAGER_REGISTER_PATH)?;
        let response = self.client.post(url).json(r).send().await?;
        response.error_for_status()?;

        Ok(())
    }

    pub async fn update(&self, q: &UpdateTaskQuery, r: &UpdateTaskRequest) -> anyhow::Result<()> {
        let url = self.base.join(MANAGER_UPDATE_TASK_PATH)?;
        let response = self.client.patch(url).query(q).json(r).send().await?;
        response.error_for_status()?;

        Ok(())
    }
}
