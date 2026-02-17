use common::constants::WORKER_HEALTHCHECK_PATH;
use reqwest::IntoUrl;
use reqwest::Url;

use common::types::CreateTaskRequest;
use common::constants::WORKER_CRACK_TASK_PATH;

#[derive(Clone, Debug)]
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

        Ok(Self {
            client,
            base: url,
        })
    }

    pub async fn create_task(&self, r: &CreateTaskRequest) -> anyhow::Result<()> {
        let url = self.base.join(WORKER_CRACK_TASK_PATH)?;
        let response = self.client.post(url).json(r).send().await?;
        response.error_for_status()?;

        Ok(())
    }

    pub async fn healthcheck(&self) -> anyhow::Result<()> {
        let url = self.base.join(WORKER_HEALTHCHECK_PATH)?;
        let response = self.client.get(url).send().await?;
        response.error_for_status()?;

        Ok(())
    }
}
