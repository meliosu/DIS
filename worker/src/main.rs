use anyhow::anyhow;

use common::{constants::MANAGER_INTERNAL_PORT, types::RegisterRequest};
use worker::constants::{INTERNAL_PORT, MANAGER_HOSTNAME};

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run().await {
        log::error!("{e}");
    }
}

async fn run() -> anyhow::Result<()> {
    let manager_addr = format!("http://{MANAGER_HOSTNAME}:{MANAGER_INTERNAL_PORT}");

    let client = worker::manager::Client::new(manager_addr)
        .map_err(|e| anyhow!("creating manager client: {e}"))?;

    let hostname =
        std::env::var("HOSTNAME").map_err(|e| anyhow!("getting HOSTNAME variable: {e}"))?;

    let worker_address = format!("{hostname}:{INTERNAL_PORT}");

    client
        .register(&RegisterRequest { worker_address })
        .await
        .map_err(|e| anyhow!("registering with manager: {e}"))?;

    let addr = format!("0.0.0.0:{INTERNAL_PORT}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| anyhow!("binding tcp listener: {e}"))?;

    let addr = match listener.local_addr() {
        Ok(local_addr) => format!("{local_addr}"),
        _ => addr,
    };

    log::info!("listening on {addr}");

    let state = worker::state::State::new(client);

    axum::serve(listener, worker::api::internal::router(state)).await?;

    Ok(())
}
