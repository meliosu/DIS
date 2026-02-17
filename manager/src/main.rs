use anyhow::{anyhow, bail};

use common::constants::MANAGER_INTERNAL_PORT;
use manager::config::CONFIG;
use manager::constants::EXTERNAL_PORT;

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run().await {
        log::error!("FATAL: {e}");
    }
}

async fn run_service(name: &str, port: u16, router: axum::Router) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| anyhow!("binding tcp listener: {e}"))?;

    let listen_addr = match listener.local_addr() {
        Ok(local_addr) => format!("{local_addr}"),
        _ => addr,
    };

    log::info!("{name} listening on {listen_addr}");

    axum::serve(listener, router)
        .await
        .map_err(|e| anyhow!("{e}"))
}

async fn run() -> anyhow::Result<()> {
    log::info!("timeout {:?}, alphabet {}", CONFIG.timeout, CONFIG.alphabet);

    let state = manager::state::State::default();

    {
        let state = state.clone();
        tokio::spawn(async move {
            let interval = CONFIG.timeout / 2;

            loop {
                tokio::time::sleep(interval).await;
                manager::redistribute::redistribute(&state).await;
            }
        });
    }

    tokio::select! {
        internal_result = run_service("internal service", MANAGER_INTERNAL_PORT, manager::api::internal::router(state.clone())) => {
            if let Err(e) = internal_result {
                bail!("internal service: {e}");
            }
        }

        external_result = run_service("external service", EXTERNAL_PORT, manager::api::external::router(state.clone())) => {
            if let Err(e) = external_result {
                bail!("external service: {e}");
            }
        }
    }

    Ok(())
}
