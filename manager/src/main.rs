use anyhow::{anyhow, bail};

const EXTERNAL_PORT: u16 = 80;
const INTERNAL_PORT: u16 = 7123;

#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run().await {
        log::error!("FATAL: {e}");
    }
}

async fn run_service(port: u16, router: axum::Router) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| anyhow!("binding tcp listener: {e}"))?;

    let listen_addr = match listener.local_addr() {
        Ok(local_addr) => format!("{local_addr}"),
        _ => addr,
    };

    log::info!("listening on {listen_addr}");

    axum::serve(listener, router).await.map_err(|e| anyhow!("{e}"))
}

async fn run() -> anyhow::Result<()> {
    tokio::select! {
        internal_result = run_service(INTERNAL_PORT, manager::api::internal::router()) => {
            if let Err(e) = internal_result {
                bail!("internal service: {e}");
            }
        }

        external_result = run_service(EXTERNAL_PORT, manager::api::external::router()) => {
            if let Err(e) = external_result {
                bail!("external service: {e}");
            }
        }
    }

    Ok(())
}
