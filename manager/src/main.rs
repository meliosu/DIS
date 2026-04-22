#[tokio::main]
async fn main() {
    env_logger::init();

    if let Err(e) = run().await {
        log::error!("{e}");
    }
}

async fn run() -> anyhow::Result<()> {
    todo!()
}
