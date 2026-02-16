use axum::routing::{get, post};

pub fn router() -> axum::Router {
    axum::Router::new()
        .route("/api/hash/crack", post(crack_hash))
        .route("/api/hash/status", get(get_crack_status))
}

async fn crack_hash() {

}

async fn get_crack_status() {

}
