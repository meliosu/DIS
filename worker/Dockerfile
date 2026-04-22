FROM rust:1.95-slim AS build

WORKDIR /build
COPY ./common ./common

WORKDIR /build/worker
COPY ./worker/Cargo.toml ./worker/Cargo.lock .

RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && echo "fn dummy() {}" > src/lib.rs \
    && cargo build --release

COPY ./worker/src src
RUN touch src/main.rs src/lib.rs && cargo build --release

FROM ubuntu:25.04 AS deploy

WORKDIR /app
COPY --from=build /build/worker/target/release/worker /app/worker

ENTRYPOINT ["/app/worker"]
