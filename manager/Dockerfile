FROM rust:1.93-slim AS build

WORKDIR /build
COPY Cargo.toml Cargo.lock .

RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && echo "fn dummy() {}" > src/lib.rs \
    && cargo build --release

COPY . .
RUN touch src/main.rs src/lib.rs && cargo build --release

FROM ubuntu:24.04 AS deploy

WORKDIR /app
COPY --from=build /build/target/release/manager /app/manager

ENTRYPOINT ["/app/manager"]
