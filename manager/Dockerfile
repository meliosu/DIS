FROM rust:1.93-slim AS build

WORKDIR /build
COPY ./common ./common

WORKDIR /build/manager
COPY ./manager/Cargo.toml ./manager/Cargo.lock .

RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && echo "fn dummy() {}" > src/lib.rs \
    && cargo build --release

COPY ./manager/src src
RUN touch src/main.rs src/lib.rs && cargo build --release

FROM ubuntu:24.04 AS deploy

WORKDIR /app
COPY --from=build /build/manager/target/release/manager /app/manager

ENTRYPOINT ["/app/manager"]
