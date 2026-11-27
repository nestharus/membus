# Multi-stage container image for Membus v2 and membus-ab-router using immutable digest-pinned official bases.
# Builder: official rust:1.98.1-slim-bookworm
FROM rust@sha256:ff521445a372125ed4f76e1453a1f8098f2d05332d1601d30db1c1f62757e730 AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        clang \
        cmake \
        g++ \
        libclang-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Cache dependency compilation before copying full source tree.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin \
    && echo "fn main() {}" > src/main.rs \
    && echo "fn main() {}" > src/bin/membus-ab-router.rs \
    && echo "" > src/lib.rs \
    && cargo build --release --locked --bin membus --bin membus-ab-router \
    && rm -rf src

COPY src ./src
RUN touch src/lib.rs src/main.rs src/bin/membus-ab-router.rs \
    && cargo build --release --locked --bin membus --bin membus-ab-router

# Runtime: official debian:bookworm-slim
FROM debian@sha256:e67eeead736ff7bbe258ef088bff683635c561b1fcfefad0806c1df4ab49ce0e AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 membus \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin membus \
    && mkdir -p /var/lib/membus /var/lib/membus/checkpoints \
    && chown -R 10001:10001 /var/lib/membus

COPY --from=builder /src/target/release/membus /usr/local/bin/membus
COPY --from=builder /src/target/release/membus-ab-router /usr/local/bin/membus-ab-router

USER 10001:10001
WORKDIR /var/lib/membus

EXPOSE 8080 8081 8443
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/membus"]
CMD ["serve", "--data-dir", "/var/lib/membus", "--http-listen", "127.0.0.1:8080"]
