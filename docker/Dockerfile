ARG RUST_VERSION=1.97.1
FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /src
COPY onedis /src/onedis
COPY tantivy /src/tantivy
RUN cargo build --locked --release --manifest-path /src/onedis/Cargo.toml

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 onedis \
    && useradd --uid 10001 --gid onedis --no-create-home --shell /usr/sbin/nologin onedis \
    && install -d -o onedis -g onedis /etc/onedis /var/lib/onedis/db /var/lib/onedis/wal

COPY --from=builder /src/onedis/target/release/onedis-server /usr/local/bin/onedis-server
COPY --chown=onedis:onedis onedis/docker/onedis.toml /etc/onedis/onedis.toml

USER onedis:onedis
WORKDIR /var/lib/onedis
ENV RUST_LOG=info
EXPOSE 6379 9121
VOLUME ["/var/lib/onedis"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["curl", "--fail", "--silent", "http://127.0.0.1:9121/readyz"]
ENTRYPOINT ["/usr/local/bin/onedis-server"]
CMD ["--config", "/etc/onedis/onedis.toml"]
