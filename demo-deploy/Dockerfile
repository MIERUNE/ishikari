# syntax=docker/dockerfile:1.7

# Release image for Ishikari (pure Rust: PMTiles + HTTP).
#
# glibc build on distroless/cc: mature malloc and DNS behave well under high QPS
# with no allocator tuning. The one rule is that the builder and runtime must
# share the same Debian generation so their glibc matches — here both are
# trixie / Debian 13 (glibc 2.41): `rust:1.96-slim-trixie` and
# `distroless/cc-debian13`. Ishikari is pure Rust and compiles against trixie's
# glibc, so this newer base is safe for the demo image.
# Build multi-arch with `docker buildx --platform linux/amd64,linux/arm64` if
# the target cluster supports Arm nodes. The checked-in GKE demo currently uses
# linux/amd64 because its asia-northeast1 Autopilot cluster cannot provision Arm
# workloads.
FROM rust:1.96-slim-trixie AS builder
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY ishikari/Cargo.toml ishikari/Cargo.toml
COPY ishikari-sim/Cargo.toml ishikari-sim/Cargo.toml
COPY ishikari-terrain/Cargo.toml ishikari-terrain/Cargo.toml
COPY ishikari/src ishikari/src
COPY ishikari-sim/src ishikari-sim/src
COPY ishikari-terrain/src ishikari-terrain/src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release --locked -p ishikari \
    && cp target/release/ishikari /ishikari

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /ishikari /ishikari
# Public HTTP ingress + cluster-internal listener (/_internal/*, metrics, peer
# forwarding) + gossip.
EXPOSE 8080 9090 7946
ENTRYPOINT ["/ishikari"]
