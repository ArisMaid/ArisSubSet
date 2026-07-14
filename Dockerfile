# syntax=docker/dockerfile:1.7

FROM node:24-bookworm AS web-builder
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web ./
RUN npm run build

FROM rust:1.96-bookworm AS builder
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release \
    && cp /src/target/release/ass-subset-service /tmp/ass-subset-service

FROM python:3.11-slim-bookworm
ENV LISTEN_ADDR=0.0.0.0:8080 \
    FONT_DIRS=/fonts \
    WATCH_DIRS=/watch \
    BACKUP_DIR=/backups \
    DATA_DIR=/data \
    FONT_WORKER_PATH=/app/workers/font_worker.py \
    PYTHON_BIN=python3
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && apt-get clean
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=builder /tmp/ass-subset-service /usr/local/bin/ass-subset-service
COPY workers ./workers
COPY --from=web-builder /web/dist ./web
VOLUME ["/fonts", "/watch", "/backups", "/data"]
EXPOSE 8080
CMD ["ass-subset-service"]
