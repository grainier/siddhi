# Multi-stage build for Siddhi Rust
# Following industry standards from Flink, Kafka Streams, and WSO2 Siddhi

# Stage 1: Builder
FROM rust:1.89.0 as builder

# Install dependencies for building
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    cmake \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /usr/src/siddhi

# Copy Cargo files first for better caching
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./

# Copy proto files
COPY proto ./proto

# Copy source code and tests (needed for dev dependencies)
COPY src ./src
COPY tests ./tests

# Build the application in release mode
RUN cargo build --release --bin run_siddhi

# Stage 2: Runtime base image
FROM debian:bookworm-slim as runtime-base

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -ms /bin/bash siddhi

# Stage 3: Siddhi Runner image (following WSO2/Flink pattern)
FROM runtime-base as siddhi-runner

# Set working directory
WORKDIR /opt/siddhi

# Copy the binary from builder
COPY --from=builder /usr/src/siddhi/target/release/run_siddhi /opt/siddhi/bin/siddhi-runner

# Create directories for Siddhi apps and configuration
RUN mkdir -p \
    /opt/siddhi/apps \
    /opt/siddhi/deployment \
    /opt/siddhi/config \
    /opt/siddhi/extensions \
    /opt/siddhi/state \
    /opt/siddhi/logs \
    && chown -R siddhi:siddhi /opt/siddhi

# Copy default configuration
COPY --chown=siddhi:siddhi config /opt/siddhi/config

# Environment variables (following Flink/Kafka pattern)
ENV SIDDHI_HOME=/opt/siddhi
ENV SIDDHI_APPS_DIR=/opt/siddhi/apps
ENV SIDDHI_DEPLOYMENT_DIR=/opt/siddhi/deployment
ENV SIDDHI_CONFIG_DIR=/opt/siddhi/config
ENV SIDDHI_EXTENSIONS_DIR=/opt/siddhi/extensions
ENV SIDDHI_STATE_DIR=/opt/siddhi/state
ENV SIDDHI_LOG_DIR=/opt/siddhi/logs

# Configuration via environment variables (like Flink's FLINK_PROPERTIES)
ENV SIDDHI_PROPERTIES=""

# Node configuration for distributed mode
ENV NODE_ID=""
ENV SEED_NODES=""
ENV CLUSTER_ENABLED="false"

# Ports
# 8006 - HTTP API for app deployment
# 8080 - Health check endpoint
# 7000 - Cluster communication (gRPC/TCP)
# 9090 - Prometheus metrics
EXPOSE 8006 8080 7000 9090

# Volume mounts for persistence and apps
VOLUME ["/opt/siddhi/apps", "/opt/siddhi/deployment", "/opt/siddhi/state", "/opt/siddhi/logs"]

# Switch to non-root user
USER siddhi

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Entry point script to handle different modes
COPY --chown=siddhi:siddhi docker-entrypoint.sh /opt/siddhi/
RUN chmod +x /opt/siddhi/docker-entrypoint.sh

ENTRYPOINT ["/opt/siddhi/docker-entrypoint.sh"]

# Default command runs the siddhi-runner
CMD ["run"]

# Stage 4: Development image with additional tools
FROM siddhi-runner as siddhi-dev

USER root

# Install development tools
RUN apt-get update && apt-get install -y \
    vim \
    net-tools \
    iputils-ping \
    telnet \
    htop \
    && rm -rf /var/lib/apt/lists/*

USER siddhi

# Stage 5: Alpine variant for smaller size
FROM alpine:3.18 as siddhi-runner-alpine

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    libgcc \
    libssl3 \
    curl \
    jq \
    bash

# Create non-root user
RUN adduser -D -s /bin/bash siddhi

WORKDIR /opt/siddhi

# Copy binary from builder
COPY --from=builder /usr/src/siddhi/target/release/run_siddhi /opt/siddhi/bin/siddhi-runner

# Create directories
RUN mkdir -p \
    /opt/siddhi/apps \
    /opt/siddhi/deployment \
    /opt/siddhi/config \
    /opt/siddhi/extensions \
    /opt/siddhi/state \
    /opt/siddhi/logs \
    && chown -R siddhi:siddhi /opt/siddhi

# Copy configuration
COPY --chown=siddhi:siddhi config /opt/siddhi/config

# Same environment variables and ports
ENV SIDDHI_HOME=/opt/siddhi
ENV SIDDHI_APPS_DIR=/opt/siddhi/apps
ENV SIDDHI_DEPLOYMENT_DIR=/opt/siddhi/deployment
ENV SIDDHI_CONFIG_DIR=/opt/siddhi/config
ENV SIDDHI_EXTENSIONS_DIR=/opt/siddhi/extensions
ENV SIDDHI_STATE_DIR=/opt/siddhi/state
ENV SIDDHI_LOG_DIR=/opt/siddhi/logs
ENV SIDDHI_PROPERTIES=""
ENV NODE_ID=""
ENV SEED_NODES=""
ENV CLUSTER_ENABLED="false"

EXPOSE 8006 8080 7000 9090

VOLUME ["/opt/siddhi/apps", "/opt/siddhi/deployment", "/opt/siddhi/state", "/opt/siddhi/logs"]

USER siddhi

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

COPY --chown=siddhi:siddhi docker-entrypoint.sh /opt/siddhi/
RUN chmod +x /opt/siddhi/docker-entrypoint.sh

ENTRYPOINT ["/opt/siddhi/docker-entrypoint.sh"]
CMD ["run"]