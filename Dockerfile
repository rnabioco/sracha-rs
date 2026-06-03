# --- Stage 1: Build the Rust binary ---
FROM rust:1.92-slim AS builder

RUN apt-get update && apt-get install -y \
    git \
    pkg-config \
    libssl-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install --git https://github.com/rnabioco/sracha-rs sracha

# --- Stage 2: Create the lightweight runtime image ---
# Switched from debian:bookworm to ubuntu:24.04 for GLIBC 2.38+ support
FROM ubuntu:24.04

# Avoid interactive prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies (Ubuntu 24.04 uses libssl3)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/local/cargo/bin/sracha /usr/local/bin/sracha

# Set the default command to verify installation
CMD ["sracha", "--help"]
