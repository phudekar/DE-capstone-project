FROM rust:1.85-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y git pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/dileepbapat/mock-stock.git .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/de-stock /usr/local/bin/
ENTRYPOINT ["de-stock"]
CMD ["/etc/de-stock/default.toml"]
