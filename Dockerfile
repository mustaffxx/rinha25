FROM rust:1.88-alpine AS builder

RUN apk add --no-cache \
    musl-dev \
    openssl-dev \
    openssl-libs-static \
    pkgconfig

WORKDIR /app
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

FROM alpine
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rinha25 /rinha25
CMD ["/rinha25"]
