FROM oven/bun:1-alpine AS frontend-builder

WORKDIR /app/admin-ui
COPY admin-ui/package.json admin-ui/bun.lock* ./
RUN bun install --frozen-lockfile --ignore-scripts
COPY admin-ui ./
RUN bun run build

FROM rust:1.92-alpine AS builder

# musl-dev/perl/make：既有；cmake + clang/llvm + g++：TLS 指纹依赖 boring-sys2(BoringSSL)
# 需 cmake 编译 + libclang 跑 bindgen。alpine 的 clang 包自带 builtin headers(stddef.h 等)，
# 无需额外 -I（区别于 pip libclang）。若 musl 下 BoringSSL 构建失败，回退方案见 README/memory：
# 把本 stage 换成 rust:1.92-bookworm(glibc) + 对应 glibc runtime base。
RUN apk add --no-cache musl-dev perl make cmake clang clang-dev llvm-dev g++ linux-headers
ENV LIBCLANG_PATH=/usr/lib

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY --from=frontend-builder /app/admin-ui/dist /app/admin-ui/dist

# 部署构建：关闭默认(避免 native-tls) 但显式启用 tls-fingerprint —— 走 rustls + BoringSSL 指纹，
# 二者不冲突(无 openssl-sys)。若不需指纹，用纯 `--no-default-features` 即可(不引入 BoringSSL)。
RUN cargo build --release --no-default-features --features tls-fingerprint

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/target/release/kiro-rs /app/kiro-rs

VOLUME ["/app/config"]

EXPOSE 8990

CMD ["./kiro-rs", "-c", "/app/config/config.json", "--credentials", "/app/config/credentials.json"]
