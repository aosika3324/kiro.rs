FROM oven/bun:1-alpine AS frontend-builder

WORKDIR /app/admin-ui
COPY admin-ui/package.json admin-ui/bun.lock* ./
RUN bun install --frozen-lockfile --ignore-scripts
COPY admin-ui ./
RUN bun run build

# glibc 构建镜像(非 alpine/musl)：TLS 指纹依赖 boring-sys2(BoringSSL) 的 bindgen 需
# dlopen libclang，而 rust:alpine 的 build-script 是静态 musl 二进制、不支持动态加载
# (报 "Dynamic loading not supported")。glibc(bookworm) 无此限制。
# 需要：cmake(编 BoringSSL) + clang/libclang-dev(bindgen) + git(boring-sys2 打补丁) + perl。
FROM rust:1.92-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake clang libclang-dev git perl make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
COPY --from=frontend-builder /app/admin-ui/dist /app/admin-ui/dist

# 部署构建：关闭默认(避免 native-tls) 但显式启用 tls-fingerprint —— 走 rustls + BoringSSL 指纹，
# 二者不冲突(无 openssl-sys)。若不需指纹，用纯 `--no-default-features` 即可(不引入 BoringSSL)。
RUN cargo build --release --no-default-features --features tls-fingerprint

# glibc 运行时(与 bookworm builder 的 glibc 匹配)。debian-slim ~80MB。
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/kiro-rs /app/kiro-rs

VOLUME ["/app/config"]

EXPOSE 8990

CMD ["./kiro-rs", "-c", "/app/config/config.json", "--credentials", "/app/config/credentials.json"]
