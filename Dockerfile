# =========================================================================
# 1. Builder Stage: 애플리케이션 컴파일 단계
# =========================================================================
FROM rust:1.89.0-bookworm AS builder

# C 라이브러리에 의존하는 크레이트(예: openssl) 컴파일에 필요한 패키지 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- 의존성 캐싱 섹션 ---
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && \
    echo "fn main() {println!(\"if you see this, the build cache is not working.\");}" > src/main.rs && \
    cargo build --release

# --- 애플리케이션 빌드 섹션 ---
COPY src ./src
RUN touch src/main.rs && cargo build --release


# =========================================================================
# 2. Runtime Stage: 최종 실행 단계
# =========================================================================
FROM gcr.io/distroless/cc-debian12

ARG APP_NAME=messages-email-sender

# 메타데이터 추가
LABEL maintainer="lee@lou2.kr" \
      description="Messages Email Sender"

# 보안을 위해 non-root 사용자로 실행 (UID 1000)
USER 1000
WORKDIR /app

# 빌더 파일 복사
COPY --from=builder --chown=1000:1000 /app/target/release/${APP_NAME} ./app

# 실행
ENTRYPOINT ["./app"]