# Email Sender Service

고성능 이메일 발송 마이크로서비스입니다. NATS JetStream에서 메시지를 소비하여 AWS SES를 통해 이메일을 발송합니다.

## 주요 특징

- **동시성 처리**: 설정 가능한 동시성 제한으로 대량 이메일 처리
- **속도 제한**: Governor를 사용한 초당 발송량 제어
- **멱등성 보장**: UUID 기반 캐시로 중복 메시지 방지
- **신뢰성**: 명시적 ACK/NAK으로 메시지 처리 보장
- **관찰성**: 실시간 RPS 모니터링 및 구조화된 로깅

## 아키텍처

```
NATS JetStream → Pull Consumer → Rate Limiter → AWS SES
                      ↓
                Idempotency Cache
```

- **메시지 브로커**: NATS JetStream (Pull Consumer)
- **직렬화**: MessagePack (rmp-serde)
- **이메일 서비스**: AWS SES v2
- **캐시**: Moka (TTL 기반)
- **런타임**: Tokio

## 메시지 형식

MessagePack으로 직렬화된 페이로드:

```rust
struct EmailPayload {
    uuid: String,        // 멱등성 키
    email: String,       // 수신자 이메일
    subject: String,     // 제목
    body: String,        // HTML 본문
}
```

## 환경 설정

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `NATS_URL` | `nats://127.0.0.1:4222` | NATS 서버 URL |
| `STREAM` | `messages` | JetStream 스트림 이름 |
| `SUBJECT` | `messages.email` | 구독할 주제 |
| `CONSUMER` | `email-processor` | Durable Consumer 이름 |
| `SES_RATE_PER_SEC` | `50` | 초당 발송 제한 |
| `FROM_EMAIL` | `no-reply@localhost` | 발신자 이메일 |
| `CONCURRENCY_LIMIT` | `500` | 동시 처리 제한 |

AWS 설정:
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION` (기본: `ap-northeast-2`)

## 시작하기

### 1. 환경 설정

```bash
# 환경 변수 파일 생성
cp .env.sample .env
# 필요한 값들을 설정하세요 (특히 AWS 자격증명과 FROM_EMAIL)
```

### 2. 빌드 및 실행

```bash
# 개발 모드
cargo run

# 릴리스 빌드
cargo build --release
RUST_LOG=info ./target/release/messages-email-sender
```

### 3. Docker 실행

```bash
# 빌드 스크립트 사용
./run.sh

# 또는 수동 실행
docker build -t messages-email-sender .
docker run --env-file .env messages-email-sender
```

## 동작 원리

1. **메시지 수신**: NATS JetStream에서 배치 단위로 메시지 가져오기
2. **동시 처리**: 설정된 동시성 제한 내에서 병렬 처리
3. **속도 제어**: Governor를 통한 초당 발송량 제한
4. **멱등성 검사**: UUID 기반 캐시로 중복 메시지 스킵
5. **이메일 발송**: AWS SES를 통한 개별 이메일 전송
6. **결과 처리**: 
   - 성공 시 ACK
   - 실패 시 5초 지연 후 NAK (재시도)
   - 파싱 실패 시 즉시 ACK (독성 메시지 제거)

## 모니터링

서비스는 다음과 같은 로그를 제공합니다:

- **시작 로그**: 설정 정보 출력
- **RPS 로그**: 초당 발송 통계 (`🚀 Sent per second (RPS): N`)
- **오류 로그**: 발송 실패 및 재시도 정보
- **디버그 로그**: 개별 메시지 처리 상태

로그 레벨 설정:
```bash
RUST_LOG=info                    # 기본 정보
RUST_LOG=debug                   # 상세 디버그
RUST_LOG=messages_email_sender=debug,aws=warn  # 선택적 로깅
```

## 성능 튜닝

| 설정 | 용도 | 권장값 |
|------|------|--------|
| `SES_RATE_PER_SEC` | AWS SES 할당량에 맞춘 속도 제한 | 50-200 |
| `CONCURRENCY_LIMIT` | 동시 처리 메시지 수 | 100-1000 |

## AWS SES 설정

1. **이메일 인증**: 발신 이메일 주소 또는 도메인 검증
2. **샌드박스 해제**: 프로덕션 사용을 위한 제한 해제
3. **IAM 권한**: `ses:SendEmail` 권한 필요
4. **리전 설정**: 기본 `ap-northeast-2` (서울)

## 의존성

주요 크레이트:
- `async-nats`: NATS JetStream 클라이언트
- `aws-sdk-sesv2`: AWS SES v2 SDK
- `governor`: 속도 제한
- `moka`: 고성능 캐시
- `rmp-serde`: MessagePack 직렬화
- `tokio`: 비동기 런타임
- `tracing`: 구조화된 로깅
