# Email Sender (Messages Module)

이 저장소는 메시징 시스템인 `messages`의 이메일 발송 전용 MSA입니다.  
NATS JetStream에서 이메일 요청 메시지를 Pull 소비하여 AWS SES로 발송합니다. 고가용성과 멱등성, 배치·재시도, 속도 제한을 고려한 경량 러스트 서비스입니다.

## 핵심 기능

- __NATS JetStream Pull Consumer__: `STREAM`/`SUBJECT`로 필터링된 메시지 소비
- __배치 처리 + 속도 제한__: 초당 `SES_RATE_PER_SEC`만큼 묶어서 발송
- __멱등성 보장__: `uuid` 기반 TTL 캐시로 중복 메시지 스킵
- __개별 성공/실패 처리__: 성공 시 ack, 실패 시 지연 NAK로 재시도
- __Poison 메시지 제거__: 파싱 실패 메시지는 즉시 ack하여 큐 정리
- __Graceful 종료__: Ctrl+C 시 통계 로그 후 종료
- __관찰성__: `tracing` 기반 로그 (`RUST_LOG`로 제어)

## 아키텍처 개요

- 메시지 브로커: NATS JetStream
- 소비 방식: Pull Consumer (`ack_policy=explicit`, `max_ack_pending` 설정)
- 메일 발송: AWS SES v2 `SendEmail` (병렬 실행)
- 언어/런타임: Rust + Tokio
- 멱등성 캐시: `moka::future::Cache` (TTL 기반)

## 메시지 형식

NATS 메시지 페이로드(JSON):
```json
{
  "uuid": "a1b2c3d4-...-z9",
  "email": "user@example.com",
  "subject": "메일 제목",
  "body": "<p>HTML 본문 또는 텍스트</p>"
}
```

- __uuid__: 요청 식별자(멱등성 키)
- __email__: 수신자 이메일
- __subject__: 제목
- __body__: HTML 또는 텍스트(현재 코드상 HTML로 전송)

## 환경변수

[.env.sample](cci:7://file:///Users/jake/dev/projects/messages/services/sender-email/.env.sample:0:0-0:0) 참고:

- __NATS_URL__ (기본: `nats://127.0.0.1:4222`)
- __STREAM__ (기본: `messages`)
- __SUBJECT__ (기본: `messages.email`)
- __CONSUMER__ (기본: `email-processor`)
- __SES_RATE_PER_SEC__ 초당 발송량 제한 (기본: `50`, 샘플: `25`)
- __FROM_EMAIL__ 발신 주소 (기본: `no-reply@localhost`)
- __FETCH_TIMEOUT_MS__ Pull fetch 만료(ms) (기본: `100`)
- __MAX_ACK_PENDING__ JetStream ack 대기 상한 (기본: `1000`)
- __NAK_DELAY_SECS__ 실패 시 재시도 지연(초) (기본: `10`)
- __IDEMPOTENCY_WINDOW_SECS__ 멱등성 TTL(초) (기본: `60`)
- __AWS_ACCESS_KEY_ID__, __AWS_SECRET_ACCESS_KEY__, __AWS_REGION__
    - 리전 기본값: `ap-northeast-2` (서울). 환경변수가 있으면 이를 우선합니다.
- 선택: __RUST_LOG__ 예: `info` 또는 `messages_email_sender=debug,aws_smithy_http=warn`

## 로컬 실행

1) 의존성 설치
- Rust 1.89+ (edition 2024)
- NATS Server (JetStream 활성화)
- AWS 자격증명 구성(Profile 또는 env)

2) .env 생성
```
cp .env.sample .env
# 값 채우기 (특히 AWS 자격증명/리전, FROM_EMAIL)
```

3) NATS JetStream 준비(예시)
- 스트림 생성:
    - 이름: `messages`
    - 주제: `messages.email`
- 컨슈머 생성:
    - Durable: `email-processor`
    - 필터 주제: `messages.email`
    - Ack 정책: explicit

참고: 조직 표준 툴/인프라로 생성해도 됩니다. CLI 예시는 환경에 따라 다를 수 있어 여기서는 개념만 안내합니다.

4) 빌드 & 실행
```
cargo build --release
RUST_LOG=info ./target/release/messages-email-sender
```

## Docker

- 빌드/실행 스크립트: [run.sh](cci:7://file:///Users/jake/dev/projects/messages/services/sender-email/run.sh:0:0-0:0)
- 수동 실행:
```
docker build -t messages-email-sender:latest .
docker run --env-file .env --name=messages-email-sender -d messages-email-sender:latest
```
- Dockerfile
    - 멀티스테이지 빌드: `rust:1.89-slim` → `distroless/cc-debian12`
    - 실행 사용자: UID 1000
    - 엔트리포인트: `/app/messages-email-sender`

## 동작 방식 상세

- 메인 루프는 정확히 1초 간격으로 동작하며 각 틱에서:
    - `ses_rate_per_sec`만큼 Pull fetch
    - JSON 파싱 실패 → 즉시 ack (poison 제거)
    - 멱등성 캐시 히트 → 즉시 ack (중복 스킵)
    - 나머지를 AWS SES로 병렬 발송
        - 성공 → ack
        - 실패 → `NAK_DELAY_SECS` 지연 NAK로 재시도
- 장시간 빈 큐면 60틱(분 단위)마다 디버그 로그
- 종료 시 통계 출력: 총 발송/poison/empty_ticks

관련 코드:
- [src/main.rs](cci:7://file:///Users/jake/dev/projects/messages/services/sender-email/src/main.rs:0:0-0:0)의 [process_batch()](cci:1://file:///Users/jake/dev/projects/messages/services/sender-email/src/main.rs:138:0-238:1)와 [send_ses_bulk()](cci:1://file:///Users/jake/dev/projects/messages/services/sender-email/src/main.rs:75:0-136:1) 참고
- 환경 로드: [AppConfig::from_env()](cci:1://file:///Users/jake/dev/projects/messages/services/sender-email/src/main.rs:44:4-72:5)

## 튜닝 포인트

- __SES_RATE_PER_SEC__: 초당 발송량 제한. SES 할당량/스로틀에 맞춰 조정.
- __FETCH_TIMEOUT_MS__: Pull 대기 시간. 지연과 공회전을 균형 있게.
- __MAX_ACK_PENDING__: 컨슈머 ack 대기 상한. 처리량/메모리에 맞춰 조정.
- __NAK_DELAY_SECS__: 실패 재시도 지연. 일시적 오류 완화에 유용.
- __IDEMPOTENCY_WINDOW_SECS__: 중복 스킵 TTL. 재전송 패턴에 맞춰 조정.

## AWS SES 요건

- 발신 도메인/이메일 인증(검증) 필요
- 프로덕션 전환/할당량 확보
- IAM 권한: SESv2 `SendEmail` 등
- 리전: 기본 `ap-northeast-2`, 환경변수로 오버라이드 가능

## 로깅/관찰성

- `tracing` 사용. `RUST_LOG`로 레벨 제어
- 주요 로그:
    - 시작/설정 요약
    - 배치 결과(성공/poison/중복)
    - SES 실패 및 NAK 처리
    - 종료 통계

## 제한 사항

- 템플릿 메일/첨부파일 미지원(단순 본문 발송)
- 대량 발송 최적화(예: SES Bulk API 템플릿)는 향후 개선 여지
- 헬스체크/프로메테우스 메트릭 미포함

## 개발

- 의존성: [Cargo.toml](cci:7://file:///Users/jake/dev/projects/messages/services/sender-email/Cargo.toml:0:0-0:0) 참고
    - async-nats, aws-sdk-sesv2, tokio, tracing, serde, serde_json, anyhow, moka 등
- 코드 엔트리: [src/main.rs](cci:7://file:///Users/jake/dev/projects/messages/services/sender-email/src/main.rs:0:0-0:0)

## 라이선스

조직 정책을 따릅니다. 별도 라이선스 파일이 있는 경우 이를 우선합니다.
