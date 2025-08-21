use std::env;
use std::time::Duration;

use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::{AckPolicy, Consumer};
use async_nats::jetstream::message::AckKind;
use async_nats::jetstream::{self, stream::Config as StreamConfig};
use futures::StreamExt;
use serde::Deserialize;
use tracing::{debug, error, info, warn};

use aws_config::meta::region::RegionProviderChain;
// AWS SDK
use aws_config::BehaviorVersion;
use aws_sdk_sesv2::types::{
    Body, Content, Destination, EmailContent, Message as SesMessage, MessageTag,
};
use aws_sdk_sesv2::{Client as SesClient, Error as SesError};

use moka::future::Cache;

#[derive(Debug, Deserialize, Clone)]
struct EmailPayload {
    uuid: String,
    email: String,
    subject: String,
    body: String,
}

/// 환경 설정을 관리하는 구조체
struct AppConfig {
    nats_url: String,
    stream_name: String,
    subject_filter: String,
    durable_name: String,
    ses_rate_per_sec: usize,
    from_email: String,
    fetch_timeout_ms: u64,
    max_ack_pending: i64,
    nak_delay_secs: u64,
    idempotency_window_secs: u64,
}

impl AppConfig {
    fn from_env() -> Self {
        Self {
            nats_url: env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string()),
            stream_name: env::var("STREAM").unwrap_or_else(|_| "messages".to_string()),
            subject_filter: env::var("SUBJECT").unwrap_or_else(|_| "messages.email".to_string()),
            durable_name: env::var("CONSUMER").unwrap_or_else(|_| "email-processor".to_string()),
            ses_rate_per_sec: env::var("SES_RATE_PER_SEC")
                .unwrap_or_else(|_| "50".to_string())
                .parse()
                .unwrap_or(50),
            from_email: env::var("FROM_EMAIL").unwrap_or_else(|_| "no-reply@localhost".to_string()),
            fetch_timeout_ms: env::var("FETCH_TIMEOUT_MS")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            max_ack_pending: env::var("MAX_ACK_PENDING")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
            nak_delay_secs: env::var("NAK_DELAY_SECS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
            idempotency_window_secs: env::var("IDEMPOTENCY_WINDOW_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .unwrap_or(60),
        }
    }
}

/// AWS SES 발송 (벌크 API 사용)
async fn send_ses_bulk(
    client: &SesClient,
    messages: &[EmailPayload],
    from_email: &str,
) -> Result<Vec<bool>, SesError> {
    debug!("Sending {} emails via SES", messages.len());

    // 병렬 발송 (실제로는 SendBulkTemplatedEmail이 더 효율적)
    let send_futures = messages.iter().map(|msg| {
        let destination = Destination::builder().to_addresses(&msg.email).build();
        let subject_content = Content::builder()
            .data(&msg.subject)
            .charset("UTF-8")
            .build()
            .unwrap();
        let body_content = Content::builder()
            .data(&msg.body)
            .charset("UTF-8")
            .build()
            .unwrap();
        let body = Body::builder().html(body_content).build();
        let ses_message = SesMessage::builder()
            .subject(subject_content)
            .body(body)
            .build();
        let email_content = EmailContent::builder().simple(ses_message).build();

        client
            .send_email()
            .from_email_address(from_email)
            .destination(destination)
            .email_tags(
                MessageTag::builder()
                    .name("request_id")
                    .value(msg.uuid.clone())
                    .build()
                    .unwrap(),
            )
            .content(email_content)
            .send()
    });

    let results = futures::future::join_all(send_futures).await;

    // 개별 결과 추적 (성공/실패)
    let mut success_flags = Vec::with_capacity(results.len());
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(_) => {
                success_flags.push(true);
                debug!("Email sent successfully to: {}", messages[i].email);
            }
            Err(e) => {
                success_flags.push(false);
                warn!("Failed to send email to {}: {}", messages[i].email, e);
            }
        }
    }

    Ok(success_flags)
}

/// 배치 처리 핵심 로직
async fn process_batch(
    consumer: &Consumer<PullConfig>,
    ses_client: &SesClient,
    config: &AppConfig,
    processed_uuids: &Cache<String, ()>,
) -> anyhow::Result<(usize, usize)> {
    let mut messages_to_process = Vec::new();
    let mut payloads = Vec::new();
    let mut poison_count = 0;
    let mut duplicate_count = 0;

    // 메시지 가져오기
    let mut batch = consumer
        .fetch()
        .max_messages(config.ses_rate_per_sec)
        .expires(Duration::from_millis(config.fetch_timeout_ms))
        .messages()
        .await?;

    while let Some(Ok(msg)) = batch.next().await {
        match serde_json::from_slice::<EmailPayload>(&msg.payload) {
            Ok(payload) => {
                // 멱등성 검사
                if processed_uuids.contains_key(&payload.uuid) {
                    duplicate_count += 1;
                    warn!(
                        "Duplicate message detected within TTL, skipping. UUID: {}",
                        payload.uuid
                    );
                    // 중복 메시지는 즉시 ack 처리하여 큐에서 제거
                    if let Err(e) = msg.ack().await {
                        error!("Failed to ack duplicate message: {}", e);
                    }
                    continue;
                }

                // 멱등성 검증을 위한 데이터 추가
                processed_uuids.insert(payload.uuid.clone(), ()).await;

                payloads.push(payload);
                messages_to_process.push(msg);
            }
            Err(e) => {
                poison_count += 1;
                debug!("Invalid message format, acking to remove: {}", e);
                let _ = msg.ack().await;
            }
        }
    }

    let processed_count = payloads.len();

    // 발송할 메시지가 있는 경우
    if !payloads.is_empty() {
        match send_ses_bulk(ses_client, &payloads, &config.from_email).await {
            Ok(success_flags) => {
                // 개별 메시지별로 ack/nak 처리
                for (msg, success) in messages_to_process.into_iter().zip(success_flags) {
                    if success {
                        if let Err(e) = msg.ack().await {
                            error!("Failed to ack message: {}", e);
                        }
                    } else {
                        // 실패한 메시지는 지연 NAK 처리하여 재시도
                        if let Err(e) = msg
                            .ack_with(AckKind::Nak(Some(Duration::from_secs(
                                config.nak_delay_secs,
                            ))))
                            .await
                        {
                            error!("Failed to nak message: {}", e);
                        }
                    }
                }
                info!(
                    "Batch processed: {} sent, {} poison, {} duplicates skipped",
                    processed_count, poison_count, duplicate_count
                );
            }
            Err(e) => {
                error!("SES bulk send failed: {}", e);
                // 전체 실패 시 모든 메시지 nak
                for msg in messages_to_process {
                    let _ = msg
                        .ack_with(AckKind::Nak(Some(Duration::from_secs(
                            config.nak_delay_secs,
                        ))))
                        .await;
                }
            }
        }
    } else if poison_count > 0 || duplicate_count > 0 {
        debug!(
            "Batch finished: {} poison removed, {} duplicates skipped",
            poison_count, duplicate_count
        );
    }

    Ok((processed_count, poison_count))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 로깅 초기화 (환경변수 RUST_LOG로 제어)
    tracing_subscriber::fmt::init();

    // 설정 로드
    let config = AppConfig::from_env();
    info!(
        "Starting email processor (rate: {}/sec, fetch_timeout: {}ms)",
        config.ses_rate_per_sec, config.fetch_timeout_ms
    );

    // AWS SES 클라이언트 초기화
    let region_provider = RegionProviderChain::default_provider().or_else("ap-northeast-2");
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let ses_client = SesClient::new(&aws_config);

    // NATS 연결
    let client = async_nats::connect(&config.nats_url).await?;
    let js = jetstream::new(client);

    let stream = js
        .get_or_create_stream(StreamConfig {
            name: config.stream_name.clone(),
            subjects: vec![format!("{}.*", config.stream_name)],
            ..Default::default()
        })
        .await
        .map_err(|e| {
            error!(
                "Failed to get or create stream '{}': {}",
                config.stream_name, e
            );
            e
        })?;

    let consumer_config = PullConfig {
        durable_name: Some(config.durable_name.clone()),
        filter_subject: config.subject_filter.clone(),
        ack_policy: AckPolicy::Explicit,
        ack_wait: Duration::from_secs(30),
        max_ack_pending: config.max_ack_pending,
        ..Default::default()
    };

    let consumer = stream
        .get_or_create_consumer(&config.durable_name, consumer_config)
        .await?;

    let processed_uuids: Cache<String, ()> = Cache::builder()
        .time_to_live(Duration::from_secs(config.idempotency_window_secs))
        .build();

    info!("Consumer ready, starting processing loop");

    // 메인 루프: 정확한 1초 간격
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // 통계
    let mut total_sent = 0;
    let mut total_poison = 0;
    let mut empty_ticks = 0;

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                info!(
                    "Shutting down. Stats: sent={}, poison={}, empty_ticks={}",
                    total_sent, total_poison, empty_ticks
                );
                break;
            }
            _ = interval.tick() => {
                match process_batch(&consumer, &ses_client, &config, &processed_uuids).await {
                    Ok((0, 0)) => {
                        empty_ticks += 1;
                        // 장시간 빈 큐일 때만 로그 (매 60초)
                        if empty_ticks % 60 == 0 {
                            debug!("Queue empty for {} ticks", empty_ticks);
                        }
                    }
                    Ok((sent, poison)) => {
                        total_sent += sent;
                        total_poison += poison;
                        empty_ticks = 0;
                        debug!("Tick complete: sent={}, poison={}", sent, poison);
                    }
                    Err(e) => {
                        error!("Batch processing error: {}", e);
                        // 에러 시 다음 틱까지 대기 (interval이 처리)
                    }
                }
            }
        }
    }

    Ok(())
}
