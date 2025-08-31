use std::env;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::consumer::{AckPolicy, Consumer, pull::Config as PullConfig};
use async_nats::jetstream::message::{AckKind, Message};
use async_nats::jetstream::stream::Config as StreamConfig;
use futures::StreamExt;
use governor::{Quota, RateLimiter};
use moka::future::Cache;
use serde::Deserialize;
use tracing::{debug, error, info};

use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_sesv2::Client as SesClient;
use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message as SesMessage};

// 이메일 발송에 필요한 정보를 담는 구조체
#[derive(Debug, Deserialize, Clone)]
struct EmailPayload {
    uuid: String,
    email: String,
    subject: String,
    body: String,
}

// 애플리케이션 설정을 관리하는 구조체
#[derive(Clone, Debug)]
struct AppConfig {
    nats_url: String,
    stream_name: String,
    subject_filter: String,
    durable_name: String,
    ses_rate_per_sec: u32,
    from_email: String,
    concurrency_limit: usize,
}

impl AppConfig {
    // 환경 변수에서 설정을 로드
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
            concurrency_limit: env::var("CONCURRENCY_LIMIT")
                .unwrap_or_else(|_| "500".to_string())
                .parse()
                .unwrap_or(500),
        }
    }
}

// 개별 이메일을 AWS SES를 통해 발송하는 로직 (기존과 거의 동일)
async fn send_single_email(
    ses_client: Arc<SesClient>,
    nats_msg: Message,
    payload: EmailPayload,
    config: Arc<AppConfig>,
    sent_count: Arc<AtomicUsize>,
) {
    // SES 메시지 구성
    let destination = Destination::builder().to_addresses(&payload.email).build();
    let subject = Content::builder()
        .data(&payload.subject)
        .charset("UTF-8")
        .build()
        .unwrap();
    let body_html = Content::builder()
        .data(&payload.body)
        .charset("UTF-8")
        .build()
        .unwrap();
    let body = Body::builder().html(body_html).build();
    let ses_message = SesMessage::builder().subject(subject).body(body).build();
    let email_content = EmailContent::builder().simple(ses_message).build();

    // 이메일 발송
    match ses_client
        .send_email()
        .from_email_address(&config.from_email)
        .destination(destination)
        .content(email_content)
        .send()
        .await
    {
        Ok(_) => {
            sent_count.fetch_add(1, Ordering::Relaxed);
            debug!("Email sent to {}", payload.email);
            if let Err(e) = nats_msg.ack().await {
                error!("메시지 ACK 실패 ({}): {}", payload.email, e);
            }
        }
        Err(e) => {
            error!("이메일 발송 실패 ({}): {}", payload.email, e);
            if let Err(e) = nats_msg
                .ack_with(AckKind::Nak(Some(Duration::from_secs(5))))
                .await
            {
                error!("메시지 NAK 실패 ({}): {}", payload.email, e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config = Arc::new(AppConfig::from_env());
    info!("이메일 프로세서 시작. 설정: {:?}", config);

    // AWS SDK 클라이언트 설정
    let region_provider = RegionProviderChain::default_provider().or_else("ap-northeast-2");
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let ses_client = Arc::new(SesClient::new(&aws_config));

    // NATS JetStream 설정
    let client = async_nats::connect(&config.nats_url).await?;
    let stream = jetstream::new(client)
        .get_or_create_stream(StreamConfig {
            name: config.stream_name.clone(),
            subjects: vec![format!("{}.*", config.stream_name)],
            ..Default::default()
        })
        .await?;
    let consumer: Consumer<PullConfig> = stream
        .get_or_create_consumer(
            &config.durable_name,
            PullConfig {
                durable_name: Some(config.durable_name.clone()),
                filter_subject: config.subject_filter.clone(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_ack_pending: (config.concurrency_limit * 2) as i64,
                ..Default::default()
            },
        )
        .await?;

    // 멱등성 캐시 설정
    let idempotent_cache = Arc::new(
        Cache::builder()
            .time_to_live(Duration::from_secs(60))
            .build(),
    );

    // 초당 발송 카운터 및 로깅 태스크
    let sent_count = Arc::new(AtomicUsize::new(0));
    let sent_count_logger = Arc::clone(&sent_count);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let count = sent_count_logger.swap(0, Ordering::Relaxed);
            if count > 0 {
                info!("🚀 Sent per second (RPS): {}", count);
            }
        }
    });

    // Rate Limiter 설정
    let lim = RateLimiter::direct(
        Quota::per_second(NonZeroU32::new(config.ses_rate_per_sec).unwrap())
            .allow_burst(NonZeroU32::new(1).unwrap())
    );
    let rate_limiter = Arc::new(lim);

    info!(
        "NATS JetStream에서 메시지 컨슈밍 시작... (동시성: {}, 비율: {} RPS)",
        config.concurrency_limit, config.ses_rate_per_sec
    );

    loop {
        // 메시지 컨슈밍
        let messages = consumer
            .fetch()
            .max_messages(config.concurrency_limit)
            .messages()
            .await?;

        messages
            .for_each_concurrent(config.concurrency_limit, |message_result| {
                let limiter = Arc::clone(&rate_limiter);
                let client = Arc::clone(&ses_client);
                let conf = Arc::clone(&config);
                let counter = Arc::clone(&sent_count);
                let cache = Arc::clone(&idempotent_cache);

                async move {
                    let nats_msg = match message_result {
                        Ok(msg) => msg,
                        Err(e) => {
                            error!("NATS 메시지 수신 실패: {}", e);
                            return;
                        }
                    };

                    // 메시지 파싱
                    let payload: EmailPayload = match rmp_serde::from_slice(&nats_msg.payload) {
                        Ok(p) => p,
                        Err(e) => {
                            debug!(
                                "잘못된 형식의 메시지(Poison Pill): {}. 메시지를 ACK 처리합니다.",
                                e
                            );
                            let _ = nats_msg.ack().await;
                            return;
                        }
                    };

                    // 멱등성 체크
                    if cache.get(&payload.uuid).await.is_some() {
                        debug!(
                            "중복된 메시지 수신 (UUID: {}), ACK 처리합니다.",
                            payload.uuid
                        );
                        let _ = nats_msg.ack().await;
                        return;
                    }
                    cache.insert(payload.uuid.clone(), ()).await;

                    // 초당 발송 수 고려하여 대기
                    limiter.until_ready().await;

                    // 메시지 발송
                    tokio::spawn(send_single_email(client, nats_msg, payload, conf, counter));
                }
            })
            .await;
    }
}
