use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::{AckPolicy, Consumer};
use async_nats::jetstream::message::{AckKind, Message};
use async_nats::jetstream::{self, stream::Config as StreamConfig};
use futures::StreamExt;
use moka::future::Cache;
use serde::Deserialize;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
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
    ses_rate_per_sec: usize,
    from_email: String,
    channel_capacity: usize,
    max_ack_pending: i64,
    nak_delay_secs: u64,
    idempotency_window_secs: u64,
    fetch_batch_size: usize,
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
            channel_capacity: env::var("CHANNEL_CAPACITY")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .unwrap_or(5000),
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
            fetch_batch_size: env::var("FETCH_BATCH_SIZE")
                .unwrap_or_else(|_| "200".to_string())
                .parse()
                .unwrap_or(200),
        }
    }
}

// 토큰 버킷 알고리즘을 사용한 속도 제한기
#[derive(Clone)]
struct RateLimiter {
    semaphore: Arc<Semaphore>,
}

impl RateLimiter {
    fn new(rate_per_sec: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(rate_per_sec)),
        }
    }

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.semaphore.clone().acquire_owned().await
    }

    // 1초마다 토큰을 리필하는 백그라운드 태스크 실행
    fn spawn_refill_task(&self, rate_per_sec: usize) {
        let semaphore = Arc::clone(&self.semaphore);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let current = semaphore.available_permits();
                if current < rate_per_sec {
                    semaphore.add_permits(rate_per_sec - current);
                }
            }
        });
    }
}

// NATS 메시지와 파싱된 페이로드를 함께 관리하는 구조체
struct ProcessableMessage {
    payload: EmailPayload,
    nats_msg: Message,
}

// NATS에서 메시지를 가져와 채널로 전송 (생산자)
async fn fetch_and_dispatch(
    consumer: Consumer<PullConfig>,
    tx: Sender<ProcessableMessage>,
    idempotent_cache: Arc<Cache<String, ()>>,
    config: Arc<AppConfig>,
) -> anyhow::Result<()> {
    info!(
        "Fetcher task started. Fetch batch size: {}",
        config.fetch_batch_size
    );
    loop {
        let batch_result = consumer
            .fetch()
            .max_messages(config.fetch_batch_size)
            .expires(Duration::from_secs(5))
            .messages()
            .await;
        match batch_result {
            Ok(mut batch) => {
                while let Some(Ok(msg)) = batch.next().await {
                    match serde_json::from_slice::<EmailPayload>(&msg.payload) {
                        Ok(payload) => {
                            // 멱등성 보장을 위해 이미 처리된 메시지인지 확인
                            if idempotent_cache.get(&payload.uuid).await.is_some() {
                                let _ = msg.ack().await;
                                continue;
                            }
                            idempotent_cache.insert(payload.uuid.clone(), ()).await;
                            let processable = ProcessableMessage {
                                payload,
                                nats_msg: msg,
                            };
                            if tx.send(processable).await.is_err() {
                                error!("Channel closed, shutting down fetcher");
                                return Ok(());
                            }
                        }
                        Err(e) => {
                            // 잘못된 형식의 메시지(Poison Pill)는 ACK 처리하여 제거
                            debug!("Invalid message format (poison pill): {}", e);
                            let _ = msg.ack().await;
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to fetch messages: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// 채널에서 메시지를 받아 이메일 발송 (소비자)
async fn email_sender(
    mut rx: Receiver<ProcessableMessage>,
    ses_client: Arc<SesClient>,
    rate_limiter: RateLimiter,
    config: Arc<AppConfig>,
) -> anyhow::Result<()> {
    info!("Sender task started. Rate: {}/sec", config.ses_rate_per_sec);

    while let Some(msg) = rx.recv().await {
        // 속도 제한에 따라 허용될 때까지 대기
        let permit = match rate_limiter.acquire_permit().await {
            Ok(p) => p,
            Err(_) => {
                error!("Rate limiter semaphore closed. Shutting down sender.");
                break;
            }
        };

        // 각 이메일 발송을 별도의 태스크로 분리하여 병렬 처리
        tokio::spawn(send_single_email(
            Arc::clone(&ses_client),
            msg,
            Arc::clone(&config),
            permit,
        ));
    }

    info!("Channel closed. Shutting down sender task.");
    Ok(())
}

// 개별 이메일을 AWS SES를 통해 발송
async fn send_single_email(
    ses_client: Arc<SesClient>,
    msg: ProcessableMessage,
    config: Arc<AppConfig>,
    _permit: OwnedSemaphorePermit, // permit의 소유권을 가져와 작업이 끝날 때까지 토큰을 유지
) {
    let start = Instant::now();
    let payload = msg.payload;
    let nats_msg = msg.nats_msg;

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

    match ses_client
        .send_email()
        .from_email_address(&config.from_email)
        .destination(destination)
        .content(email_content)
        .send()
        .await
    {
        Ok(_) => {
            debug!("Email sent to {} in {:?}", payload.email, start.elapsed());
            // 성공 시 NATS 메시지 ACK
            if let Err(e) = nats_msg.ack().await {
                error!("Failed to ACK message for {}: {}", payload.email, e);
            }
        }
        Err(e) => {
            error!("Failed to send email to {}: {}", payload.email, e);
            // 실패 시 NATS 메시지 NAK (재시도 요청)
            if let Err(e) = nats_msg
                .ack_with(AckKind::Nak(Some(Duration::from_secs(
                    config.nak_delay_secs,
                ))))
                .await
            {
                error!("Failed to NAK message for {}: {}", payload.email, e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config = Arc::new(AppConfig::from_env());
    info!("Starting email processor with config: {:?}", config);

    // AWS SDK 클라이언트 초기화
    let region_provider = RegionProviderChain::default_provider().or_else("ap-northeast-2");
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let ses_client = Arc::new(SesClient::new(&aws_config));

    // NATS Jetstream 연결 및 스트림/소비자 설정
    let client = async_nats::connect(&config.nats_url).await?;
    let js = jetstream::new(client);
    let stream = js
        .get_or_create_stream(StreamConfig {
            name: config.stream_name.clone(),
            subjects: vec![format!("{}.*", config.stream_name)],
            ..Default::default()
        })
        .await?;
    let consumer = stream
        .get_or_create_consumer(
            &config.durable_name,
            PullConfig {
                durable_name: Some(config.durable_name.clone()),
                filter_subject: config.subject_filter.clone(),
                ack_policy: AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                max_ack_pending: config.max_ack_pending,
                ..Default::default()
            },
        )
        .await?;

    // 멱등성 보장을 위한 캐시 초기화
    let idempotent_cache = Arc::new(
        Cache::builder()
            .time_to_live(Duration::from_secs(config.idempotency_window_secs))
            .build(),
    );

    // 속도 제한기 초기화 및 토큰 리필 태스크 시작
    let rate_limiter = RateLimiter::new(config.ses_rate_per_sec);
    rate_limiter.spawn_refill_task(config.ses_rate_per_sec);

    // 생산자와 소비자 간의 통신을 위한 MPSC 채널 생성
    let (tx, rx) = mpsc::channel(config.channel_capacity);

    // 생산자(fetcher)와 소비자(sender) 태스크 실행
    let fetcher = tokio::spawn(fetch_and_dispatch(
        consumer,
        tx,
        idempotent_cache,
        Arc::clone(&config),
    ));
    let sender = tokio::spawn(email_sender(rx, ses_client, rate_limiter, config));

    info!("All tasks started successfully");

    // 종료 신호(Ctrl+C) 또는 태스크 종료 대기
    tokio::select! {
        _ = tokio::signal::ctrl_c() => { info!("Received shutdown signal"); }
        res = fetcher => { error!("Fetcher task ended: {:?}", res); }
        res = sender => { error!("Sender task ended: {:?}", res); }
    }

    info!("Shutdown complete");
    Ok(())
}
