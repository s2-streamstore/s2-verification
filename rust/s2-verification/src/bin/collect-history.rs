use clap::{Parser, ValueEnum};
use eyre::eyre;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use s2_sdk::{
    S2,
    types::{
        AppendRetryPolicy, BasinName, CreateStreamInput, ReadFrom, ReadInput, ReadLimits,
        ReadStart, ReadStop, RetryConfig, S2Config, S2Endpoints, S2Error, StreamName,
    },
};
use s2_verification::history::{
    CallFinish, Event, LabeledEvent, client, fencing_token_client, initialize_tail,
    match_seq_num_client,
};
use std::env;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use xxhash_rust::xxh3::xxh3_64;

#[derive(ValueEnum, Clone, Debug)]
enum Workflow {
    Regular,
    MatchSeqNum,
    Fencing,
}

#[derive(Parser, Debug)]
struct Args {
    basin: String,
    stream: String,
    #[clap(long, default_value_t = 5)]
    num_concurrent_clients: u16,
    #[clap(long, default_value_t = 100)]
    num_ops_per_client: usize,
    #[clap(long, value_enum, default_value = "regular")]
    workflow: Workflow,
}

pub fn init_tracing() {
    let registry =
        tracing_subscriber::registry().with(tracing_subscriber::EnvFilter::from_default_env());
    let formatter = tracing_subscriber::fmt::layer()
        .pretty()
        .with_thread_ids(true)
        .with_writer(std::io::stderr);
    registry.with(formatter.compact()).init()
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    init_tracing();

    let Args {
        basin,
        stream,
        num_concurrent_clients,
        num_ops_per_client,
        workflow,
    } = Args::parse();

    let basin: BasinName = basin.parse()?;
    let stream: StreamName = stream.parse()?;

    let custom_endpoints = S2Endpoints::from_env().ok();
    let mut config = S2Config::new(env::var("S2_ACCESS_TOKEN")?).with_retry(
        RetryConfig::new()
            .with_max_attempts(NonZeroU32::new(1024).expect("non-zero"))
            .with_min_base_delay(Duration::from_millis(1000)),
    );

    if let Some(endpoints) = custom_endpoints {
        config = config.with_endpoints(endpoints);
    }

    let test_config = config.clone().with_retry(
        RetryConfig::default().with_append_retry_policy(AppendRetryPolicy::NoSideEffects),
    );

    let s2 = S2::new(config)?;
    let basin_client = s2.basin(basin.clone());
    let _stream_exists = match basin_client
        .create_stream(CreateStreamInput::new(stream.clone()))
        .await
    {
        Ok(_) => true,
        Err(S2Error::Server(e)) if e.code.as_str() == "resource_already_exists" => true,
        Err(e) => return Err(eyre!(e)),
    };

    let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
    let client_ids = Arc::new(AtomicU64::new(1));
    let op_ids = Arc::new(AtomicU64::new(0));

    let stream_client = s2.basin(basin.clone()).stream(stream.clone());
    let _resp = stream_client.check_tail().await?;
    let batch = stream_client
        .read(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::TailOffset(1)))
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(1))),
        )
        .await;

    // See if the specified stream has been written to; if it has, grab the tail and hash.
    //
    // Note that we are not using `check_tail` since we need access not just to the tail, but
    // also the actual record's content.
    let rectify = match batch {
        Ok(batch) => {
            let last = batch.records.last().expect("batch has at least one record");
            let tail = last.seq_num + 1;
            Some((tail, xxh3_64(last.body.as_ref())))
        }
        Err(S2Error::ReadUnwritten(position)) => {
            assert_eq!(position.seq_num, 0);
            assert_eq!(position.timestamp, 0);
            None
        }
        Err(e) => return Err(eyre!(e)),
    };

    if let Some((tail, xxh3)) = rectify {
        info!("stream is not empty, inserting a starter append event to rectify");
        initialize_tail(
            history_tx.clone(),
            op_ids.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            tail,
            xxh3,
        )
        .await?;
    }

    let writer = tokio::spawn(async move {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock normal behavior");

        let path = format!("./data/records.{}.jsonl", now.as_secs());
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .expect("output file");

        let mut writer = tokio::io::BufWriter::new(file);
        while let Some(record) = history_rx.recv().await {
            let mut json = serde_json::to_string(&record).expect("serialize record");
            json.push('\n');
            writer
                .write_all(json.as_bytes())
                .await
                .expect("write to writer");
        }
        writer.flush().await.expect("flush writer");
        info!("writer finished");

        println!("{path}");
    });

    debug!("starting concurrent clients");
    let futs = FuturesUnordered::new();
    for _client_id in 0..num_concurrent_clients {
        let s2 = S2::new(test_config.clone())?;
        let stream_client = s2.basin(basin.clone()).stream(stream.clone());

        let fut: std::pin::Pin<
            Box<dyn std::future::Future<Output = eyre::Result<Vec<LabeledEvent>>> + Send>,
        > = match workflow {
            Workflow::Regular => Box::pin(client(
                num_ops_per_client,
                stream_client,
                client_ids.clone(),
                op_ids.clone(),
                history_tx.clone(),
            )),
            Workflow::MatchSeqNum => Box::pin(match_seq_num_client(
                num_ops_per_client,
                stream_client,
                client_ids.clone(),
                op_ids.clone(),
                history_tx.clone(),
            )),
            Workflow::Fencing => Box::pin(fencing_token_client(
                num_ops_per_client,
                stream_client,
                client_ids.clone(),
                op_ids.clone(),
                history_tx.clone(),
            )),
        };

        futs.push(fut);
    }
    let deferred = futs.collect::<Vec<_>>().await;
    debug!(?deferred, "all clients finished");

    for result in deferred {
        for fin in result? {
            assert!(matches!(
                fin.event,
                Event::Finish(CallFinish::AppendIndefiniteFailure)
            ));
            history_tx.send(fin)?;
        }
    }

    // tx drop signals to the writer task that it can stop
    drop(history_tx);

    writer.await?;

    Ok(())
}
