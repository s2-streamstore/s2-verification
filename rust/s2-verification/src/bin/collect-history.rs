use clap::{Parser, ValueEnum};
use eyre::eyre;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use s2::client::{AppendRetryPolicy, ClientError, S2Endpoints};
use s2::types::{CreateStreamRequest, ReadLimit, ReadOutput, ReadRequest, ReadStart};
use s2::{Client, ClientConfig, types};
use s2_verification::history::{
    client, fencing_token_client, initialize_tail, match_seq_num_client,
};
use std::env;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tonic::Code;
use tonic::codegen::http;
use tracing::{debug, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

    let disable_tls = std::env::var("S2_DISABLE_TLS")
        .ok()
        .and_then(|val| val.to_lowercase().parse::<bool>().ok())
        .unwrap_or(false);

    let basin = types::BasinName::try_from(basin)?;
    let stream: &'static str = stream.leak();

    let config = ClientConfig::new(env::var("S2_ACCESS_TOKEN")?)
        .with_endpoints(S2Endpoints::from_env().map_err(|e| eyre::eyre!(e))?)
        .with_append_retry_policy(AppendRetryPolicy::NoSideEffects)
        .with_uri_scheme(if disable_tls {
            http::uri::Scheme::HTTP
        } else {
            http::uri::Scheme::HTTPS
        });

    let basin_client = Client::new(config.clone()).basin_client(basin.clone());
    let _stream_exists = match basin_client
        .create_stream(CreateStreamRequest::new(stream))
        .await
    {
        Ok(_) => true,
        Err(ClientError::Service(status)) => status.code() == Code::AlreadyExists,
        Err(e) => return Err(eyre!(e)),
    };

    let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
    let op_ids = Arc::new(AtomicU64::new(0));

    let stream_client = Client::new(config.clone())
        .basin_client(basin.clone())
        .stream_client(stream);
    let _resp = stream_client.check_tail().await?;
    let resp = stream_client
        .read(ReadRequest::new(ReadStart::TailOffset(1)).with_limit(ReadLimit::new().with_count(1)))
        .await?;

    let rectify = match resp {
        ReadOutput::Batch(batch) => {
            let last = batch.records.last().expect("batch has at least one record");
            let tail = last.seq_num + 1;
            Some((tail, crc32fast::hash(last.body.as_ref())))
        }
        ReadOutput::NextSeqNum(0) => None,
        _ => return Err(eyre!("impossible to rectify")),
    };
    if let Some((tail, crc)) = rectify {
        info!(
            "check-tail indicates stream is not empty, inserting a starter append event to rectify"
        );
        initialize_tail(
            history_tx.clone(),
            op_ids.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            tail,
            crc,
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
        info!(path, "writer finished");
    });

    debug!("starting concurrent clients");
    let mut futs = FuturesUnordered::new();
    for client_id in 0..num_concurrent_clients {
        let stream_client = Client::new(config.clone())
            .basin_client(basin.clone())
            .stream_client(stream);

        let fut: std::pin::Pin<Box<dyn std::future::Future<Output = eyre::Result<()>> + Send>> =
            match workflow {
                Workflow::Regular => Box::pin(client(
                    num_ops_per_client,
                    stream_client,
                    client_id,
                    op_ids.clone(),
                    history_tx.clone(),
                )),
                Workflow::MatchSeqNum => Box::pin(match_seq_num_client(
                    num_ops_per_client,
                    stream_client,
                    client_id,
                    op_ids.clone(),
                    history_tx.clone(),
                )),
                Workflow::Fencing => Box::pin(fencing_token_client(
                    num_ops_per_client,
                    stream_client,
                    client_id,
                    op_ids.clone(),
                    history_tx.clone(),
                )),
            };

        futs.push(fut);
    }
    while let Some(_f) = futs.next().await {}
    debug!("all clients finished");

    // tx drop signals to the writer task that it can stop
    drop(history_tx);

    writer.await?;

    Ok(())
}
