use antithesis_sdk::random::AntithesisRng;
use eyre::eyre;
use rand::Rng;
use s2::{
    StreamClient, Streaming,
    client::ClientError,
    types,
    types::{AppendInput, AppendRecord, AppendRecordBatch, MeteredBytes, ReadOutput, ReadStart},
};
use serde::Serialize;
use std::sync::{Arc, atomic::AtomicU64};
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::StreamExt;
use tonic::Code;
use tracing::Level;
use tracing::{debug, error, trace};

const MAX_BATCH_BYTES: usize = 1024;
const PER_RECORD_OVERHEAD: usize = 8;

pub fn generate_records(num_records: usize) -> eyre::Result<AppendRecordBatch> {
    let mut records = AppendRecordBatch::with_max_capacity(num_records);
    let mut batch_bytes: usize = 0;
    let mut rng = AntithesisRng;

    while !records.is_full() && batch_bytes + PER_RECORD_OVERHEAD < MAX_BATCH_BYTES {
        let record_body_budget = MAX_BATCH_BYTES - batch_bytes - PER_RECORD_OVERHEAD;

        let size = rng.gen_range(1..=record_body_budget);
        let mut body = vec![0u8; size];
        rng.fill(&mut body[..]);

        let record = AppendRecord::new(body).expect("capacity");
        let metered_size = record.metered_bytes();
        if let Err(e) = records.push(record) {
            error!(?e, "failed to push record");
            break;
        }

        batch_bytes += metered_size as usize;
    }

    assert!(records.len() <= num_records);
    assert!(records.metered_bytes() <= MAX_BATCH_BYTES as u64);

    Ok(records)
}

#[derive(Serialize, Debug)]
pub enum Op {
    Append,
    Read,
    CheckTail,
}

#[derive(Serialize, Debug)]
pub struct Start {
    op: Op,
    num_records: Option<usize>,
}

#[derive(Serialize, Debug)]
pub struct Finish {
    op: Op,
    definite_failure: bool,
    failure: bool,
    tail: Option<u64>,
}

#[derive(Serialize, Debug)]
pub enum Event {
    Start(Start),
    Finish(Finish),
}

#[derive(Serialize, Debug)]
pub struct LabeledEvent {
    event: Event,
    client_id: u8,
    op_id: u64,
}

fn random_op() -> Op {
    match AntithesisRng.gen_range(0..3) {
        0 => Op::Append,
        1 => Op::Read,
        2 => Op::CheckTail,
        _ => unreachable!(),
    }
}

pub async fn client(
    num_ops: usize,
    stream: StreamClient,
    client_id: u8,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<()> {
    for sample in 0..num_ops {
        debug!(?client_id, ?sample);
        let op_id = op_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match random_op() {
            Op::Append => {
                let batch = generate_records(AntithesisRng.gen_range(1..1000))?;
                append(history_tx.clone(), stream.clone(), batch, client_id, op_id).await?;
            }
            Op::Read => read_session(history_tx.clone(), stream.clone(), client_id, op_id).await?,
            Op::CheckTail => {
                check_tail(history_tx.clone(), stream.clone(), client_id, op_id).await?
            }
        }
    }

    Ok(())
}

#[tracing::instrument(level = Level::TRACE, skip_all)]
async fn resolve_read_tail(mut stream: Streaming<ReadOutput>) -> eyre::Result<Finish> {
    let mut tail = 0;
    while let Some(resp) = stream.next().await {
        match resp {
            Ok(ReadOutput::Batch(batch)) => {
                let Some(last) = batch.records.last() else {
                    return Err(eyre!("received empty batch"));
                };
                tail = last.seq_num + 1;
            }
            Ok(ReadOutput::NextSeqNum(_nsn)) => {
                return Ok(Finish {
                    op: Op::Read,
                    definite_failure: false,
                    failure: false,
                    tail: None,
                });
            }
            Err(e) => {
                error!(?e, "read error");
                return Ok(Finish {
                    op: Op::Read,
                    definite_failure: true,
                    failure: true,
                    tail: None,
                });
            }
        }
    }
    Ok(Finish {
        op: Op::Read,
        definite_failure: false,
        failure: false,
        tail: Some(tail),
    })
}

#[tracing::instrument(level = Level::TRACE, skip(history_tx, stream))]
async fn read_session(
    history_tx: UnboundedSender<LabeledEvent>,
    stream: StreamClient,
    client_id: u8,
    op_id: u64,
) -> eyre::Result<()> {
    history_tx.send(LabeledEvent {
        event: Event::Start(Start {
            op: Op::Read,
            num_records: None,
        }),
        client_id,
        op_id,
    })?;

    let read_session = stream
        .read_session(types::ReadSessionRequest {
            start: ReadStart::TailOffset(1),
            // Read must include a limit, otherwise we will enter a tailing session.
            limit: types::ReadLimit::new().with_count(u64::MAX),
            ..Default::default()
        })
        .await;

    let finish = match read_session {
        Ok(stream) => resolve_read_tail(stream).await?,
        Err(_e) => Finish {
            op: Op::Read,
            definite_failure: true,
            failure: true,
            tail: None,
        },
    };

    history_tx.send(LabeledEvent {
        event: Event::Finish(finish),
        client_id,
        op_id,
    })?;

    Ok(())
}

#[tracing::instrument(level = Level::TRACE, skip(history_tx, stream))]
async fn check_tail(
    history_tx: UnboundedSender<LabeledEvent>,
    stream: StreamClient,
    client_id: u8,
    op_id: u64,
) -> eyre::Result<()> {
    history_tx.send(LabeledEvent {
        event: Event::Start(Start {
            op: Op::CheckTail,
            num_records: None,
        }),
        client_id,
        op_id,
    })?;

    let resp = stream.check_tail().await;
    trace!(?resp, "check_tail response");
    let finish = match resp {
        Ok(s) => Finish {
            op: Op::CheckTail,
            definite_failure: false,
            failure: false,
            tail: Some(s.seq_num),
        },
        Err(_e) => Finish {
            op: Op::CheckTail,
            definite_failure: true,
            failure: true,
            tail: None,
        },
    };
    history_tx.send(LabeledEvent {
        event: Event::Finish(finish),
        client_id,
        op_id,
    })?;

    Ok(())
}

#[tracing::instrument(level = Level::TRACE, skip(history_tx, stream, records))]
async fn append(
    history_tx: UnboundedSender<LabeledEvent>,
    stream: StreamClient,
    records: AppendRecordBatch,
    client_id: u8,
    op_id: u64,
) -> eyre::Result<()> {
    let start = Start {
        op: Op::Append,
        num_records: Some(records.len()),
    };

    history_tx.send(LabeledEvent {
        event: Event::Start(start),
        client_id,
        op_id,
    })?;

    let resp = stream.append(AppendInput::new(records)).await;
    trace!(?resp, "append response");
    let finish = match resp {
        Ok(ack) => Finish {
            op: Op::Append,
            definite_failure: false,
            failure: false,
            tail: Some(ack.end.seq_num),
        },
        Err(err) => {
            let definite_failure = match err {
                ClientError::Conversion(_e) => true,
                ClientError::Service(status) => matches!(
                    status.code(),
                    Code::InvalidArgument
                        | Code::NotFound
                        | Code::AlreadyExists
                        | Code::PermissionDenied
                        | Code::ResourceExhausted
                        | Code::FailedPrecondition
                        | Code::Aborted
                        | Code::OutOfRange
                        | Code::Unimplemented
                        | Code::Unauthenticated
                ),
            };
            Finish {
                op: Op::Append,
                definite_failure,
                failure: true,
                tail: None,
            }
        }
    };
    history_tx.send(LabeledEvent {
        event: Event::Finish(finish),
        client_id,
        op_id,
    })?;

    Ok(())
}

/// Since the linearizability model expects a stream's tail
/// to start at 0, this fn allows us to "correct" the initial state
/// for any non-empty stream, by spoofing a successful append from 0
/// to whatever the current tail is.
pub async fn initialize_tail(
    history_tx: UnboundedSender<LabeledEvent>,
    op_id: u64,
    tail: u64,
) -> eyre::Result<()> {
    history_tx.send(LabeledEvent {
        event: Event::Start(Start {
            op: Op::Append,
            num_records: Some(tail as usize),
        }),
        client_id: 0,
        op_id,
    })?;
    history_tx.send(LabeledEvent {
        event: Event::Finish(Finish {
            op: Op::Append,
            definite_failure: false,
            failure: false,
            tail: Some(tail),
        }),
        client_id: 0,
        op_id,
    })?;

    Ok(())
}
