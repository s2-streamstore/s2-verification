use antithesis_sdk::random::AntithesisRng;
use eyre::{eyre, OptionExt};
use rand::Rng;
use s2::types::{CommandRecord, FencingToken};
use s2::{
    StreamClient, Streaming,
    client::ClientError,
    types,
    types::{AppendInput, AppendRecord, AppendRecordBatch, MeteredBytes, ReadOutput, ReadStart},
};
use serde::Serialize;
use std::sync::{Arc, atomic::AtomicU64};
use std::time::Duration;
use crc32fast::Hasher;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::StreamExt;
use tonic::Code;
use tracing::Level;
use tracing::{debug, error, trace};

const MAX_BATCH_BYTES: usize = 1024;
const PER_RECORD_OVERHEAD: usize = 8;
const INDEFINITE_APPEND_WAIT: Duration = Duration::from_secs(1);

/// Create a batch of records containing random data.
pub fn generate_records(num_records: usize) -> eyre::Result<(AppendRecordBatch)> {
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
pub enum CallStart {
    Append {
        num_records: u64,
        last_record_crc32: u32,
        set_fencing_token: Option<String>,
        fencing_token: Option<String>,
        match_seq_num: Option<u64>,
    },
    Read,
    CheckTail,
}

#[derive(Serialize, Debug)]
pub enum CallFinish {
    AppendDefiniteFailure,
    AppendIndefiniteFailure,
    AppendSuccess { tail: u64 },
    CheckTailFailure,
    CheckTailSuccess { tail: u64 },
    ReadFailure,
    ReadSuccess { tail: u64, crc32: u32 },
}

#[derive(Serialize, Debug)]
pub enum Op {
    Append,
    Read,
    CheckTail,
}

#[derive(Serialize, Debug)]
pub enum Event {
    Start(CallStart),
    Finish(CallFinish),
}

#[derive(Serialize, Debug)]
pub struct LabeledEvent {
    event: Event,
    client_id: u16,
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

pub async fn fencing_token_client(
    num_ops: usize,
    stream: StreamClient,
    client_id: u16,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<()> {
    let my_token = format!("client_{client_id}");
    debug!(?my_token);
    let mut expected_next_seq_num = 0;
    for sample in 0..num_ops {
        let op_id = op_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if sample % 100 == 0 {
            // Attempt to set stream's token to my_token...
            let mut set_token_batch = AppendRecordBatch::new();
            set_token_batch
                .push(CommandRecord::fence(FencingToken::try_from(
                    my_token.clone(),
                )?))
                .map_err(|_| eyre!("failed to push fencing token"))?;
            match append(
                history_tx.clone(),
                stream.clone(),
                set_token_batch,
                client_id,
                op_id,
                Some(expected_next_seq_num),
                None,
            )
            .await?
            {
                None => {}
                Some(_tail) => {
                    debug!("token set to {}", my_token);
                }
            }
        } else {
            debug!(?client_id, ?sample);
            if let Some(tail) = match random_op() {
                Op::Append => {
                    let batch = generate_records(AntithesisRng.gen_range(1..1000))?;
                    append(
                        history_tx.clone(),
                        stream.clone(),
                        batch,
                        client_id,
                        op_id,
                        Some(expected_next_seq_num),
                        Some(my_token.clone()),
                    )
                    .await?
                }
                Op::Read => {
                    read_session(history_tx.clone(), stream.clone(), client_id, op_id).await?
                }
                Op::CheckTail => {
                    check_tail(history_tx.clone(), stream.clone(), client_id, op_id).await?
                }
            } {
                expected_next_seq_num = tail;
            }
        }
    }

    Ok(())
}

pub async fn match_seq_num_client(
    num_ops: usize,
    stream: StreamClient,
    client_id: u16,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<()> {
    let mut expected_next_seq_num = 0;
    for sample in 0..num_ops {
        debug!(?client_id, ?sample);
        let op_id = op_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(tail) = match random_op() {
            Op::Append => {
                let batch = generate_records(AntithesisRng.gen_range(1..1000))?;
                append(
                    history_tx.clone(),
                    stream.clone(),
                    batch,
                    client_id,
                    op_id,
                    Some(expected_next_seq_num),
                    None,
                )
                .await?
            }
            Op::Read => read_session(history_tx.clone(), stream.clone(), client_id, op_id).await?,
            Op::CheckTail => {
                check_tail(history_tx.clone(), stream.clone(), client_id, op_id).await?
            }
        } {
            expected_next_seq_num = tail;
        }
    }

    Ok(())
}

pub async fn client(
    num_ops: usize,
    stream: StreamClient,
    client_id: u16,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<()> {
    for sample in 0..num_ops {
        debug!(?client_id, ?sample);
        let op_id = op_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match random_op() {
            Op::Append => {
                let batch = generate_records(AntithesisRng.gen_range(1..1000))?;
                append(
                    history_tx.clone(),
                    stream.clone(),
                    batch,
                    client_id,
                    op_id,
                    None,
                    None,
                )
                .await?;
            }
            Op::Read => {
                read_session(history_tx.clone(), stream.clone(), client_id, op_id).await?;
            }
            Op::CheckTail => {
                check_tail(history_tx.clone(), stream.clone(), client_id, op_id).await?;
            }
        }
    }

    Ok(())
}

#[tracing::instrument(level = Level::TRACE, skip_all)]
async fn resolve_read_tail(mut stream: Streaming<ReadOutput>) -> eyre::Result<CallFinish> {
    let mut tail = 0;
    let mut crc32 = 0;
    while let Some(resp) = stream.next().await {
        trace!(?resp, "read response");
        match resp {
            Ok(ReadOutput::Batch(batch)) => {
                let Some(last) = batch.records.last() else {
                    return Err(eyre!("received empty batch"));
                };
                crc32 = crc32fast::hash(last.body.as_ref());
                tail = last.seq_num + 1;
            }
            Ok(ReadOutput::NextSeqNum(nsn)) => {
                trace!(nsn, "next_seq_num");
                return Ok(CallFinish::ReadSuccess { tail: nsn, crc32 });
            }
            Err(e) => {
                error!(?e, "read error");
                return Ok(CallFinish::ReadFailure);
            }
        }
    }
    Ok(CallFinish::ReadSuccess { tail, crc32 })
}

#[tracing::instrument(level = Level::TRACE, skip(history_tx, stream))]
async fn read_session(
    history_tx: UnboundedSender<LabeledEvent>,
    stream: StreamClient,
    client_id: u16,
    op_id: u64,
) -> eyre::Result<Option<u64>> {
    history_tx.send(LabeledEvent {
        event: Event::Start(CallStart::Read),
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
        Ok(stream) => {
            trace!("read_session stream");
            resolve_read_tail(stream).await?
        }
        Err(_e) => {
            trace!("read_session error");
            CallFinish::ReadFailure
        }
    };

    let tail = if let CallFinish::ReadSuccess { tail, crc32 } = finish {
        Some(tail)
    } else {
        None
    };

    history_tx.send(LabeledEvent {
        event: Event::Finish(finish),
        client_id,
        op_id,
    })?;

    Ok(tail)
}

#[tracing::instrument(level = Level::TRACE, skip(history_tx, stream))]
async fn check_tail(
    history_tx: UnboundedSender<LabeledEvent>,
    stream: StreamClient,
    client_id: u16,
    op_id: u64,
) -> eyre::Result<Option<u64>> {
    history_tx.send(LabeledEvent {
        event: Event::Start(CallStart::CheckTail),
        client_id,
        op_id,
    })?;
    let resp = stream.check_tail().await;
    trace!(?resp, "check_tail response");
    let finish = match resp {
        Ok(pos) => CallFinish::CheckTailSuccess { tail: pos.seq_num },
        Err(_e) => CallFinish::CheckTailFailure,
    };
    let tail = if let CallFinish::CheckTailSuccess { tail } = finish {
        Some(tail)
    } else {
        None
    };
    history_tx.send(LabeledEvent {
        event: Event::Finish(finish),
        client_id,
        op_id,
    })?;

    Ok(tail)
}

#[tracing::instrument(level = Level::TRACE, skip(history_tx, stream, records))]
async fn append(
    history_tx: UnboundedSender<LabeledEvent>,
    stream: StreamClient,
    records: AppendRecordBatch,
    client_id: u16,
    op_id: u64,
    match_seq_num: Option<u64>,
    fencing_token: Option<String>,
) -> eyre::Result<Option<u64>> {
    let mut set_fencing_token = None;
    if records.len() == 1
        && let Some(rec) = records.as_ref().iter().next()
        && let Some(header) = rec.headers().first()
        && header.name.is_empty()
        && header.value == "fence"
    {
        let token = String::from_utf8(rec.body().to_vec())?;
        set_fencing_token = Some(token);
    }

    // Grab crc32 of last record in batch.
    let mut crc_hasher = Hasher::new();
    crc_hasher.update(records.as_ref().iter().last().ok_or_eyre("no records in batch")?.body());
    let crc = crc_hasher.finalize();

    let start = CallStart::Append {
        num_records: records.len() as u64,
        last_record_crc32: crc,
        set_fencing_token,
        fencing_token: fencing_token.clone(),
        match_seq_num,
    };
    history_tx.send(LabeledEvent {
        event: Event::Start(start),
        client_id,
        op_id,
    })?;

    let mut input = AppendInput::new(records);
    if let Some(match_seq_num) = match_seq_num {
        input = input.with_match_seq_num(match_seq_num);
    }
    if let Some(fencing_token) = fencing_token {
        input = input.with_fencing_token(FencingToken::try_from(fencing_token)?);
    }
    let resp = stream.append(input).await;
    trace!(?resp, "append response");
    let finish = match resp {
        Ok(ack) => CallFinish::AppendSuccess {
            tail: ack.end.seq_num,
        },
        Err(e) => {
            let finish = match e {
                ClientError::Conversion(_) => CallFinish::AppendDefiniteFailure,
                ClientError::Service(status)
                    if matches!(
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
                    ) =>
                {
                    CallFinish::AppendDefiniteFailure
                }
                _ => CallFinish::AppendIndefiniteFailure,
            };

            if let CallFinish::AppendIndefiniteFailure = finish {
                // The append experienced an indefinite failure, meaning we do not know
                // if it had a side-effect.

                // This is fine, and something we can account for in the linearizability model,
                // however this append should only be considered to be finished after any
                // potential side-effect must have occurred.

                // Only once we are confident that any effect from the append would have already
                // occurred (regardless of whether or not it did occur), can we consider this
                // append to be finished.

                // In the future, a good mechanism for this will be to use an empty-batch append
                // against the same stream. If that succeeds, then we know:
                //  - all prior attempted appends, if they were to become durable, would have
                //    become so by this point

                // Until we support that mechanism, we can alternatively wait for a period of time
                // greater than the worst-case durability flush period for S2. In other words, we
                // wait a period of time long enough that it would be impossible for S2 to have not
                // committed a prior append that still will end up being validly appended.

                tokio::time::sleep(INDEFINITE_APPEND_WAIT).await;
            }

            finish
        }
    };

    let tail = if let CallFinish::AppendSuccess { tail } = finish {
        Some(tail)
    } else {
        None
    };

    history_tx.send(LabeledEvent {
        event: Event::Finish(finish),
        client_id,
        op_id,
    })?;

    Ok(tail)
}

/// Since the linearizability model expects a stream's tail
/// to start at 0, this fn allows us to "correct" the initial state
/// for any non-empty stream, by spoofing a successful append from 0
/// to whatever the current tail is.
pub async fn initialize_tail(
    history_tx: UnboundedSender<LabeledEvent>,
    op_id: u64,
    tail: u64,
    crc32: u32
) -> eyre::Result<()> {
    history_tx.send(LabeledEvent {
        event: Event::Start(CallStart::Append {
            num_records: tail,
            last_record_crc32: crc32,
            set_fencing_token: None,
            fencing_token: None,
            match_seq_num: None,
        }),
        client_id: 0,
        op_id,
    })?;
    history_tx.send(LabeledEvent {
        event: Event::Finish(CallFinish::AppendSuccess { tail }),
        client_id: 0,
        op_id,
    })?;

    Ok(())
}
