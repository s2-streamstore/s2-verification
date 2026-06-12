use antithesis_sdk::random::AntithesisRng;
use rand::Rng;
use s2_sdk::{
    S2Stream,
    types::{
        AppendInput, AppendRecord, AppendRecordBatch, CommandRecord, FencingToken, MeteredBytes,
        ReadBatch, ReadFrom, ReadInput, ReadLimits, ReadStart, ReadStop, S2Error, Streaming,
    },
};
use serde::Serialize;
use std::sync::{Arc, atomic::AtomicU64};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::StreamExt;
use tracing::{Level, warn};
use tracing::{debug, error, trace};
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

const MAX_BATCH_BYTES: usize = 1024;
const PER_RECORD_OVERHEAD: usize = 8;

/// Amount of time for a client which has experienced an indefinite failure
/// to wait before acquiring a new client_id and attempting another op.
///
/// This is not required but avoids churning rapidly through new clients
/// during a period of S2 outage.
const INDEFINITE_FAILURE_BACKOFF: Duration = Duration::from_millis(1000);

/// Maximum amount of client_ids to support in the log. If we hit this, our clients
/// will end (early). Generally speaking, the more discrete clients in our log, the
/// harder it is to verify linearizability in a reasonable amount of time.
const MAX_CLIENT_IDS: u64 = 20;

/// Fold one record-body hash into a cumulative stream hash.
///
/// The cumulative hash over a stream is defined as the left fold of this
/// function over the xxh3 of every record body, in sequence order, starting
/// from 0 for the empty stream. Each value commits to the entire stream
/// prefix, so it can be stored in the linearizability model's state in place
/// of the stream contents.
///
/// Must stay in sync with `chainHash` in the Go model.
pub fn chain_hash(stream_hash: u64, record_hash: u64) -> u64 {
    xxh3_64_with_seed(&record_hash.to_le_bytes(), stream_hash)
}

pub struct GeneratedBatch {
    batch: AppendRecordBatch,
    record_hashes: Vec<u64>,
}

/// Create a batch of records containing random data.
/// Returns the batch and the xxh3 hash of each record's body.
pub fn generate_records(num_records: usize) -> eyre::Result<GeneratedBatch> {
    let mut records = Vec::new();
    let mut batch_bytes: usize = 0;
    let mut record_hashes = Vec::new();
    let mut rng = AntithesisRng;

    while records.len() < num_records && batch_bytes + PER_RECORD_OVERHEAD < MAX_BATCH_BYTES {
        let record_body_budget = MAX_BATCH_BYTES - batch_bytes - PER_RECORD_OVERHEAD;

        let size = rng.gen_range(1..=record_body_budget);
        let mut body = vec![0u8; size];
        rng.fill(&mut body[..]);

        // Compute hash before creating record
        record_hashes.push(xxh3_64(&body));

        let record = AppendRecord::new(body)?;
        let metered_size = record.metered_bytes();

        batch_bytes += metered_size;
        records.push(record);
    }

    let batch = AppendRecordBatch::try_from_iter(records)?;
    Ok(GeneratedBatch {
        batch,
        record_hashes,
    })
}

#[derive(Serialize, Debug, Clone)]
pub enum CallStart {
    Append {
        num_records: u64,
        /// xxh3 of each record body in the batch, in order. The model folds
        /// these onto its cumulative stream hash via [`chain_hash`].
        record_hashes: Vec<u64>,
        set_fencing_token: Option<String>,
        fencing_token: Option<String>,
        match_seq_num: Option<u64>,
    },
    Read,
    CheckTail,
}

#[derive(Serialize, Clone, Debug)]
pub enum CallFinish {
    AppendDefiniteFailure,
    AppendIndefiniteFailure,
    AppendSuccess {
        tail: u64,
    },
    CheckTailFailure,
    CheckTailSuccess {
        tail: u64,
    },
    ReadFailure,
    ReadSuccess {
        tail: u64,
        /// Cumulative [`chain_hash`] over every record body observed from the
        /// head of the stream (seq_num 0) through the tail.
        xxh3: u64,
    },
}

#[derive(Serialize, Debug)]
pub enum Op {
    Append,
    Read,
    CheckTail,
}

#[derive(Serialize, Debug, Clone)]
pub enum Event {
    Start(CallStart),
    Finish(CallFinish),
}

#[derive(Serialize, Clone, Debug)]
pub struct LabeledEvent {
    pub event: Event,
    client_id: u64,
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

/// Handle an indefinite failure by deferring the event and attempting to rotate to a new client ID.
///
/// Returns `Some(new_client_id)` if a new client ID was successfully acquired,
/// or `None` if the maximum number of client IDs has been reached (caller should break).
async fn handle_indefinite_failure(
    fin: &LabeledEvent,
    deferred: &mut Vec<LabeledEvent>,
    client_id_atomic: &Arc<AtomicU64>,
) -> Option<u64> {
    // Call failed indefinitely, so we hold on to the finish log, and also assume a new
    // client identity, as the old one can no longer be used.
    deferred.push(fin.clone());
    tokio::time::sleep(INDEFINITE_FAILURE_BACKOFF).await;
    let client_id_candidate = client_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if client_id_candidate < MAX_CLIENT_IDS {
        Some(client_id_candidate)
    } else {
        warn!("max client ids reached");
        None
    }
}

/// Run a client that randomly selects between ops.
///
/// When append operations are attempted, this client will specify a fencing token value.
/// The fencing token is unique to this client.
///
/// Additionally, every 100 operations (including zero'th), the client will attempt to set the
/// stream's fencing token. This append will use a `matchSeqNum` to avoid a simple last-write-win
/// situation.
///
/// Returns a list of deferred events, which were not communicated via `history_tx`.
/// These correspond to `AppendIndefiniteFailure` events.
pub async fn fencing_token_client(
    num_ops: usize,
    stream: S2Stream,
    client_id_atomic: Arc<AtomicU64>,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<Vec<LabeledEvent>> {
    const ATTEMPT_TO_SET_FENCE_TOKEN_EVERY: usize = 100;

    let mut client_id = client_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let my_token = FencingToken::generate(6)?;
    debug!(?my_token);
    let mut deferred = Vec::new();
    let mut expected_next_seq_num = 0;
    'samples: for sample in 0..num_ops {
        let op_id = op_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if sample % ATTEMPT_TO_SET_FENCE_TOKEN_EVERY == 0 {
            // Attempt to set stream's token to my_token...
            let fence_record: AppendRecord = CommandRecord::fence(my_token.clone()).into();
            // For fence commands, the body is the token bytes - compute hash
            let token_xxh3 = xxh3_64(my_token.as_bytes());
            let set_token_batch = AppendRecordBatch::try_from_iter([fence_record])?;
            let fin = append(
                history_tx.clone(),
                stream.clone(),
                set_token_batch,
                client_id,
                op_id,
                Some(expected_next_seq_num),
                None,
                Some(my_token.to_string()),
                vec![token_xxh3],
            )
            .await?;
            match fin.event {
                Event::Finish(CallFinish::AppendDefiniteFailure) => {}
                Event::Finish(CallFinish::AppendIndefiniteFailure) => {
                    if let Some(new_client_id) =
                        handle_indefinite_failure(&fin, &mut deferred, &client_id_atomic).await
                    {
                        client_id = new_client_id;
                    } else {
                        break 'samples;
                    }
                }
                Event::Finish(CallFinish::AppendSuccess { tail }) => {
                    expected_next_seq_num = tail;
                }
                _ => unreachable!(),
            }
        } else {
            debug!(?client_id, ?sample);
            let resp = match random_op() {
                Op::Append => {
                    let GeneratedBatch {
                        batch,
                        record_hashes,
                    } = generate_records(AntithesisRng.gen_range(1..1000))?;
                    let fin = append(
                        history_tx.clone(),
                        stream.clone(),
                        batch,
                        client_id,
                        op_id,
                        None,
                        Some(my_token.clone()),
                        None,
                        record_hashes,
                    )
                    .await?;
                    if let Event::Finish(CallFinish::AppendIndefiniteFailure) = fin.event {
                        if let Some(new_client_id) =
                            handle_indefinite_failure(&fin, &mut deferred, &client_id_atomic).await
                        {
                            client_id = new_client_id;
                        } else {
                            break 'samples;
                        }
                    }
                    fin
                }
                Op::Read => {
                    read_session(history_tx.clone(), stream.clone(), client_id, op_id).await?
                }
                Op::CheckTail => {
                    check_tail(history_tx.clone(), stream.clone(), client_id, op_id).await?
                }
            };
            if let Event::Finish(f) = resp.event
                && let CallFinish::AppendSuccess { tail }
                | CallFinish::ReadSuccess { tail, .. }
                | CallFinish::CheckTailSuccess { tail } = f
            {
                expected_next_seq_num = tail;
            }
        }
    }

    Ok(deferred)
}

/// Run a client that randomly selects between ops.
///
/// When append operations are attempted, this client will specify a `match_seq_num` value,
/// based on the most recent guess from a prior call.
///
/// Returns a list of deferred events, which were not communicated via `history_tx`.
/// These correspond to `AppendIndefiniteFailure` events.
pub async fn match_seq_num_client(
    num_ops: usize,
    stream: S2Stream,
    client_id_atomic: Arc<AtomicU64>,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<Vec<LabeledEvent>> {
    let mut client_id = client_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut deferred = Vec::new();
    let mut expected_next_seq_num = 0;
    'samples: for sample in 0..num_ops {
        debug!(?client_id, ?sample);
        let op_id = op_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let resp = match random_op() {
            Op::Append => {
                let GeneratedBatch {
                    batch,
                    record_hashes,
                } = generate_records(AntithesisRng.gen_range(1..1000))?;
                let fin = append(
                    history_tx.clone(),
                    stream.clone(),
                    batch,
                    client_id,
                    op_id,
                    Some(expected_next_seq_num),
                    None,
                    None,
                    record_hashes,
                )
                .await?;
                if let Event::Finish(CallFinish::AppendIndefiniteFailure) = fin.event {
                    if let Some(new_client_id) =
                        handle_indefinite_failure(&fin, &mut deferred, &client_id_atomic).await
                    {
                        client_id = new_client_id;
                    } else {
                        break 'samples;
                    }
                }
                fin
            }
            Op::Read => read_session(history_tx.clone(), stream.clone(), client_id, op_id).await?,
            Op::CheckTail => {
                check_tail(history_tx.clone(), stream.clone(), client_id, op_id).await?
            }
        };

        if let Event::Finish(f) = resp.event
            && let CallFinish::AppendSuccess { tail }
            | CallFinish::ReadSuccess { tail, .. }
            | CallFinish::CheckTailSuccess { tail } = f
        {
            expected_next_seq_num = tail;
        }
    }

    Ok(deferred)
}

/// Run a client that randomly selects between ops.
///
/// Appends are not gated by `match_seq_num` or fencing token. Fewer definite failures
/// are expected, compared to the `match_seq_num_client` and `fencing_token_client`.
///
/// Returns a list of deferred events, which were not communicated via `history_tx`.
/// These correspond to `AppendIndefiniteFailure` events.
pub async fn client(
    num_ops: usize,
    stream: S2Stream,
    client_id_atomic: Arc<AtomicU64>,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<Vec<LabeledEvent>> {
    let mut client_id = client_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut deferred = Vec::new();
    'samples: for sample in 0..num_ops {
        debug!(?client_id, ?sample);
        let op_id = op_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match random_op() {
            Op::Append => {
                let GeneratedBatch {
                    batch,
                    record_hashes,
                } = generate_records(AntithesisRng.gen_range(1..1000))?;
                let fin = append(
                    history_tx.clone(),
                    stream.clone(),
                    batch,
                    client_id,
                    op_id,
                    None,
                    None,
                    None,
                    record_hashes,
                )
                .await?;
                if let Event::Finish(CallFinish::AppendIndefiniteFailure) = fin.event {
                    if let Some(new_client_id) =
                        handle_indefinite_failure(&fin, &mut deferred, &client_id_atomic).await
                    {
                        client_id = new_client_id;
                    } else {
                        break 'samples;
                    }
                }
            }
            Op::Read => {
                read_session(history_tx.clone(), stream.clone(), client_id, op_id).await?;
            }
            Op::CheckTail => {
                check_tail(history_tx.clone(), stream.clone(), client_id, op_id).await?;
            }
        }
    }

    Ok(deferred)
}

#[tracing::instrument(level = Level::TRACE, skip_all)]
async fn resolve_read_tail(mut stream: Streaming<ReadBatch>) -> eyre::Result<CallFinish> {
    let mut tail = 0;
    let mut stream_hash = 0;
    while let Some(resp) = stream.next().await {
        trace!(?resp, "read response");
        match resp {
            Ok(batch) => {
                // Check if we got a tail position (indicates end of stream)
                if let Some(tail_pos) = batch.tail
                    && batch.records.is_empty()
                {
                    panic!(
                        "read_session yielded a tail-only empty batch: tail={}",
                        tail_pos.seq_num
                    );
                }
                for record in &batch.records {
                    stream_hash = chain_hash(stream_hash, xxh3_64(record.body.as_ref()));
                    tail = record.seq_num + 1;
                }
            }
            Err(e) => {
                error!(?e, "read error");
                return Ok(CallFinish::ReadFailure);
            }
        }
    }
    Ok(CallFinish::ReadSuccess {
        tail,
        xxh3: stream_hash,
    })
}

#[tracing::instrument(level = Level::TRACE, skip(history_tx, stream))]
async fn read_session(
    history_tx: UnboundedSender<LabeledEvent>,
    stream: S2Stream,
    client_id: u64,
    op_id: u64,
) -> eyre::Result<LabeledEvent> {
    history_tx.send(LabeledEvent {
        event: Event::Start(CallStart::Read),
        client_id,
        op_id,
    })?;

    let read_session = stream
        .read_session(
            ReadInput::new()
                // Read the entire stream from the head, so the cumulative hash
                // covers every record, not just the tail.
                .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(0)))
                // Read must include a limit, otherwise we will enter a tailing session.
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(usize::MAX))),
        )
        .await;

    let finish = match read_session {
        Ok(stream) => {
            trace!("read_session stream");
            resolve_read_tail(stream).await?
        }
        // Reading from seq_num 0 on an empty stream is reported as unwritten;
        // that is still an authoritative observation of an empty stream.
        Err(S2Error::ReadUnwritten(pos)) if pos.seq_num == 0 => {
            trace!("read_session on empty stream");
            CallFinish::ReadSuccess { tail: 0, xxh3: 0 }
        }
        Err(_e) => {
            trace!("read_session error");
            CallFinish::ReadFailure
        }
    };

    history_tx.send(LabeledEvent {
        event: Event::Finish(finish.clone()),
        client_id,
        op_id,
    })?;

    Ok(LabeledEvent {
        event: Event::Finish(finish.clone()),
        client_id,
        op_id,
    })
}

#[tracing::instrument(level = Level::TRACE, skip(history_tx, stream))]
async fn check_tail(
    history_tx: UnboundedSender<LabeledEvent>,
    stream: S2Stream,
    client_id: u64,
    op_id: u64,
) -> eyre::Result<LabeledEvent> {
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

    history_tx.send(LabeledEvent {
        event: Event::Finish(finish.clone()),
        client_id,
        op_id,
    })?;

    Ok(LabeledEvent {
        event: Event::Finish(finish.clone()),
        client_id,
        op_id,
    })
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = Level::TRACE, skip(history_tx, stream, records))]
async fn append(
    history_tx: UnboundedSender<LabeledEvent>,
    stream: S2Stream,
    records: AppendRecordBatch,
    client_id: u64,
    op_id: u64,
    match_seq_num: Option<u64>,
    fencing_token: Option<FencingToken>,
    // If this append is setting a fencing token, pass the token string here for logging
    set_fencing_token: Option<String>,
    // The xxh3 hash of each record body, in order (caller must compute since fields are private)
    record_hashes: Vec<u64>,
) -> eyre::Result<LabeledEvent> {
    assert_eq!(
        record_hashes.len(),
        records.len(),
        "one hash per record in the batch"
    );

    let start = CallStart::Append {
        num_records: records.len() as u64,
        record_hashes,
        set_fencing_token,
        fencing_token: fencing_token.as_ref().map(|t| t.to_string()),
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
        input = input.with_fencing_token(fencing_token);
    }
    let resp = stream.append(input).await;
    trace!(?resp, "append response");
    let finish = match resp {
        Ok(ack) => CallFinish::AppendSuccess {
            tail: ack.end.seq_num,
        },
        Err(e) => match &e {
            // Validation errors are definite failures
            S2Error::Validation(_) => CallFinish::AppendDefiniteFailure,
            // Append condition failures (fencing token mismatch, seq num mismatch) are definite
            S2Error::AppendConditionFailed(_) => CallFinish::AppendDefiniteFailure,
            // Server errors - check the code for definite vs indefinite
            S2Error::Server(err) => {
                match err.code.as_str() {
                    // Re: table on side-effect possibilities at <https://s2.dev/docs/api/error-codes>
                    "rate_limited" | "hot_server" | "transaction_conflict" => {
                        CallFinish::AppendDefiniteFailure
                    }
                    _ => CallFinish::AppendIndefiniteFailure,
                }
            }
            // Client errors and other errors are indefinite (might succeed on retry)
            _ => CallFinish::AppendIndefiniteFailure,
        },
    };

    match finish {
        CallFinish::AppendIndefiniteFailure => {}
        CallFinish::AppendDefiniteFailure | CallFinish::AppendSuccess { .. } => {
            history_tx.send(LabeledEvent {
                event: Event::Finish(finish.clone()),
                client_id,
                op_id,
            })?;
        }
        _ => unreachable!(),
    }

    Ok(LabeledEvent {
        event: Event::Finish(finish),
        client_id,
        op_id,
    })
}

/// Read the entire stream from the head, returning the tail and the xxh3
/// hash of every record body, in sequence order.
///
/// Returns `(0, [])` for an empty stream.
pub async fn read_all_record_hashes(stream: &S2Stream) -> eyre::Result<(u64, Vec<u64>)> {
    let read_session = stream
        .read_session(
            ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(0)))
                // Read must include a limit, otherwise we will enter a tailing session.
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(usize::MAX))),
        )
        .await;

    let mut session = match read_session {
        Ok(session) => session,
        Err(S2Error::ReadUnwritten(pos)) if pos.seq_num == 0 => return Ok((0, Vec::new())),
        Err(e) => return Err(eyre::eyre!(e)),
    };

    let mut tail = 0;
    let mut record_hashes = Vec::new();
    while let Some(resp) = session.next().await {
        let batch = resp.map_err(|e| eyre::eyre!(e))?;
        for record in &batch.records {
            record_hashes.push(xxh3_64(record.body.as_ref()));
            tail = record.seq_num + 1;
        }
    }
    Ok((tail, record_hashes))
}

/// Since the linearizability model expects a stream's tail
/// to start at 0, this fn allows us to "correct" the initial state
/// for any non-empty stream, by spoofing a successful append from 0
/// to whatever the current tail is.
pub async fn initialize_tail(
    history_tx: UnboundedSender<LabeledEvent>,
    op_id: u64,
    tail: u64,
    record_hashes: Vec<u64>,
) -> eyre::Result<()> {
    assert_eq!(
        record_hashes.len() as u64,
        tail,
        "rectifying append must cover every record from the head of the stream"
    );
    history_tx.send(LabeledEvent {
        event: Event::Start(CallStart::Append {
            num_records: tail,
            record_hashes,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// These vectors are mirrored in the Go model's tests (`TestChainHashVectors`)
    /// to guarantee both sides compute the same cumulative stream hash.
    #[test]
    fn chain_hash_vectors() {
        let h1 = chain_hash(0, xxh3_64(b"foo"));
        let h2 = chain_hash(h1, xxh3_64(b"bar"));
        let h3 = chain_hash(h2, xxh3_64(b"baz"));
        assert_eq!(xxh3_64(b"foo"), 0xab6e5f64077e7d8a);
        assert_eq!(h1, 0x4d2b003ee417c3a5);
        assert_eq!(h2, 0x132e5d5dd7936edd);
        assert_eq!(h3, 0x732ee99abc5002ff);
    }
}
