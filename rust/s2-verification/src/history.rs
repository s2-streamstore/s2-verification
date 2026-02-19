use antithesis_sdk::random::AntithesisRng;
use rand::Rng;
use s2_sdk::{
    S2Stream,
    types::{
        AppendConditionFailed, AppendInput, AppendRecord, AppendRecordBatch, CommandRecord,
        FencingToken, MeteredBytes, ReadBatch, ReadFrom, ReadInput, ReadLimits, ReadStart,
        ReadStop, S2Error, Streaming,
    },
};
use serde::Serialize;
use std::sync::{Arc, atomic::AtomicU64};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::StreamExt;
use tracing::{Level, warn};
use tracing::{debug, error, trace};
use xxhash_rust::xxh3::xxh3_64;

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

pub struct GeneratedBatch {
    batch: AppendRecordBatch,
    last_xxh3: u64,
}

/// Create a batch of records containing random data.
/// Returns the batch and the xxh3 hash of the last record's body.
pub fn generate_records(num_records: usize) -> eyre::Result<GeneratedBatch> {
    let mut records = Vec::new();
    let mut batch_bytes: usize = 0;
    let mut last_xxh3: u64 = 0;
    let mut rng = AntithesisRng;

    while records.len() < num_records && batch_bytes + PER_RECORD_OVERHEAD < MAX_BATCH_BYTES {
        let record_body_budget = MAX_BATCH_BYTES - batch_bytes - PER_RECORD_OVERHEAD;

        let size = rng.gen_range(1..=record_body_budget);
        let mut body = vec![0u8; size];
        rng.fill(&mut body[..]);

        // Compute hash before creating record
        last_xxh3 = xxh3_64(&body);

        let record = AppendRecord::new(body)?;
        let metered_size = record.metered_bytes();

        batch_bytes += metered_size;
        records.push(record);
    }

    let batch = AppendRecordBatch::try_from_iter(records)?;
    Ok(GeneratedBatch { batch, last_xxh3 })
}

#[derive(Serialize, Debug, Clone)]
pub enum CallStart {
    Append {
        num_records: u64,
        last_record_xxh3: u64,
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
    AppendSuccess { tail: u64 },
    CheckTailFailure,
    CheckTailSuccess { tail: u64 },
    ReadFailure,
    ReadSuccess { tail: u64, xxh3: u64 },
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
                token_xxh3,
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
                    let GeneratedBatch { batch, last_xxh3 } =
                        generate_records(AntithesisRng.gen_range(1..1000))?;
                    let fin = append(
                        history_tx.clone(),
                        stream.clone(),
                        batch,
                        client_id,
                        op_id,
                        None,
                        Some(my_token.clone()),
                        None,
                        last_xxh3,
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
                let GeneratedBatch { batch, last_xxh3 } =
                    generate_records(AntithesisRng.gen_range(1..1000))?;
                let fin = append(
                    history_tx.clone(),
                    stream.clone(),
                    batch,
                    client_id,
                    op_id,
                    Some(expected_next_seq_num),
                    None,
                    None,
                    last_xxh3,
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
                let GeneratedBatch { batch, last_xxh3 } =
                    generate_records(AntithesisRng.gen_range(1..1000))?;
                let fin = append(
                    history_tx.clone(),
                    stream.clone(),
                    batch,
                    client_id,
                    op_id,
                    None,
                    None,
                    None,
                    last_xxh3,
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
    let mut xxh3 = 0;
    while let Some(resp) = stream.next().await {
        trace!(?resp, "read response");
        match resp {
            Ok(batch) => {
                // Check if we got a tail position (indicates end of stream)
                if let Some(tail_pos) = batch.tail
                    && batch.records.is_empty()
                {
                    // No records but we have tail - stream is caught up
                    return Ok(CallFinish::ReadSuccess {
                        tail: tail_pos.seq_num,
                        xxh3,
                    });
                }
                if let Some(last) = batch.records.last() {
                    xxh3 = xxh3_64(last.body.as_ref());
                    tail = last.seq_num + 1;
                }
            }
            Err(e) => {
                error!(?e, "read error");
                return Ok(CallFinish::ReadFailure);
            }
        }
    }
    Ok(CallFinish::ReadSuccess { tail, xxh3 })
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
                .with_start(ReadStart::new().with_from(ReadFrom::TailOffset(1)))
                // Read must include a limit, otherwise we will enter a tailing session.
                .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(usize::MAX))),
        )
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

/// When an append with `match_seq_num` receives `AppendConditionFailed::SeqNumMismatch`,
/// read back from the stream to determine if the original append actually succeeded
/// (e.g. due to a retry after a connection break).
async fn reverify_append(
    stream: &S2Stream,
    records: &AppendRecordBatch,
    match_seq_num: u64,
) -> CallFinish {
    let input = ReadInput::new()
        .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(match_seq_num)))
        .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(records.len())));

    let batch = match stream.read(input).await {
        Ok(batch) => batch,
        Err(e) => {
            debug!(
                ?e,
                match_seq_num, "reverify read failed, treating as indefinite"
            );
            return CallFinish::AppendIndefiniteFailure;
        }
    };

    debug!(
        match_seq_num,
        read_count = batch.records.len(),
        expected_count = records.len(),
        "reverifying append via read-back"
    );

    if batch.records.len() != records.len() {
        return CallFinish::AppendDefiniteFailure;
    }

    for (appended, read_back) in records.iter().zip(batch.records.iter()) {
        if appended.body() != &*read_back.body || appended.headers() != &read_back.headers[..] {
            return CallFinish::AppendDefiniteFailure;
        }
    }

    // Records match — the original append did succeed.
    let tail = batch
        .records
        .last()
        .map(|r| r.seq_num + 1)
        .unwrap_or(match_seq_num);
    CallFinish::AppendSuccess { tail }
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
    // The xxh3 hash of the last record body (caller must compute since fields are private)
    last_record_xxh3: u64,
) -> eyre::Result<LabeledEvent> {
    let xxh3 = last_record_xxh3;

    let start = CallStart::Append {
        num_records: records.len() as u64,
        last_record_xxh3: xxh3,
        set_fencing_token,
        fencing_token: fencing_token.as_ref().map(|t| t.to_string()),
        match_seq_num,
    };
    history_tx.send(LabeledEvent {
        event: Event::Start(start),
        client_id,
        op_id,
    })?;

    // When match_seq_num is set, keep a clone for potential reverification
    // in case we get AppendConditionFailed::SeqNumMismatch.
    let records_for_reverify = match_seq_num.map(|_| records.clone());

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
            // SeqNumMismatch with match_seq_num: reverify via read-back to determine
            // if the original append actually succeeded (e.g. after a retry on connection break).
            S2Error::AppendConditionFailed(AppendConditionFailed::SeqNumMismatch(_))
                if match_seq_num.is_some() =>
            {
                reverify_append(
                    &stream,
                    records_for_reverify.as_ref().expect("should not be None"),
                    match_seq_num.expect("should not be None"),
                )
                .await
            }
            // Other append condition failures (e.g. fencing token mismatch) are definite
            S2Error::AppendConditionFailed(_) => CallFinish::AppendDefiniteFailure,
            // Server errors - check the code for definite vs indefinite
            S2Error::Server(err) => {
                match err.code.as_str() {
                    // Re: table on side-effect possibilities at <https://s2.dev/docs/api/error-codes>
                    "rate_limited" | "hot_server" => CallFinish::AppendDefiniteFailure,
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

/// Since the linearizability model expects a stream's tail
/// to start at 0, this fn allows us to "correct" the initial state
/// for any non-empty stream, by spoofing a successful append from 0
/// to whatever the current tail is.
pub async fn initialize_tail(
    history_tx: UnboundedSender<LabeledEvent>,
    op_id: u64,
    tail: u64,
    xxh3: u64,
) -> eyre::Result<()> {
    history_tx.send(LabeledEvent {
        event: Event::Start(CallStart::Append {
            num_records: tail,
            last_record_xxh3: xxh3,
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
