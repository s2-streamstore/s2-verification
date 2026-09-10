use antithesis_sdk::random::AntithesisRng;
use rand::Rng;
use s2_sdk::{
    S2Stream,
    error::{ReadError, ReadSessionError},
    types::{
        AppendInput, AppendRecord, AppendRecordBatch, CommandRecord, FencingToken, MeteredBytes,
        ReadBatch, ReadFrom, ReadInput, ReadLimits, ReadSessionConfig, ReadStart, ReadStop,
    },
};
use serde::Serialize;
use std::ops::Range;
use std::sync::{Arc, atomic::AtomicU64};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::{Stream, StreamExt};
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
        stream_hash: u64,
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

/// An append whose outcome the client could not determine.
///
/// Its `Start` event has already been recorded in the history; its `Finish` is withheld and
/// emitted from the [`AppendLedger`] once every client has stopped issuing operations.
#[derive(Debug, Clone)]
pub struct DeferredAppend {
    finish: LabeledEvent,
    record_hashes: Vec<u64>,
    match_seq_num: Option<u64>,
}

/// What the clients learned about their appends: which ones are still undecided, and where the
/// ones known to have succeeded landed.
///
/// Each client returns its own ledger; [`AppendLedger::merge`] combines them. Once every client
/// has finished, the deferred finish events are emitted in one of two ways:
/// - [`AppendLedger::into_indefinite`]: every deferred append finishes as an
///   `AppendIndefiniteFailure`, without touching the system again. The checker must then
///   consider each one as possibly durable at any point until the end of the history, at a cost
///   exponential in their number.
/// - [`AppendLedger::resolve`]: the stream is read back once to learn what actually happened to
///   each deferred append. This is the only interaction with the system after the clients have
///   finished, and it only reads; if it fails, fall back to `into_indefinite`.
#[derive(Debug, Clone, Default)]
pub struct AppendLedger {
    /// Appends whose outcome is unknown.
    pub deferred: Vec<DeferredAppend>,
    /// Sequence number ranges occupied by appends known to have succeeded.
    pub landed: Vec<Range<u64>>,
}

impl AppendLedger {
    pub fn merge(&mut self, other: Self) {
        self.deferred.extend(other.deferred);
        self.landed.extend(other.landed);
    }

    /// An `AppendIndefiniteFailure` finish event per deferred append.
    pub fn into_indefinite(self) -> Vec<LabeledEvent> {
        self.deferred.into_iter().map(|d| d.finish).collect()
    }

    /// Read the whole stream and decide each deferred append against its contents, as described
    /// in [`AppendLedger::resolve_against`].
    ///
    /// Must only be called once every client has finished, so that no operation in the history
    /// can have observed a batch which becomes durable after this read, and only on the merged
    /// ledger of all of them, so that every successful append is accounted for.
    pub async fn resolve(&self, stream: &S2Stream) -> eyre::Result<Vec<LabeledEvent>> {
        if self.deferred.is_empty() {
            return Ok(Vec::new());
        }
        let (tail, record_hashes) = read_all_record_hashes(stream).await?;
        Ok(self.resolve_against(tail, &record_hashes))
    }

    /// Decide each deferred append against the record hashes observed on the stream.
    ///
    /// `tail` is the stream's tail and `stream_hashes` the hash of every record up to it, so
    /// `stream_hashes[i]` is the record at sequence number `tail - stream_hashes.len() + i`.
    ///
    /// Each deferred append is searched for as a contiguous run of record hashes at positions
    /// not occupied by a `landed` append, and at its `match_seq_num` if it had one:
    /// - found at exactly one position, which no other deferred append could also occupy: the
    ///   append became durable there, so it finishes as `AppendSuccess { tail }`, which pins it
    ///   to a single linearization point;
    /// - not found, and the stream has not been trimmed: nothing in the history can have
    ///   observed it, since no client is still running, so it finishes as
    ///   `AppendDefiniteFailure`. Should the request still commit afterwards, that is outside
    ///   the recorded history;
    /// - otherwise it is ambiguous and left as an `AppendIndefiniteFailure`.
    ///
    /// Both resolved outcomes are one of the two the checker would have had to consider for the
    /// indefinite one, so resolving can never make a non-linearizable history pass.
    pub fn resolve_against(&self, tail: u64, stream_hashes: &[u64]) -> Vec<LabeledEvent> {
        let first_seq_num = tail - stream_hashes.len() as u64;
        let mut free = vec![true; stream_hashes.len()];
        for seq_num in self.landed.iter().flat_map(Clone::clone) {
            if let Some(idx) = seq_num.checked_sub(first_seq_num)
                && let Some(slot) = free.get_mut(idx as usize)
            {
                *slot = false;
            }
        }

        // Indices at which each deferred append may have landed.
        let candidates: Vec<Vec<usize>> = self
            .deferred
            .iter()
            .map(|d| {
                let n = d.record_hashes.len();
                let matches_at = |idx: &usize| {
                    stream_hashes.get(*idx..*idx + n) == Some(d.record_hashes.as_slice())
                        && free[*idx..*idx + n].iter().all(|f| *f)
                };
                match d.match_seq_num {
                    Some(seq_num) => seq_num
                        .checked_sub(first_seq_num)
                        .map(|idx| idx as usize)
                        .filter(matches_at)
                        .into_iter()
                        .collect(),
                    None => (0..stream_hashes.len()).filter(matches_at).collect(),
                }
            })
            .collect();

        // Several deferred appends may match overlapping positions (e.g. repeated fence commands
        // with identical bodies), in which case those positions cannot be attributed to any of
        // them.
        let mut claims = vec![0usize; stream_hashes.len()];
        for (d, candidates) in self.deferred.iter().zip(&candidates) {
            for idx in candidates {
                for claim in &mut claims[*idx..*idx + d.record_hashes.len()] {
                    *claim += 1;
                }
            }
        }

        self.deferred
            .iter()
            .zip(candidates)
            .map(|(d, candidates)| {
                let n = d.record_hashes.len();
                let mut finish = d.finish.clone();
                match candidates.as_slice() {
                    [] if first_seq_num == 0 => {
                        debug!(
                            finish.client_id,
                            finish.op_id, "deferred append never became durable"
                        );
                        finish.event = Event::Finish(CallFinish::AppendDefiniteFailure);
                    }
                    [idx] if claims[*idx..*idx + n].iter().all(|c| *c == 1) => {
                        let tail = first_seq_num + (*idx + n) as u64;
                        debug!(
                            finish.client_id,
                            finish.op_id, tail, "deferred append became durable"
                        );
                        finish.event = Event::Finish(CallFinish::AppendSuccess { tail });
                    }
                    _ => {
                        warn!(
                            finish.client_id,
                            finish.op_id,
                            num_candidates = candidates.len(),
                            "deferred append cannot be located on the stream, leaving indefinite"
                        );
                    }
                }
                finish
            })
            .collect()
    }
}

fn random_op() -> Op {
    match AntithesisRng.gen_range(0..3) {
        0 => Op::Append,
        1 => Op::Read,
        2 => Op::CheckTail,
        _ => unreachable!(),
    }
}

/// Handle an indefinite failure by attempting to rotate to a new client ID, as the old one can
/// no longer be used. The finish event itself has been deferred by [`append`].
///
/// Returns `Some(new_client_id)` if a new client ID was successfully acquired,
/// or `None` if the maximum number of client IDs has been reached (caller should break).
async fn handle_indefinite_failure(client_id_atomic: &Arc<AtomicU64>) -> Option<u64> {
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
/// Returns the ledger of appends, including the deferred ones whose finish was not
/// communicated via `history_tx`.
pub async fn fencing_token_client(
    num_ops: usize,
    stream: S2Stream,
    client_id_atomic: Arc<AtomicU64>,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<AppendLedger> {
    const ATTEMPT_TO_SET_FENCE_TOKEN_EVERY: usize = 100;

    let mut client_id = client_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let my_token = FencingToken::generate(6)?;
    debug!(?my_token);
    let mut ledger = AppendLedger::default();
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
                &mut ledger,
            )
            .await?;
            match fin.event {
                Event::Finish(CallFinish::AppendDefiniteFailure) => {}
                Event::Finish(CallFinish::AppendIndefiniteFailure) => {
                    if let Some(new_client_id) = handle_indefinite_failure(&client_id_atomic).await
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
                        &mut ledger,
                    )
                    .await?;
                    if let Event::Finish(CallFinish::AppendIndefiniteFailure) = fin.event {
                        if let Some(new_client_id) =
                            handle_indefinite_failure(&client_id_atomic).await
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

    Ok(ledger)
}

/// Run a client that randomly selects between ops.
///
/// When append operations are attempted, this client will specify a `match_seq_num` value,
/// based on the most recent guess from a prior call.
///
/// Returns the ledger of appends, including the deferred ones whose finish was not
/// communicated via `history_tx`.
pub async fn match_seq_num_client(
    num_ops: usize,
    stream: S2Stream,
    client_id_atomic: Arc<AtomicU64>,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<AppendLedger> {
    let mut client_id = client_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut ledger = AppendLedger::default();
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
                    &mut ledger,
                )
                .await?;
                if let Event::Finish(CallFinish::AppendIndefiniteFailure) = fin.event {
                    if let Some(new_client_id) = handle_indefinite_failure(&client_id_atomic).await
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

    Ok(ledger)
}

/// Run a client that randomly selects between ops.
///
/// Appends are not gated by `match_seq_num` or fencing token. Fewer definite failures
/// are expected, compared to the `match_seq_num_client` and `fencing_token_client`.
///
/// Returns the ledger of appends, including the deferred ones whose finish was not
/// communicated via `history_tx`.
pub async fn client(
    num_ops: usize,
    stream: S2Stream,
    client_id_atomic: Arc<AtomicU64>,
    op_id_atomic: Arc<AtomicU64>,
    history_tx: UnboundedSender<LabeledEvent>,
) -> eyre::Result<AppendLedger> {
    let mut client_id = client_id_atomic.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut ledger = AppendLedger::default();
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
                    &mut ledger,
                )
                .await?;
                if let Event::Finish(CallFinish::AppendIndefiniteFailure) = fin.event {
                    if let Some(new_client_id) = handle_indefinite_failure(&client_id_atomic).await
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

    Ok(ledger)
}

#[tracing::instrument(level = Level::TRACE, skip_all)]
async fn resolve_read_tail(
    mut stream: impl Stream<Item = Result<ReadBatch, ReadSessionError>> + Unpin,
) -> eyre::Result<CallFinish> {
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
    Ok(CallFinish::ReadSuccess { tail, stream_hash })
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
            ReadSessionConfig::default(),
        )
        .await;

    let finish = match read_session {
        Ok(stream) => {
            trace!("read_session stream");
            resolve_read_tail(stream).await?
        }
        // Reading from seq_num 0 on an empty stream is reported as unwritten;
        // that is still an authoritative observation of an empty stream.
        Err(ReadSessionError::Read(ReadError::ReadUnwritten(pos))) if pos.seq_num == 0 => {
            trace!("read_session on empty stream");
            CallFinish::ReadSuccess {
                tail: 0,
                stream_hash: 0,
            }
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
    ledger: &mut AppendLedger,
) -> eyre::Result<LabeledEvent> {
    assert_eq!(
        record_hashes.len(),
        records.len(),
        "one hash per record in the batch"
    );

    let start = CallStart::Append {
        num_records: records.len() as u64,
        record_hashes: record_hashes.clone(),
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
        Ok(ack) => {
            ledger.landed.push(ack.start.seq_num..ack.end.seq_num);
            CallFinish::AppendSuccess {
                tail: ack.end.seq_num,
            }
        }
        Err(e) if e.has_no_side_effects() => CallFinish::AppendDefiniteFailure,
        Err(_) => CallFinish::AppendIndefiniteFailure,
    };
    let fin = LabeledEvent {
        event: Event::Finish(finish.clone()),
        client_id,
        op_id,
    };

    match finish {
        CallFinish::AppendIndefiniteFailure => {
            ledger.deferred.push(DeferredAppend {
                finish: fin.clone(),
                record_hashes,
                match_seq_num,
            });
        }
        CallFinish::AppendDefiniteFailure | CallFinish::AppendSuccess { .. } => {
            history_tx.send(fin.clone())?;
        }
        _ => unreachable!(),
    }

    Ok(fin)
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
            ReadSessionConfig::default(),
        )
        .await;

    let mut session = match read_session {
        Ok(session) => session,
        Err(ReadSessionError::Read(ReadError::ReadUnwritten(pos))) if pos.seq_num == 0 => {
            return Ok((0, Vec::new()));
        }
        Err(e) => return Err(eyre::eyre!(e)),
    };

    let mut tail = 0;
    let mut record_hashes = Vec::new();
    while let Some(resp) = session.next().await {
        match resp {
            Ok(batch) => {
                for record in &batch.records {
                    record_hashes.push(xxh3_64(record.body.as_ref()));
                    tail = record.seq_num + 1;
                }
            }
            // A session that resumes after an interruption picks up at the next seq_num, which
            // is reported as unwritten once every record has been read.
            Err(ReadSessionError::Read(ReadError::ReadUnwritten(pos))) if pos.seq_num == tail => {
                break;
            }
            Err(e) => return Err(eyre::eyre!(e)),
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

    fn deferred(op_id: u64, record_hashes: Vec<u64>, match_seq_num: Option<u64>) -> DeferredAppend {
        DeferredAppend {
            finish: LabeledEvent {
                event: Event::Finish(CallFinish::AppendIndefiniteFailure),
                client_id: 1,
                op_id,
            },
            record_hashes,
            match_seq_num,
        }
    }

    fn outcomes(resolved: Vec<LabeledEvent>) -> Vec<(u64, Event)> {
        resolved
            .into_iter()
            .map(|fin| (fin.op_id, fin.event))
            .collect()
    }

    #[test]
    fn resolve_deferred_appends_against_stream() {
        let ledger = AppendLedger {
            deferred: vec![
                // Unique match.
                deferred(0, vec![12, 13], None),
                // Not on the stream.
                deferred(1, vec![99], None),
                // Also matches at 4, but that position is occupied by a successful append.
                deferred(2, vec![10, 11], None),
                // match_seq_num pins where the batch can be, even though it occurs elsewhere.
                deferred(3, vec![15], Some(8)),
                deferred(4, vec![15], Some(6)),
                // Two deferred appends with identical records contend for the same position.
                deferred(5, vec![14], None),
                deferred(6, vec![14], None),
            ],
            // Records 4..=5 came from an append that is known to have succeeded.
            landed: std::iter::once(4..6).collect(),
        };
        let resolved = ledger.resolve_against(9, &[10, 11, 12, 13, 10, 11, 14, 15, 15]);
        assert!(matches!(
            outcomes(resolved).as_slice(),
            [
                (0, Event::Finish(CallFinish::AppendSuccess { tail: 4 })),
                (1, Event::Finish(CallFinish::AppendDefiniteFailure)),
                (2, Event::Finish(CallFinish::AppendSuccess { tail: 2 })),
                (3, Event::Finish(CallFinish::AppendSuccess { tail: 9 })),
                (4, Event::Finish(CallFinish::AppendDefiniteFailure)),
                (5, Event::Finish(CallFinish::AppendIndefiniteFailure)),
                (6, Event::Finish(CallFinish::AppendIndefiniteFailure)),
            ]
        ));
    }

    #[test]
    fn resolve_deferred_appends_against_trimmed_stream() {
        let ledger = AppendLedger {
            deferred: vec![
                deferred(0, vec![14, 15], None),
                // Could have been trimmed away, so cannot be called a definite failure.
                deferred(1, vec![99], None),
                deferred(2, vec![99], Some(1)),
            ],
            landed: vec![],
        };
        // Records 0..=2 were trimmed; records 3..=5 remain.
        let resolved = ledger.resolve_against(6, &[13, 14, 15]);
        assert!(matches!(
            outcomes(resolved).as_slice(),
            [
                (0, Event::Finish(CallFinish::AppendSuccess { tail: 6 })),
                (1, Event::Finish(CallFinish::AppendIndefiniteFailure)),
                (2, Event::Finish(CallFinish::AppendIndefiniteFailure)),
            ]
        ));
    }

    #[test]
    fn read_success_serializes_stream_hash() {
        let json = serde_json::to_string(&CallFinish::ReadSuccess {
            tail: 7,
            stream_hash: 42,
        })
        .expect("serialize read success");
        assert_eq!(json, r#"{"ReadSuccess":{"tail":7,"stream_hash":42}}"#);
    }
}
