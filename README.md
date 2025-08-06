# s2-verification

This repo contains a [Porcupine](https://github.com/anishathalye/porcupine) model for verifying linearizability of the core [s2.dev](https://s2.dev/) service, as well as a binary for collecting concurrent client logs which can be provided to that model.

We use this internally as a part of our custom [turmoil](https://github.com/tokio-rs/turmoil)-based [deterministic simulation testing framework](https://s2.dev/blog/dst), as well as on workloads that we run on the [Antithesis](https://antithesis.com/) platform. In these setups, we subject (simulated) S2 to a range of stresses, and assert on different invariants (maintaining linearizability being a basic one of them).

A concurrent history can also be collected against the prod S2 service, and evaluated with Porcupine.

## Installing

You will need active installations of Rust and Golang. (Also `make`.)

```bash
make build-go
```

## Running

### Create a basin

Create a S2 basin for your test streams. This can be done on the [https://s2.dev/dashboard](dashboard), or via the [CLI](https://s2.dev/docs/quickstart):
```bash
s2 create-basin linearizability-testing-0001
```

Feel free to use an existing basin if that is easier.

### Collect concurrent history

Run the `collect-history` binary. This will make a new stream with the provided name if one does not already exist.

Note that the Porcupine model assumes that the `tail` of the stream is 0 at start. The `collect-history` bin will check the current tail, before starting the concurrent clients; if it is not 0, it will synthesize (necessarily successful) append logs which represent a move from 0 -> the actual current tail, and these will be the first two entries in the resulting log. Actual concurrent client logs will begin after that.

```bash
export RUST_LOG=info
export S2_ACCESS_TOKEN="my-token"

cargo run --release -- \
  linearizability-testing-0001 \
  stream-1 \
  --num-concurrent-clients 5 \
  --num-ops-per-client 100
```

A few things to keep in mind:
- All requests within a client are made sequentially, and there are no sleeps. A client will move on to a subsequent request as soon as it receives a response from a prior one.
- Each client will randomly choose between an append, read, and check-tail operation.
- It is hard to predict how hard of a time Porcupine will have assembling a linear history. In general, the more clients, the harder it is to construct a history in a reasonable amount of time.

There are three types of client behaviors that can be selected from, using the `workflow` argument in the CLI.

#### `regular` (default)

The concurrent clients will not use any concurrency primitives. All appends are expected to succeed, unless there is a loss of availability.

#### `match-seq-num`

Concurrent clients will all maintain a local guess of what the next expected sequence number is, updated whenever `tail` info is received (from a successful append, read, or check-tail op). This value will be used for a `matchSeqNum` guard on the subsequent append made by that client.

All clients will be racing, in effect, so we expect to see many appends receiving definite failures (whenever the `matchSeqNum` condition cannot be satisfied). The more concurrent clients, the more apparent this race becomes.

#### `fencing`

Concurrent clients will attempt to set a fencing token, to a token unique to each client, whenever a client's operation counter % 100 == 0. Similar to above, this will result in a race, and we expect many definite append failures.

The `setFencingToken` op will be guarded by a `matchSeqNum` to avoid a simple last-write-win situation.

### Outputs

If all goes well, you should see something like: `writer finished, path: "./data/records.1754354415.jsonl"`. This file contains your history.

The log contains JSON objects (per line) with details about calls and returns.

The ordering expresses the relative timing of events (start and finish calls) observed by the history binary.

For example:

The start of an append, for a batch containing 5 records, which specifies a `match_seq_num` clause, is represented like:
```json
{
  "event": {
    "Start": {
      "Append": {
        "num_records": 5,
        "set_fencing_token": null,
        "fencing_token": null,
        "match_seq_num": 2733
      }
    }
  },
  "client_id": 2,
  "op_id": 4984
}
```

... and its eventual (successful) return would be represented like:
```json
{
  "event": {
    "Finish": {
      "Success": {
        "tail": 2738
      }
    }
  },
  "client_id": 2,
  "op_id": 4984
}
```

### Evaluate linearizability

Provide the log obtained above to the Porcupine model:

```bash
# if you haven't already:
make build-go

# invoke the binary with your history jsonl file
./golang/linearizability/bin/basic \
  -file="./data/records.1754354415.jsonl"
```