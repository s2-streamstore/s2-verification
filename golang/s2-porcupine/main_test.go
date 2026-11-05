package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/zeebo/xxh3"
)

// Mirrors `chain_hash_vectors` in the Rust collector's tests, guaranteeing
// both sides compute the same cumulative stream hash.
func TestChainHashVectors(t *testing.T) {
	rFoo := xxh3.Hash([]byte("foo"))
	if rFoo != 0xab6e5f64077e7d8a {
		t.Errorf("xxh3(foo) = %#x", rFoo)
	}
	h1 := chainHash(0, rFoo)
	h2 := chainHash(h1, xxh3.Hash([]byte("bar")))
	h3 := chainHash(h2, xxh3.Hash([]byte("baz")))
	if h1 != 0x4d2b003ee417c3a5 {
		t.Errorf("h1 = %#x", h1)
	}
	if h2 != 0x132e5d5dd7936edd {
		t.Errorf("h2 = %#x", h2)
	}
	if h3 != 0x732ee99abc5002ff {
		t.Errorf("h3 = %#x", h3)
	}
}

func TestEventsFromReaderHandlesLargeRecordHashLine(t *testing.T) {
	const numRecords = 5000

	recordHashes := make([]uint64, numRecords)
	for i := range recordHashes {
		recordHashes[i] = ^uint64(0) - uint64(i)
	}

	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	start := map[string]any{
		"event": map[string]any{
			"Start": map[string]any{
				"Append": map[string]any{
					"num_records":       numRecords,
					"record_hashes":     recordHashes,
					"set_fencing_token": nil,
					"fencing_token":     nil,
					"match_seq_num":     nil,
				},
			},
		},
		"client_id": 0,
		"op_id":     0,
	}
	if err := encoder.Encode(start); err != nil {
		t.Fatalf("encode start: %v", err)
	}
	if firstLineLen := buf.Len(); firstLineLen <= 64*1024 {
		t.Fatalf("expected first line to exceed scanner default limit, got %d bytes", firstLineLen)
	}

	finish := map[string]any{
		"event": map[string]any{
			"Finish": map[string]any{
				"AppendSuccess": map[string]any{
					"tail": numRecords,
				},
			},
		},
		"client_id": 0,
		"op_id":     0,
	}
	if err := encoder.Encode(finish); err != nil {
		t.Fatalf("encode finish: %v", err)
	}

	events, err := eventsFromReader(&buf)
	if err != nil {
		t.Fatalf("eventsFromReader: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	input, ok := events[0].Value.(StreamInput)
	if !ok {
		t.Fatalf("expected StreamInput, got %T", events[0].Value)
	}
	if len(input.RecordHashes) != numRecords {
		t.Fatalf("expected %d record hashes, got %d", numRecords, len(input.RecordHashes))
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)
	if result != porcupine.Ok {
		t.Errorf("Expected oversized linearizable history to pass, got result: %v", result)
	}
}

func TestEventsFromReaderRejectsMalformedJSON(t *testing.T) {
	_, err := eventsFromReader(strings.NewReader(`{"event":{"Start":"Read"},"client_id":1,"op_id":1`))
	if err == nil {
		t.Fatal("expected malformed JSON to fail")
	}
}

func TestEventsFromReaderDecodesReadSuccessStreamHash(t *testing.T) {
	input := strings.NewReader(`{"event":{"Finish":{"ReadSuccess":{"tail":7,"stream_hash":42}}},"client_id":1,"op_id":2}`)
	events, err := eventsFromReader(input)
	if err != nil {
		t.Fatalf("eventsFromReader: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	output, ok := events[0].Value.(StreamOutput)
	if !ok {
		t.Fatalf("expected StreamOutput, got %T", events[0].Value)
	}
	if output.StreamHash == nil || *output.StreamHash != 42 {
		t.Fatalf("expected stream hash 42, got %v", output.StreamHash)
	}
}

func TestBasicNoConcurrency(t *testing.T) {
	batch := []uint64{11, 22, 33, 44}
	h := foldRecordHashes(0, batch)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(4)), RecordHashes: batch}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 0, ClientId: 0},

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4)), StreamHash: Ptr(h)}, Id: 1, ClientId: 0},

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 2, ClientId: 0},
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
	}
}

func TestBasicNoConcurrencyDefiniteFailure1(t *testing.T) {
	batch1 := []uint64{11, 22, 33, 44}
	batch2 := []uint64{55, 66, 77, 88, 99}
	h1 := foldRecordHashes(0, batch1)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(4)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4)), StreamHash: Ptr(h1)}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed unambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(5)), RecordHashes: batch2}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true, DefiniteFailure: true}, Id: 3, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4)), StreamHash: Ptr(h1)}, Id: 4, ClientId: 0},
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
	}
}
func TestBasicNoConcurrencyDefiniteFailure2(t *testing.T) {
	batch1 := []uint64{11, 22, 33, 44}
	batch2 := []uint64{55, 66, 77, 88, 99}
	h1 := foldRecordHashes(0, batch1)
	h2 := foldRecordHashes(h1, batch2)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(4)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4)), StreamHash: Ptr(h1)}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed unambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(5)), RecordHashes: batch2}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true, DefiniteFailure: true}, Id: 3, ClientId: 0},
		// actual tail = 4

		// Read
		// this should break linearizability:
		//  - it supposes that the prior append actually did succeed, when we are told it must not have
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(9)), StreamHash: Ptr(h2)}, Id: 4, ClientId: 0},
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result == porcupine.Ok {
		t.Errorf("Expected linearizable events to fail, got result: %v", result)
	}
}
func TestBasicNoConcurrencyIndefiniteFailure1(t *testing.T) {
	batch1 := []uint64{11, 22, 33, 44}
	batch2 := []uint64{55, 66, 77, 88, 99}
	h1 := foldRecordHashes(0, batch1)
	h2 := foldRecordHashes(h1, batch2)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(4)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4)), StreamHash: Ptr(h1)}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed ambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(5)), RecordHashes: batch2}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true}, Id: 3, ClientId: 0},
		// actual tail = 4, or 9

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(9)), StreamHash: Ptr(h2)}, Id: 4, ClientId: 0},
		// actual tail = 9
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
	}
}
func TestBasicNoConcurrencyIndefiniteFailure2(t *testing.T) {
	batch1 := []uint64{11, 22, 33, 44}
	batch2 := []uint64{55, 66, 77, 88, 99}
	h1 := foldRecordHashes(0, batch1)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(4)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4)), StreamHash: Ptr(h1)}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed ambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(5)), RecordHashes: batch2}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true}, Id: 3, ClientId: 0},
		// actual tail = 4, or 9

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(4)), StreamHash: Ptr(h1)}, Id: 4, ClientId: 0},
		// actual tail = 4
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
	}
}

// Sequence numbers above 2^32 must be represented exactly: a match_seq_num
// that collides with the true tail modulo 2^32 must still be rejected.
func TestLargeSeqNumsNotTruncated(t *testing.T) {
	const bigTail = uint64(1)<<32 + 5
	batch1 := []uint64{11}
	batch2 := []uint64{22}

	events := []porcupine.Event{
		// Append a batch bringing the tail past 2^32.
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(bigTail), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(bigTail)}, Id: 0, ClientId: 0},

		// match_seq_num=5 equals the tail mod 2^32 but not the true tail;
		// a successful append here must not be linearizable.
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(1)), RecordHashes: batch2, MatchSeqNum: Ptr(uint64(5))}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(bigTail + 1)}, Id: 1, ClientId: 0},
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)
	if result == porcupine.Ok {
		t.Errorf("Expected truncation-colliding match_seq_num append to fail, got result: %v", result)
	}

	// Sanity check: the same history with the correct match_seq_num passes.
	events[2].Value = StreamInput{InputType: 0, NumRecords: Ptr(uint64(1)), RecordHashes: batch2, MatchSeqNum: Ptr(bigTail)}
	result, _ = porcupine.CheckEventsVerbose(model, events, 0)
	if result != porcupine.Ok {
		t.Errorf("Expected matching large match_seq_num append to pass, got result: %v", result)
	}
}

// A read whose tail and final record agree with the model, but whose earlier
// records differ from what was acknowledged, must be flagged. The previous
// model (which tracked only the hash of the last record) could not catch
// this; the cumulative stream hash commits to every record from the head.
func TestReadDetectsCorruptedPrefix(t *testing.T) {
	batch1 := []uint64{11, 22}
	batch2 := []uint64{33}
	corrupted := []uint64{98, 99} // a different first batch...
	hCorrupt := foldRecordHashes(foldRecordHashes(0, corrupted), batch2)

	events := []porcupine.Event{
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(2)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(2))}, Id: 0, ClientId: 0},

		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(1)), RecordHashes: batch2}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(3))}, Id: 1, ClientId: 0},

		// Read observes the right tail, and ends with the right last record
		// (batch2), but the stream prefix does not match what was acknowledged.
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(3)), StreamHash: Ptr(hCorrupt)}, Id: 2, ClientId: 0},
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result == porcupine.Ok {
		t.Errorf("Expected corrupted-prefix read to fail linearizability, got result: %v", result)
	}
}

// Sanity-check the happy-path version of the above: the read reporting the
// true cumulative hash over both batches passes.
func TestReadVerifiesWholeStream(t *testing.T) {
	batch1 := []uint64{11, 22}
	batch2 := []uint64{33}
	h := foldRecordHashes(foldRecordHashes(0, batch1), batch2)

	events := []porcupine.Event{
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(2)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(2))}, Id: 0, ClientId: 0},

		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint64(1)), RecordHashes: batch2}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(3))}, Id: 1, ClientId: 0},

		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint64(3)), StreamHash: Ptr(h)}, Id: 2, ClientId: 0},
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
	}
}

type historyBuilder struct {
	lines []string
}

func (b *historyBuilder) add(t *testing.T, clientID, opID int, event any) {
	t.Helper()
	line, err := json.Marshal(map[string]any{"event": event, "client_id": clientID, "op_id": opID})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	b.lines = append(b.lines, string(line))
}

func (b *historyBuilder) records(t *testing.T) []Record {
	t.Helper()
	records, err := recordsFromReader(strings.NewReader(strings.Join(b.lines, "\n")))
	if err != nil {
		t.Fatalf("recordsFromReader: %v", err)
	}
	return records
}

func appendStart(recordHashes []uint64, setFencingToken, fencingToken *string) any {
	return map[string]any{"Start": map[string]any{"Append": map[string]any{
		"num_records":       len(recordHashes),
		"record_hashes":     recordHashes,
		"set_fencing_token": setFencingToken,
		"fencing_token":     fencingToken,
		"match_seq_num":     nil,
	}}}
}

func appendSuccess(tail uint64) any {
	return map[string]any{"Finish": map[string]any{"AppendSuccess": map[string]any{"tail": tail}}}
}

const appendIndefinite = `AppendIndefiniteFailure`

func readStart() any {
	return map[string]any{"Start": "Read"}
}

func readSuccess(recordHashes []uint64) any {
	return map[string]any{"Finish": map[string]any{"ReadSuccess": map[string]any{
		"tail":        len(recordHashes),
		"stream_hash": foldRecordHashes(0, recordHashes),
	}}}
}

func finalReadSuccess(recordHashes []uint64) any {
	return map[string]any{"Finish": map[string]any{"ReadSuccess": map[string]any{
		"tail":          len(recordHashes),
		"stream_hash":   foldRecordHashes(0, recordHashes),
		"record_hashes": recordHashes,
	}}}
}

func indefinite() any {
	return map[string]any{"Finish": appendIndefinite}
}

func checkRecords(t *testing.T, records []Record) porcupine.CheckResult {
	t.Helper()
	result, _ := porcupine.CheckEventsVerbose(s2Model.ToModel(), eventsFromRecords(records), 10*time.Second)
	return result
}

func opFinish(records []Record, opID int) *FinishEvent {
	for _, rec := range records {
		if rec.OpID == opID && rec.Event.Finish != nil {
			return rec.Event.Finish
		}
	}
	return nil
}

func TestResolveIndefiniteAppends(t *testing.T) {
	// Client 1's append succeeds. Clients 2 and 3 each append indefinitely, and
	// only client 2's record reaches the stream.
	var h historyBuilder
	h.add(t, 1, 0, appendStart([]uint64{1, 2}, nil, nil))
	h.add(t, 2, 1, appendStart([]uint64{3}, nil, nil))
	h.add(t, 3, 2, appendStart([]uint64{4, 5}, nil, nil))
	h.add(t, 1, 0, appendSuccess(2))
	h.add(t, 2, 1, indefinite())
	h.add(t, 3, 2, indefinite())
	h.add(t, 4, 3, readStart())
	h.add(t, 4, 3, finalReadSuccess([]uint64{1, 2, 3}))

	records, res := resolveIndefiniteAppends(h.records(t))
	if res != (Resolution{Applied: 1, NotApplied: 1}) {
		t.Fatalf("unexpected resolution: %+v", res)
	}
	if fin := opFinish(records, 1); fin == nil || fin.AppendSuccess == nil || fin.AppendSuccess.Tail != 3 {
		t.Fatalf("expected op 1 to resolve to a success with tail 3, got %+v", fin)
	}
	for _, rec := range records {
		if rec.OpID == 2 {
			t.Fatalf("expected op 2 to be dropped, found %+v", rec)
		}
	}
	if result := checkRecords(t, records); result != porcupine.Ok {
		t.Fatalf("expected linearizable, got %v", result)
	}
}

func TestResolveLeavesSharedRecordsIndefinite(t *testing.T) {
	// Both appends carry the same record, as repeated fence attempts with one
	// token do, so the final read cannot tell which of them it observed.
	var h historyBuilder
	h.add(t, 1, 0, appendStart([]uint64{7}, nil, nil))
	h.add(t, 1, 0, appendSuccess(1))
	h.add(t, 2, 1, appendStart([]uint64{7}, nil, nil))
	h.add(t, 2, 1, indefinite())
	h.add(t, 3, 2, readStart())
	h.add(t, 3, 2, finalReadSuccess([]uint64{7}))

	records, res := resolveIndefiniteAppends(h.records(t))
	if res != (Resolution{Unresolved: 1}) {
		t.Fatalf("unexpected resolution: %+v", res)
	}
	if fin := opFinish(records, 1); fin == nil || !fin.AppendIndefiniteFailure {
		t.Fatalf("expected op 1 to stay indefinite, got %+v", fin)
	}
	if result := checkRecords(t, records); result != porcupine.Ok {
		t.Fatalf("expected linearizable, got %v", result)
	}
}

func TestResolveDropsAbsentAppendWithSharedRecords(t *testing.T) {
	// Record 7 also belongs to another append, but this append's record 8 is
	// not on the stream, so it cannot have taken effect.
	var h historyBuilder
	h.add(t, 1, 0, appendStart([]uint64{7}, nil, nil))
	h.add(t, 1, 0, appendSuccess(1))
	h.add(t, 2, 1, appendStart([]uint64{8, 7}, nil, nil))
	h.add(t, 2, 1, indefinite())
	h.add(t, 3, 2, readStart())
	h.add(t, 3, 2, finalReadSuccess([]uint64{7}))

	records, res := resolveIndefiniteAppends(h.records(t))
	if res != (Resolution{NotApplied: 1}) {
		t.Fatalf("unexpected resolution: %+v", res)
	}
	if result := checkRecords(t, records); result != porcupine.Ok {
		t.Fatalf("expected linearizable, got %v", result)
	}
}

func TestResolvePartialAppendStillFailsCheck(t *testing.T) {
	// Only part of the batch reached the stream. The append is dropped, which
	// leaves record 3 on the final read unexplained, so the check must fail.
	var h historyBuilder
	h.add(t, 1, 0, appendStart([]uint64{3, 4}, nil, nil))
	h.add(t, 1, 0, indefinite())
	h.add(t, 2, 1, readStart())
	h.add(t, 2, 1, finalReadSuccess([]uint64{3}))

	records, res := resolveIndefiniteAppends(h.records(t))
	if res != (Resolution{NotApplied: 1}) {
		t.Fatalf("unexpected resolution: %+v", res)
	}
	if result := checkRecords(t, records); result != porcupine.Illegal {
		t.Fatalf("expected not linearizable, got %v", result)
	}
}

func TestResolvedAppendIsStillChecked(t *testing.T) {
	// The stream is fenced with token "a", so an append carrying token "b"
	// must not take effect. The final read shows that it did.
	a, b := "a", "b"
	var h historyBuilder
	h.add(t, 1, 0, appendStart([]uint64{100}, &a, nil))
	h.add(t, 1, 0, appendSuccess(1))
	h.add(t, 2, 1, appendStart([]uint64{200}, nil, &b))
	h.add(t, 2, 1, indefinite())
	h.add(t, 3, 2, readStart())
	h.add(t, 3, 2, finalReadSuccess([]uint64{100, 200}))

	records, res := resolveIndefiniteAppends(h.records(t))
	if res != (Resolution{Applied: 1}) {
		t.Fatalf("unexpected resolution: %+v", res)
	}
	if result := checkRecords(t, records); result != porcupine.Illegal {
		t.Fatalf("expected not linearizable, got %v", result)
	}
}

func TestResolveSkipsFinalReadThatOverlapsAnotherOp(t *testing.T) {
	// Still a valid history: the read is checked like any other.
	var h historyBuilder
	h.add(t, 1, 0, appendStart([]uint64{1}, nil, nil))
	h.add(t, 2, 1, readStart())
	h.add(t, 1, 0, indefinite())
	h.add(t, 2, 1, finalReadSuccess([]uint64{1}))

	input := h.records(t)
	records, res := resolveIndefiniteAppends(input)
	if res.Skipped == "" || res.Unresolved != 1 || len(records) != len(input) {
		t.Fatalf("expected resolution to be skipped, got %+v with %d records", res, len(records))
	}
	if result := checkRecords(t, records); result != porcupine.Ok {
		t.Fatalf("expected linearizable, got %v", result)
	}
}

func TestResolveSkipsInconsistentFinalRead(t *testing.T) {
	// The read's record hashes don't match its tail, so they can't be trusted
	// to settle anything. The model then judges the read itself.
	var h historyBuilder
	h.add(t, 1, 0, appendStart([]uint64{1}, nil, nil))
	h.add(t, 1, 0, indefinite())
	h.add(t, 2, 1, readStart())
	h.add(t, 2, 1, map[string]any{"Finish": map[string]any{"ReadSuccess": map[string]any{
		"tail":          2,
		"stream_hash":   foldRecordHashes(0, []uint64{1}),
		"record_hashes": []uint64{1},
	}}})

	input := h.records(t)
	records, res := resolveIndefiniteAppends(input)
	if res.Skipped == "" || len(records) != len(input) {
		t.Fatalf("expected resolution to be skipped, got %+v with %d records", res, len(records))
	}
	if result := checkRecords(t, records); result != porcupine.Illegal {
		t.Fatalf("expected not linearizable, got %v", result)
	}
}

func TestResolveWithoutFinalReadLeavesHistoryUnchanged(t *testing.T) {
	var h historyBuilder
	h.add(t, 1, 0, appendStart([]uint64{1}, nil, nil))
	h.add(t, 1, 0, indefinite())
	h.add(t, 2, 1, readStart())
	h.add(t, 2, 1, readSuccess([]uint64{1}))

	input := h.records(t)
	records, res := resolveIndefiniteAppends(input)
	if res != (Resolution{Unresolved: 1}) || len(records) != len(input) {
		t.Fatalf("expected the history unchanged, got %+v with %d records", res, len(records))
	}
}

// Many indefinite appends overlapping many reads: each read pins which of them
// have taken effect so far. Without resolution the checker does not finish this
// history within 20s even at 12 appends.
func TestResolveKeepsManyIndefiniteAppendsTractable(t *testing.T) {
	const n = 24
	var h historyBuilder
	var stream []uint64
	for i := 0; i < n; i++ {
		h.add(t, i+1, i, appendStart([]uint64{uint64(1000 + i)}, nil, nil))
	}
	for i := 0; i < n; i += 2 {
		stream = append(stream, uint64(1000+i))
	}
	for j := 0; j <= len(stream); j++ {
		opID := n + j
		h.add(t, 100, opID, readStart())
		h.add(t, 100, opID, readSuccess(stream[:j]))
	}
	for i := 0; i < n; i++ {
		h.add(t, i+1, i, indefinite())
	}
	h.add(t, 101, 2*n+1, readStart())
	h.add(t, 101, 2*n+1, finalReadSuccess(stream))

	records, res := resolveIndefiniteAppends(h.records(t))
	if res != (Resolution{Applied: n / 2, NotApplied: n / 2}) {
		t.Fatalf("unexpected resolution: %+v", res)
	}
	if result := checkRecords(t, records); result != porcupine.Ok {
		t.Fatalf("expected linearizable, got %v", result)
	}
}
