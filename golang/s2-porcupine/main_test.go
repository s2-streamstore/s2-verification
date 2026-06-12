package main

import (
	"testing"

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

func TestBasicNoConcurrency(t *testing.T) {
	batch := []uint64{11, 22, 33, 44}
	h := foldRecordHashes(0, batch, nil)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), RecordHashes: batch}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), StreamHash: Ptr(h)}, Id: 1, ClientId: 0},

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 2, ClientId: 0},
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
	h1 := foldRecordHashes(0, batch1, nil)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), StreamHash: Ptr(h1)}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed unambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(5)), RecordHashes: batch2}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true, DefiniteFailure: true}, Id: 3, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), StreamHash: Ptr(h1)}, Id: 4, ClientId: 0},
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
	h1 := foldRecordHashes(0, batch1, nil)
	h2 := foldRecordHashes(h1, batch2, nil)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), StreamHash: Ptr(h1)}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed unambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(5)), RecordHashes: batch2}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true, DefiniteFailure: true}, Id: 3, ClientId: 0},
		// actual tail = 4

		// Read
		// this should break linearizability:
		//  - it supposes that the prior append actually did succeed, when we are told it must not have
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(9)), StreamHash: Ptr(h2)}, Id: 4, ClientId: 0},
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
	h1 := foldRecordHashes(0, batch1, nil)
	h2 := foldRecordHashes(h1, batch2, nil)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), StreamHash: Ptr(h1)}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed ambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(5)), RecordHashes: batch2}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true}, Id: 3, ClientId: 0},
		// actual tail = 4, or 9

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(9)), StreamHash: Ptr(h2)}, Id: 4, ClientId: 0},
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
	h1 := foldRecordHashes(0, batch1, nil)

	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), StreamHash: Ptr(h1)}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed ambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(5)), RecordHashes: batch2}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true}, Id: 3, ClientId: 0},
		// actual tail = 4, or 9

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), StreamHash: Ptr(h1)}, Id: 4, ClientId: 0},
		// actual tail = 4
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
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
	hCorrupt := foldRecordHashes(foldRecordHashes(0, corrupted, nil), batch2, nil)

	events := []porcupine.Event{
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(2)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(2))}, Id: 0, ClientId: 0},

		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(1)), RecordHashes: batch2}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(3))}, Id: 1, ClientId: 0},

		// Read observes the right tail, and ends with the right last record
		// (batch2), but the stream prefix does not match what was acknowledged.
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(3)), StreamHash: Ptr(hCorrupt)}, Id: 2, ClientId: 0},
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
	h := foldRecordHashes(foldRecordHashes(0, batch1, nil), batch2, nil)

	events := []porcupine.Event{
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(2)), RecordHashes: batch1}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(2))}, Id: 0, ClientId: 0},

		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(1)), RecordHashes: batch2}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(3))}, Id: 1, ClientId: 0},

		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(3)), StreamHash: Ptr(h)}, Id: 2, ClientId: 0},
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
	}
}
