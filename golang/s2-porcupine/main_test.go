package main

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

func TestBasicNoConcurrency(t *testing.T) {
	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 1, ClientId: 0},

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
	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed unambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(5)), Xxh3: Ptr(uint64(67890))}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true, DefiniteFailure: true}, Id: 3, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 4, ClientId: 0},
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
	}
}
func TestBasicNoConcurrencyDefiniteFailure2(t *testing.T) {
	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed unambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(5)), Xxh3: Ptr(uint64(67890))}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true, DefiniteFailure: true}, Id: 3, ClientId: 0},
		// actual tail = 4

		// Read
		// this should break linearizability:
		//  - it supposes that the prior append actually did succeed, when we are told it must not have
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(9)), Xxh3: Ptr(uint64(67890))}, Id: 4, ClientId: 0},
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result == porcupine.Ok {
		t.Errorf("Expected linearizable events to fail, got result: %v", result)
	}
}
func TestBasicNoConcurrencyIndefiniteFailure1(t *testing.T) {
	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed unambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(5)), Xxh3: Ptr(uint64(67890))}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true}, Id: 3, ClientId: 0},
		// actual tail = 4, or 9

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(9)), Xxh3: Ptr(uint64(67890))}, Id: 4, ClientId: 0},
		// actual tail = 9
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
	}
}
func TestBasicNoConcurrencyIndefiniteFailure2(t *testing.T) {
	events := []porcupine.Event{
		// Append (num_records=4)
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 0, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 0, ClientId: 0},
		// actual tail = 4

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 1, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 1, ClientId: 0},
		// actual tail = 4

		// Check-Tail
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 2}, Id: 2, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4))}, Id: 2, ClientId: 0},
		// actual tail = 4

		// Append (num_records=5), failed unambiguously
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 0, NumRecords: Ptr(uint32(5)), Xxh3: Ptr(uint64(67890))}, Id: 3, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: true}, Id: 3, ClientId: 0},
		// actual tail = 4, or 9

		// Read
		{Kind: porcupine.CallEvent, Value: StreamInput{InputType: 1}, Id: 4, ClientId: 0},
		{Kind: porcupine.ReturnEvent, Value: StreamOutput{Failure: false, Tail: Ptr(uint32(4)), Xxh3: Ptr(uint64(12345))}, Id: 4, ClientId: 0},
		// actual tail = 4
	}

	model := s2Model.ToModel()
	result, _ := porcupine.CheckEventsVerbose(model, events, 0)

	if result != porcupine.Ok {
		t.Errorf("Expected linearizable events to pass, got result: %v", result)
	}
}
