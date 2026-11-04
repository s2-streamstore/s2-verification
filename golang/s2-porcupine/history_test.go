package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/anishathalye/porcupine"
)

func historyRecord(opID, clientID int, event string) string {
	return fmt.Sprintf(`{"event":%s,"client_id":%d,"op_id":%d}`+"\n", event, clientID, opID)
}

func TestEventsFromReaderRejectsUnpairedHistory(t *testing.T) {
	start := historyRecord(7, 1, `{"Start":"CheckTail"}`)
	finish := historyRecord(7, 1, `{"Finish":{"CheckTailSuccess":{"tail":0}}}`)
	for _, tc := range []struct{ name, input, message string }{
		{"empty", "", "no operations"},
		{"whitespace", " \n\t", "no operations"},
		{"unfinished", start, "without Finish"},
		{"orphan finish", finish, "without an open Start"},
		{"duplicate start", start + start, "duplicate Start"},
		{"reused completed id", start + finish + start + finish, "duplicate Start"},
		{"duplicate finish", start + finish + finish, "without an open Start"},
		{"finish before start", finish + start, "without an open Start"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events, err := eventsFromReader(strings.NewReader(tc.input))
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("got %v, want %q", err, tc.message)
			}
			if events != nil {
				t.Fatalf("malformed history returned %d partial events", len(events))
			}
		})
	}
}

func TestEventsFromReaderPreservesInterleavedHistory(t *testing.T) {
	input := historyRecord(7, 1, `{"Start":"CheckTail"}`) +
		historyRecord(9, 2, `{"Start":"CheckTail"}`) +
		historyRecord(9, 2, `{"Finish":{"CheckTailSuccess":{"tail":0}}}`) +
		historyRecord(7, 1, `{"Finish":{"CheckTailSuccess":{"tail":0}}}`)
	events, err := eventsFromReader(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	ids := []int{7, 9, 9, 7}
	kinds := []porcupine.EventKind{porcupine.CallEvent, porcupine.CallEvent, porcupine.ReturnEvent, porcupine.ReturnEvent}
	if len(events) != len(ids) {
		t.Fatalf("got %d events", len(events))
	}
	for i, event := range events {
		if event.Id != ids[i] || event.Kind != kinds[i] {
			t.Errorf("event %d = %+v", i, event)
		}
	}
	result, _ := porcupine.CheckEventsVerbose(s2Model.ToModel(), events, 0)
	if result != porcupine.Ok {
		t.Fatalf("valid interleaved history: %v", result)
	}
}
