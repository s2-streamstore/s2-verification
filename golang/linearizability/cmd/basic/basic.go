package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/anishathalye/porcupine"
)

type StartEvent struct {
	Op         string `json:"op"`
	NumRecords *int   `json:"num_records"`
}

type FinishEvent struct {
	Op              string `json:"op"`
	DefiniteFailure bool   `json:"definite_failure"`
	Failure         bool   `json:"failure"`
	Tail            int    `json:"tail"`
}

type EventWrapper struct {
	Start  *StartEvent  `json:"Start,omitempty"`
	Finish *FinishEvent `json:"Finish,omitempty"`
}

func (e *EventWrapper) UnmarshalJSON(data []byte) error {
	// Temporary map to inspect which variant is present.
	var tmp map[string]json.RawMessage
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	if v, ok := tmp["Start"]; ok {
		var se StartEvent
		if err := json.Unmarshal(v, &se); err != nil {
			return fmt.Errorf("parsing Start: %w", err)
		}
		e.Start = &se
	}
	if v, ok := tmp["Finish"]; ok {
		var fe FinishEvent
		if err := json.Unmarshal(v, &fe); err != nil {
			return fmt.Errorf("parsing Finish: %w", err)
		}
		e.Finish = &fe
	}
	// Validate that exactly one is present.
	if (e.Start != nil) == (e.Finish != nil) {
		return fmt.Errorf("expected exactly one of Start/Finish, got Start=%v Finish=%v", e.Start != nil, e.Finish != nil)
	}
	return nil
}

type Record struct {
	Event    EventWrapper `json:"event"`
	ClientID int          `json:"client_id"`
	OpID     int          `json:"op_id"`
}

type StreamState struct {
	Tail uint32
}

type StreamInput struct {
	// 0 append, 1 read, 2 check-tail
	InputType  uint8
	NumRecords uint32
}

type StreamOutput struct {
	// Failures may or may not have side effects.
	Failure bool
	// Definite failures are those which are guaranteed to not have a side-effect.
	DefiniteFailure bool
	Tail            uint32
}

var s2Model = porcupine.NondeterministicModel{
	Init: func() []interface{} {
		states := []interface{}{StreamState{0}}
		return states
	},
	Step: func(state interface{}, input interface{}, output interface{}) []interface{} {
		st := state.(StreamState)
		inp := input.(StreamInput)
		out := output.(StreamOutput)

		if inp.InputType == 0 {
			resultingState := StreamState{
				Tail: st.Tail + inp.NumRecords,
			}
			// Append
			if out.Failure && out.DefiniteFailure {
				// Did not become durable.
				return []interface{}{st}
			} else if out.Failure {
				// May or may not have become durable.
				return []interface{}{resultingState, st}
			} else {
				// Durable
				if out.Tail != resultingState.Tail {
					return []interface{}{}
				} else {
					return []interface{}{resultingState}
				}
			}

		} else if inp.InputType == 1 || inp.InputType == 2 {
			// Read or Check-Tail
			if out.Failure || st.Tail == out.Tail {
				return []interface{}{st}
			} else {
				return []interface{}{}
			}
		} else {
			panic("unknown input type")
		}
	},
	Equal: func(state1, state2 interface{}) bool {
		st1 := state1.(StreamState)
		st2 := state2.(StreamState)
		return st1.Tail == st2.Tail
	},
	DescribeOperation: func(input interface{}, output interface{}) string {
		inp := input.(StreamInput)
		out := output.(StreamOutput)

		var failureStatus string
		if out.DefiniteFailure {
			failureStatus = "failed=definite"
		} else if out.Failure {
			failureStatus = "failed=unknown"
		} else {
			failureStatus = "failed=false"
		}

		if inp.InputType == 0 {
			return fmt.Sprintf("append(%d records) -> tail(%d, %s)", inp.NumRecords, out.Tail, failureStatus)
		} else if inp.InputType == 1 {
			return fmt.Sprintf("read -> tail(%d, %s)", out.Tail, failureStatus)
		} else {
			return fmt.Sprintf("check-tail -> tail(%d, %s)", out.Tail, failureStatus)
		}
	},
	DescribeState: func(state interface{}) string {
		st := state.(StreamState)
		return fmt.Sprintf("tail(%d)", st.Tail)
	},
}

func inputFromStart(se *StartEvent) StreamInput {
	var inputType uint8
	switch se.Op {
	case "Append":
		inputType = 0
	case "Read":
		inputType = 1
	case "CheckTail":
		inputType = 2
	default:
		panic(fmt.Sprintf("unknown op: %s", se.Op))
	}
	var numRecords uint32
	if se.NumRecords != nil {
		numRecords = uint32(*se.NumRecords)
	}
	return StreamInput{
		InputType:  inputType,
		NumRecords: numRecords,
	}
}

func outputFromFinish(fe *FinishEvent) StreamOutput {
	return StreamOutput{
		Failure:         fe.Failure,
		DefiniteFailure: fe.DefiniteFailure,
		Tail:            uint32(fe.Tail),
	}
}

func main() {
	jsonHandler := slog.NewJSONHandler(os.Stderr, nil)
	log := slog.New(jsonHandler)

	filePath := flag.String("file", "", "path to JSONL records file (use '-' for stdin)")
	flag.Parse()

	if *filePath == "" {
		fmt.Fprintf(os.Stderr, "usage: %s -file=records-<epoch>.jsonl\n", os.Args[0])
		os.Exit(1)
	}

	var f *os.File
	var err error
	if *filePath == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(*filePath)
		if err != nil {
			log.Error("open file", "path", *filePath, "err", err)
			os.Exit(1)
		}
		defer f.Close()
	}

	var events []porcupine.Event
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		var r Record
		line := scanner.Bytes()
		if err := json.Unmarshal(line, &r); err != nil {
			fmt.Fprintf(os.Stderr, "failed to unmarshal line: %v\nline: %s\n", err, line)
			continue
		}

		switch {
		case r.Event.Start != nil:
			inp := inputFromStart(r.Event.Start)
			events = append(events, porcupine.Event{
				Kind:     porcupine.CallEvent,
				Value:    inp,
				Id:       r.OpID,
				ClientId: r.ClientID,
			})
		case r.Event.Finish != nil:
			out := outputFromFinish(r.Event.Finish)
			events = append(events, porcupine.Event{
				Kind:     porcupine.ReturnEvent,
				Value:    out,
				Id:       r.OpID,
				ClientId: r.ClientID,
			})
		default:
			log.Info("skipping unknown event", "op_id", r.OpID, "client_id", r.ClientID)
		}
	}

	model := s2Model.ToModel()
	res, info := porcupine.CheckEventsVerbose(model, events, 0)

	// Ensure the visualizations directory exists
	if err := os.MkdirAll("./porcupine-outputs", 0755); err != nil {
		log.Error("failed to create visualizations directory", "err", err)
	}

	// Create visualization filename based on input file
	var vizPattern string
	if *filePath == "-" {
		vizPattern = "stdin-*.html"
	} else {
		baseName := filepath.Base(*filePath)
		baseName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
		vizPattern = baseName + "-*.html"
	}

	file, err := os.CreateTemp("./porcupine-outputs", vizPattern)
	if err != nil {
		log.Error("failed to create temp file", "err", err)
	}
	err = porcupine.Visualize(model, info, file)
	if err != nil {
		log.Error("failed to visualize", "err", err)
	}
	log.Info("wrote visualization", "file", file.Name())

	if res == porcupine.Ok {
		log.Info("passed: is linearizable")
	} else {
		log.Error("failed: is NOT linearizable", "res", res)
		os.Exit(1)
	}

}
