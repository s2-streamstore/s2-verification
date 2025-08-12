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

type AppendArgs struct {
	NumRecords       int     `json:"num_records"`
	LastRecordCrc32  uint32  `json:"last_record_crc32"`
	SetFencingToken  *string `json:"set_fencing_token"`
	FencingToken     *string `json:"fencing_token"`
	MatchSeqNum      *int    `json:"match_seq_num"`
}

type StartEvent struct {
	Append    *AppendArgs `json:"-"`
	CheckTail bool        `json:"-"`
	Read      bool        `json:"-"`
}

func (se *StartEvent) UnmarshalJSON(data []byte) error {
	// First try to unmarshal as a string (for CheckTail and Read)
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		switch str {
		case "CheckTail":
			se.CheckTail = true
			return nil
		case "Read":
			se.Read = true
			return nil
		default:
			return fmt.Errorf("unknown string start event: %s", str)
		}
	}

	// Try to unmarshal as an object (for Append)
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}

	if appendData, ok := obj["Append"]; ok {
		var args AppendArgs
		if err := json.Unmarshal(appendData, &args); err != nil {
			return fmt.Errorf("parsing Append args: %w", err)
		}
		se.Append = &args
		return nil
	}

	return fmt.Errorf("unknown start event format")
}

type AppendSuccessResult struct {
	Tail int `json:"tail"`
}

type ReadSuccessResult struct {
	Tail  int    `json:"tail"`
	Crc32 uint32 `json:"crc32"`
}

type CheckTailSuccessResult struct {
	Tail int `json:"tail"`
}

type FinishEvent struct {
	// Append results
	AppendSuccess           *AppendSuccessResult `json:"-"`
	AppendDefiniteFailure   bool                 `json:"-"`
	AppendIndefiniteFailure bool                 `json:"-"`
	
	// Read results
	ReadSuccess *ReadSuccessResult `json:"-"`
	ReadFailure bool               `json:"-"`
	
	// CheckTail results
	CheckTailSuccess *CheckTailSuccessResult `json:"-"`
	CheckTailFailure bool                    `json:"-"`
}

func (fe *FinishEvent) UnmarshalJSON(data []byte) error {
	// First try to unmarshal as a string (for failure events)
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		switch str {
		case "AppendDefiniteFailure":
			fe.AppendDefiniteFailure = true
			return nil
		case "AppendIndefiniteFailure":
			fe.AppendIndefiniteFailure = true
			return nil
		case "ReadFailure":
			fe.ReadFailure = true
			return nil
		case "CheckTailFailure":
			fe.CheckTailFailure = true
			return nil
		default:
			return fmt.Errorf("unknown string finish event: %s", str)
		}
	}

	// Try to unmarshal as an object (for success events)
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(data, &obj); err != nil {
		return err
	}

	if successData, ok := obj["AppendSuccess"]; ok {
		var result AppendSuccessResult
		if err := json.Unmarshal(successData, &result); err != nil {
			return fmt.Errorf("parsing AppendSuccess result: %w", err)
		}
		fe.AppendSuccess = &result
		return nil
	}

	if successData, ok := obj["ReadSuccess"]; ok {
		var result ReadSuccessResult
		if err := json.Unmarshal(successData, &result); err != nil {
			return fmt.Errorf("parsing ReadSuccess result: %w", err)
		}
		fe.ReadSuccess = &result
		return nil
	}

	if successData, ok := obj["CheckTailSuccess"]; ok {
		var result CheckTailSuccessResult
		if err := json.Unmarshal(successData, &result); err != nil {
			return fmt.Errorf("parsing CheckTailSuccess result: %w", err)
		}
		fe.CheckTailSuccess = &result
		return nil
	}

	return fmt.Errorf("unknown finish event format")
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
	Tail         uint32
	FencingToken *string
}

type StreamInput struct {
	// 0 append, 1 read, 2 check-tail
	InputType         uint8
	SetFencingToken   *string
	BatchFencingToken *string
	MatchSeqNum       *uint32
	NumRecords        *uint32
}

type StreamOutput struct {
	// Failures may or may not have side effects.
	Failure bool
	// Definite failures are those which are guaranteed to not have a side-effect.
	DefiniteFailure bool
	Tail            *uint32
}

var s2Model = porcupine.NondeterministicModel{
	Init: func() []interface{} {
		states := []interface{}{
			StreamState{
				Tail:         0,
				FencingToken: nil,
			},
		}
		return states
	},
	Step: func(state interface{}, input interface{}, output interface{}) []interface{} {
		startingState := state.(StreamState)
		inp := input.(StreamInput)
		out := output.(StreamOutput)

		if inp.InputType == 0 {
			// Append
			// TODO also make sure a set token is a single record batch
			var optimisticToken *string
			if inp.SetFencingToken != nil {
				optimisticToken = inp.SetFencingToken
			} else {
				optimisticToken = startingState.FencingToken
			}
			optimisticState := StreamState{
				Tail:         startingState.Tail + *inp.NumRecords,
				FencingToken: optimisticToken,
			}
			if out.Failure && out.DefiniteFailure {
				// Did not become durable.
				return []interface{}{startingState}
			} else if out.Failure {
				if inp.BatchFencingToken != nil && startingState.FencingToken != nil && *inp.BatchFencingToken != *startingState.FencingToken {
					// A fencing token was supplied, but did not match the current token on the stream.
					// This should not have become durable.
					return []interface{}{startingState}
				}
				if inp.MatchSeqNum != nil && *inp.MatchSeqNum != startingState.Tail {
					// A matchSeqNum position was supplied, but did not match the current tail of the stream.
					// This should not have become durable.
					return []interface{}{startingState}
				}
				// May or may not have become durable. Can't say!
				return []interface{}{optimisticState, startingState}
			} else {
				// Durable
				if inp.BatchFencingToken != nil {
					// Illegal
					if startingState.FencingToken == nil || *startingState.FencingToken != *inp.BatchFencingToken {
						return []interface{}{}
					}
				}
				if inp.MatchSeqNum != nil && *inp.MatchSeqNum != startingState.Tail {
					// Illegal
					return []interface{}{}
				}
				if *out.Tail != optimisticState.Tail {
					return []interface{}{}
				} else {
					return []interface{}{optimisticState}
				}
			}

		} else if inp.InputType == 1 || inp.InputType == 2 {
			// Read or Check-Tail
			if out.Failure || startingState.Tail == *out.Tail {
				return []interface{}{startingState}
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
		return st1.Tail == st2.Tail && st1.FencingToken == st2.FencingToken
	},
	DescribeOperation: func(input interface{}, output interface{}) string {
		inp := input.(StreamInput)
		out := output.(StreamOutput)

		if inp.InputType == 0 {
			return formatAppendCall(inp, out)
		} else if inp.InputType == 1 {
			return formatReadCall(inp, out)
		} else {
			return formatCheckTailCall(inp, out)
		}
	},
	DescribeState: func(state interface{}) string {
		st := state.(StreamState)
		if st.FencingToken == nil {
			return fmt.Sprintf("tail[%d]", st.Tail)
		} else {
			return fmt.Sprintf("tail[%d],token[%s]", st.Tail, *st.FencingToken)
		}
	},
}

func formatAppendCall(inp StreamInput, out StreamOutput) string {
	var failureStatus string = "none"
	if out.DefiniteFailure {
		failureStatus = "definite"
	} else if out.Failure {
		failureStatus = "indefinite"
	}
	var setToken string
	if inp.SetFencingToken != nil {
		setToken = fmt.Sprintf(", set_token[%s]", *inp.SetFencingToken)
	} else {
		setToken = ""
	}
	var batchToken string
	if inp.BatchFencingToken != nil {
		batchToken = fmt.Sprintf(", batch_token[%s]", *inp.BatchFencingToken)
	} else {
		batchToken = ""
	}
	var matchSeqNum string
	if inp.MatchSeqNum != nil {
		matchSeqNum = fmt.Sprintf(", match_seq_num[%d]", *inp.MatchSeqNum)
	} else {
		matchSeqNum = ""
	}

	inRepr := fmt.Sprintf("append(len[%d]%s%s%s)", *inp.NumRecords, setToken, batchToken, matchSeqNum)

	var outRepr string
	if out.Failure {
		outRepr = fmt.Sprintf("FAILED[%s]", failureStatus)
	} else {
		outRepr = fmt.Sprintf("tail[%d]", *out.Tail)
	}

	return fmt.Sprintf("%s -> %s", inRepr, outRepr)

}

func formatReadCall(inp StreamInput, out StreamOutput) string {
	if out.Failure {
		return fmt.Sprintf("read() -> failed")
	} else {
		return fmt.Sprintf("read() -> tail[%d]", *out.Tail)
	}
}

func formatCheckTailCall(inp StreamInput, out StreamOutput) string {
	if out.Failure {
		return fmt.Sprintf("check_tail() -> failed")
	} else {
		return fmt.Sprintf("check_tail() -> tail[%d]", *out.Tail)
	}
}

func inputFromStart(se *StartEvent) StreamInput {
	var inputType uint8
	var numRecords *uint32
	var setFencingToken *string
	var batchFencingToken *string
	var matchSeqNum *uint32

	switch {
	case se.Append != nil:
		inputType = 0
		num := uint32(se.Append.NumRecords)
		numRecords = &num
		setFencingToken = se.Append.SetFencingToken
		batchFencingToken = se.Append.FencingToken
		if se.Append.MatchSeqNum != nil {
			seq := uint32(*se.Append.MatchSeqNum)
			matchSeqNum = &seq
		}
	case se.Read:
		inputType = 1
	case se.CheckTail:
		inputType = 2
	default:
		panic("unknown start event type")
	}

	return StreamInput{
		InputType:         inputType,
		SetFencingToken:   setFencingToken,
		BatchFencingToken: batchFencingToken,
		MatchSeqNum:       matchSeqNum,
		NumRecords:        numRecords,
	}
}

func outputFromFinish(fe *FinishEvent) StreamOutput {
	switch {
	// Append results
	case fe.AppendSuccess != nil:
		return StreamOutput{
			Failure:         false,
			DefiniteFailure: false,
			Tail:            Ptr(uint32(fe.AppendSuccess.Tail)),
		}
	case fe.AppendDefiniteFailure:
		return StreamOutput{
			Failure:         true,
			DefiniteFailure: true,
			Tail:            nil,
		}
	case fe.AppendIndefiniteFailure:
		return StreamOutput{
			Failure:         true,
			DefiniteFailure: false,
			Tail:            nil,
		}
	// Read results
	case fe.ReadSuccess != nil:
		return StreamOutput{
			Failure:         false,
			DefiniteFailure: false,
			Tail:            Ptr(uint32(fe.ReadSuccess.Tail)),
		}
	case fe.ReadFailure:
		return StreamOutput{
			Failure:         true,
			DefiniteFailure: true,
			Tail:            nil,
		}
	// CheckTail results
	case fe.CheckTailSuccess != nil:
		return StreamOutput{
			Failure:         false,
			DefiniteFailure: false,
			Tail:            Ptr(uint32(fe.CheckTailSuccess.Tail)),
		}
	case fe.CheckTailFailure:
		return StreamOutput{
			Failure:         true,
			DefiniteFailure: true,
			Tail:            nil,
		}
	default:
		panic("unknown finish event type")
	}
}

func Ptr[T any](v T) *T {
	return &v
}

// Version is set at build time via ldflags
var Version = "dev"

func main() {
	jsonHandler := slog.NewJSONHandler(os.Stderr, nil)
	log := slog.New(jsonHandler)

	filePath := flag.String("file", "", "path to JSONL records file (use '-' for stdin)")
	version := flag.Bool("version", false, "show version information")
	flag.Parse()

	if *version {
		fmt.Printf("s2-porcupine version %s\n", Version)
		os.Exit(0)
	}

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
