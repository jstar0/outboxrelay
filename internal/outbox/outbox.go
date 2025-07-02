package outbox

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key was already used with a different command")
	ErrInvalidTransition   = errors.New("command is invalid for the task state")
)

type Command struct {
	Key    string `json:"key"`
	TaskID string `json:"taskId"`
	Action string `json:"action"`
}

type Event struct {
	Sequence    int    `json:"sequence"`
	ID          string `json:"id"`
	TaskID      string `json:"taskId"`
	Type        string `json:"type"`
	Delivered   bool   `json:"delivered"`
	Deliveries  int    `json:"deliveries"`
	PayloadHash string `json:"payloadHash"`
}

type Task struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type Snapshot struct {
	Version  int               `json:"version"`
	Tasks    []Task            `json:"tasks"`
	Events   []Event           `json:"events"`
	Commands map[string]string `json:"commands"`
}

// ConsumerSnapshot is the minimal durable state an idempotent consumer needs:
// the event identities it already applied. The projection is rebuilt from those
// identities rather than trusted as a second mutable ledger.
type ConsumerSnapshot struct {
	Version  int             `json:"version"`
	Received []ConsumedEvent `json:"received"`
}

type ConsumedEvent struct {
	Sequence int    `json:"sequence"`
	ID       string `json:"id"`
	TaskID   string `json:"taskId"`
	Type     string `json:"type"`
}

type Engine struct {
	tasks    map[string]Task
	events   []Event
	commands map[string]string
}

func New() *Engine {
	return &Engine{tasks: map[string]Task{}, commands: map[string]string{}}
}

func commandDigest(command Command) string {
	sum := sha256.Sum256([]byte(command.TaskID + "\x1f" + command.Action))
	return hex.EncodeToString(sum[:])
}

func eventType(action string) (string, string, error) {
	switch action {
	case "reserve":
		return "reserved", "task.reserved", nil
	case "confirm":
		return "confirmed", "task.confirmed", nil
	case "compensate":
		return "compensated", "task.compensated", nil
	default:
		return "", "", fmt.Errorf("unsupported action %q", action)
	}
}

func allowed(current, action string) bool {
	switch action {
	case "reserve":
		return current == ""
	case "confirm", "compensate":
		return current == "reserved"
	default:
		return false
	}
}

// Apply commits a task transition and its outbox event as one in-memory unit.
// The caller can snapshot immediately after this method and recover before any
// relay delivery, which models the transactional-outbox crash boundary.
func (e *Engine) Apply(command Command) (Event, bool, error) {
	if command.Key == "" || command.TaskID == "" {
		return Event{}, false, errors.New("command key and task id are required")
	}
	digest := commandDigest(command)
	if prior, exists := e.commands[command.Key]; exists {
		if prior != digest {
			return Event{}, false, ErrIdempotencyConflict
		}
		for _, event := range e.events {
			if event.PayloadHash == digest {
				return event, true, nil
			}
		}
		return Event{}, false, errors.New("idempotency record has no matching event")
	}
	status, typ, err := eventType(command.Action)
	if err != nil {
		return Event{}, false, err
	}
	current := e.tasks[command.TaskID]
	if !allowed(current.Status, command.Action) {
		return Event{}, false, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, current.Status, command.Action)
	}
	e.tasks[command.TaskID] = Task{ID: command.TaskID, Status: status}
	event := Event{
		Sequence: len(e.events) + 1, ID: fmt.Sprintf("evt-%04d", len(e.events)+1),
		TaskID: command.TaskID, Type: typ, PayloadHash: digest,
	}
	e.events = append(e.events, event)
	e.commands[command.Key] = digest
	return event, false, nil
}

func (e *Engine) Pending() []Event {
	pending := make([]Event, 0)
	for _, event := range e.events {
		if !event.Delivered {
			pending = append(pending, event)
		}
	}
	return pending
}

func (e *Engine) Snapshot() Snapshot {
	tasks := make([]Task, 0, len(e.tasks))
	for _, task := range e.tasks {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	events := append([]Event(nil), e.events...)
	commands := make(map[string]string, len(e.commands))
	for key, value := range e.commands {
		commands[key] = value
	}
	return Snapshot{Version: 1, Tasks: tasks, Events: events, Commands: commands}
}

func Restore(snapshot Snapshot) (*Engine, error) {
	if snapshot.Version != 1 {
		return nil, errors.New("unsupported snapshot version")
	}
	engine := New()
	seenTask := map[string]bool{}
	seenEvent := map[string]bool{}
	payloads := map[string]bool{}
	expectedTaskStatus := map[string]string{}
	for _, task := range snapshot.Tasks {
		if task.ID == "" || !validTaskStatus(task.Status) || seenTask[task.ID] {
			return nil, errors.New("invalid task snapshot")
		}
		seenTask[task.ID] = true
		engine.tasks[task.ID] = task
	}
	for index, event := range snapshot.Events {
		if event.Sequence != index+1 || event.ID != fmt.Sprintf("evt-%04d", index+1) || event.TaskID == "" || !validEventDigest(event.PayloadHash) || event.Deliveries < 0 || (event.Delivered && event.Deliveries == 0) || seenEvent[event.ID] || payloads[event.PayloadHash] {
			return nil, errors.New("invalid event ledger")
		}
		if _, exists := engine.tasks[event.TaskID]; !exists {
			return nil, errors.New("event references unknown task")
		}
		next, ok := eventTransition(expectedTaskStatus[event.TaskID], event.Type)
		if !ok {
			return nil, errors.New("invalid task event transition")
		}
		expectedTaskStatus[event.TaskID] = next
		seenEvent[event.ID], payloads[event.PayloadHash] = true, true
		engine.events = append(engine.events, event)
	}
	if len(snapshot.Commands) != len(snapshot.Events) {
		return nil, errors.New("invalid idempotency ledger")
	}
	seenCommandDigest := map[string]bool{}
	for key, digest := range snapshot.Commands {
		if key == "" || !payloads[digest] || seenCommandDigest[digest] {
			return nil, errors.New("invalid idempotency ledger")
		}
		seenCommandDigest[digest] = true
		engine.commands[key] = digest
	}
	for _, task := range snapshot.Tasks {
		if expectedTaskStatus[task.ID] != task.Status {
			return nil, errors.New("task snapshot does not match event ledger")
		}
	}
	return engine, nil
}

func validTaskStatus(status string) bool {
	return status == "reserved" || status == "confirmed" || status == "compensated"
}

func eventTransition(current, typ string) (string, bool) {
	switch typ {
	case "task.reserved":
		return "reserved", current == ""
	case "task.confirmed":
		return "confirmed", current == "reserved"
	case "task.compensated":
		return "compensated", current == "reserved"
	default:
		return "", false
	}
}

func validEventDigest(digest string) bool {
	if len(digest) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(digest)
	return err == nil && hex.EncodeToString(decoded) == digest
}

// DecodeSnapshot accepts exactly one schema-known JSON snapshot, then applies
// the same ledger checks as Restore. It is deliberately not an authenticity
// check: a signed or encrypted recovery format is outside this local reference.
func DecodeSnapshot(data []byte) (*Engine, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var snapshot Snapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode snapshot: trailing data")
	}
	return Restore(snapshot)
}

type Consumer struct {
	received   map[string]ConsumedEvent
	projection map[string]string
}

func NewConsumer() *Consumer {
	return &Consumer{received: map[string]ConsumedEvent{}, projection: map[string]string{}}
}

// Consume is idempotent: a relay may retry an event after crashing before its
// delivery acknowledgement, but the projection applies each event ID once.
func (c *Consumer) Consume(event Event) bool {
	if _, exists := c.received[event.ID]; exists {
		return false
	}
	received := ConsumedEvent{Sequence: event.Sequence, ID: event.ID, TaskID: event.TaskID, Type: event.Type}
	c.received[event.ID] = received
	current, exists := c.projection[event.TaskID]
	if !exists || received.Sequence > c.lastSequence(event.TaskID, current) {
		c.projection[event.TaskID] = event.Type
	}
	return true
}
func (c *Consumer) Projection(taskID string) string { return c.projection[taskID] }
func (c *Consumer) ReceivedCount() int              { return len(c.received) }

func (c *Consumer) lastSequence(taskID, typ string) int {
	for _, event := range c.received {
		if event.TaskID == taskID && event.Type == typ {
			return event.Sequence
		}
	}
	return 0
}

func (c *Consumer) Snapshot() ConsumerSnapshot {
	received := make([]ConsumedEvent, 0, len(c.received))
	for _, event := range c.received {
		received = append(received, event)
	}
	sort.Slice(received, func(i, j int) bool { return received[i].Sequence < received[j].Sequence })
	return ConsumerSnapshot{Version: 1, Received: received}
}

func RestoreConsumer(snapshot ConsumerSnapshot) (*Consumer, error) {
	if snapshot.Version != 1 {
		return nil, errors.New("unsupported consumer snapshot version")
	}
	consumer := NewConsumer()
	seenID := map[string]bool{}
	lastState := map[string]string{}
	lastSequence := 0
	for _, received := range snapshot.Received {
		if received.Sequence <= lastSequence || received.ID != fmt.Sprintf("evt-%04d", received.Sequence) || received.TaskID == "" || seenID[received.ID] {
			return nil, errors.New("invalid consumer event ledger")
		}
		next, ok := eventTransition(lastState[received.TaskID], received.Type)
		if !ok {
			return nil, errors.New("invalid consumer event transition")
		}
		lastSequence = received.Sequence
		lastState[received.TaskID] = next
		seenID[received.ID] = true
		consumer.Consume(Event{Sequence: received.Sequence, ID: received.ID, TaskID: received.TaskID, Type: received.Type})
	}
	return consumer, nil
}

func DecodeConsumerSnapshot(data []byte) (*Consumer, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var snapshot ConsumerSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode consumer snapshot: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode consumer snapshot: trailing data")
	}
	return RestoreConsumer(snapshot)
}

// RelayPending attempts every pending event in sequence. failSequence simulates
// a poison or unavailable consumer: that event stays pending for a later retry.
func (e *Engine) RelayPending(consumer *Consumer, failSequence int) (attempted int, err error) {
	if consumer == nil {
		return 0, errors.New("consumer is required")
	}
	for index := range e.events {
		event := &e.events[index]
		if event.Delivered {
			continue
		}
		event.Deliveries++
		attempted++
		if event.Sequence == failSequence {
			return attempted, fmt.Errorf("delivery failed for %s", event.ID)
		}
		consumer.Consume(*event)
		event.Delivered = true
	}
	return attempted, nil
}
