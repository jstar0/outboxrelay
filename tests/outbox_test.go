package tests

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jstar/outboxrelay/internal/outbox"
)

func mustApply(t *testing.T, engine *outbox.Engine, command outbox.Command) outbox.Event {
	t.Helper()
	event, replayed, err := engine.Apply(command)
	if err != nil || replayed {
		t.Fatalf("apply %#v: replayed=%v err=%v", command, replayed, err)
	}
	return event
}

func TestIdempotencyReplayAndConflict(t *testing.T) {
	engine := outbox.New()
	first := mustApply(t, engine, outbox.Command{Key: "k1", TaskID: "task", Action: "reserve"})
	replayed, duplicate, err := engine.Apply(outbox.Command{Key: "k1", TaskID: "task", Action: "reserve"})
	if err != nil || !duplicate || replayed.ID != first.ID {
		t.Fatalf("replay mismatch: %#v %v %v", replayed, duplicate, err)
	}
	_, _, err = engine.Apply(outbox.Command{Key: "k1", TaskID: "task", Action: "confirm"})
	if !errors.Is(err, outbox.ErrIdempotencyConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestInvalidTransitionDoesNotWriteAnEvent(t *testing.T) {
	engine := outbox.New()
	_, _, err := engine.Apply(outbox.Command{Key: "confirm", TaskID: "task", Action: "confirm"})
	if !errors.Is(err, outbox.ErrInvalidTransition) {
		t.Fatalf("expected transition error, got %v", err)
	}
	if len(engine.Snapshot().Events) != 0 {
		t.Fatal("invalid command created an event")
	}
}

func TestRelayRetriesWithoutDuplicatingConsumerProjection(t *testing.T) {
	engine := outbox.New()
	mustApply(t, engine, outbox.Command{Key: "reserve", TaskID: "task", Action: "reserve"})
	mustApply(t, engine, outbox.Command{Key: "confirm", TaskID: "task", Action: "confirm"})
	consumer := outbox.NewConsumer()
	if _, err := engine.RelayPending(consumer, 2); err == nil {
		t.Fatal("expected injected relay failure")
	}
	if consumer.Projection("task") != "task.reserved" || consumer.ReceivedCount() != 1 {
		t.Fatalf("unexpected first projection")
	}
	attempted, err := engine.RelayPending(consumer, 0)
	if err != nil || attempted != 1 || consumer.Projection("task") != "task.confirmed" || consumer.ReceivedCount() != 2 {
		t.Fatalf("retry mismatch attempted=%d err=%v", attempted, err)
	}
	if attempted, err := engine.RelayPending(consumer, 0); err != nil || attempted != 0 {
		t.Fatalf("delivered events replayed: %d %v", attempted, err)
	}
}

func TestSnapshotRestoreRejectsTamperedLedgers(t *testing.T) {
	engine := outbox.New()
	mustApply(t, engine, outbox.Command{Key: "reserve", TaskID: "task", Action: "reserve"})
	snapshot := engine.Snapshot()
	snapshot.Events[0].Sequence = 9
	if _, err := outbox.Restore(snapshot); err == nil {
		t.Fatal("accepted noncontiguous event sequence")
	}
	snapshot = engine.Snapshot()
	snapshot.Commands["reserve"] = "unknown"
	if _, err := outbox.Restore(snapshot); err == nil {
		t.Fatal("accepted dangling idempotency digest")
	}
}

func TestSnapshotJSONRoundTripRejectsLedgerAndSchemaTampering(t *testing.T) {
	engine := outbox.New()
	mustApply(t, engine, outbox.Command{Key: "reserve", TaskID: "task", Action: "reserve"})
	mustApply(t, engine, outbox.Command{Key: "confirm", TaskID: "task", Action: "confirm"})
	data, err := json.Marshal(engine.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := outbox.DecodeSnapshot(data)
	if err != nil {
		t.Fatalf("decode valid snapshot: %v", err)
	}
	if !reflect.DeepEqual(restored.Snapshot(), engine.Snapshot()) {
		t.Fatal("round-trip changed the snapshot")
	}

	withUnknownField := strings.Replace(string(data), `"version":1`, `"version":1,"unexpected":true`, 1)
	if _, err := outbox.DecodeSnapshot([]byte(withUnknownField)); err == nil {
		t.Fatal("accepted unknown JSON field")
	}

	tampered := engine.Snapshot()
	tampered.Tasks[0].Status = "compensated"
	if _, err := outbox.Restore(tampered); err == nil {
		t.Fatal("accepted task state that disagrees with events")
	}
	tampered = engine.Snapshot()
	tampered.Events[0].Delivered, tampered.Events[0].Deliveries = true, 0
	if _, err := outbox.Restore(tampered); err == nil {
		t.Fatal("accepted delivered event without a delivery attempt")
	}
}

func TestCompensationProjectsTerminalCompensatedState(t *testing.T) {
	engine := outbox.New()
	mustApply(t, engine, outbox.Command{Key: "reserve", TaskID: "task", Action: "reserve"})
	mustApply(t, engine, outbox.Command{Key: "compensate", TaskID: "task", Action: "compensate"})
	consumer := outbox.NewConsumer()
	if _, err := engine.RelayPending(consumer, 0); err != nil {
		t.Fatal(err)
	}
	if consumer.Projection("task") != "task.compensated" {
		t.Fatalf("got %q", consumer.Projection("task"))
	}
}

func TestConsumerSnapshotSurvivesCrashBeforeDeliveryAcknowledgement(t *testing.T) {
	engine := outbox.New()
	event := mustApply(t, engine, outbox.Command{Key: "reserve", TaskID: "task", Action: "reserve"})
	consumer := outbox.NewConsumer()
	if !consumer.Consume(event) {
		t.Fatal("first consumption should apply")
	}
	data, err := json.Marshal(consumer.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := outbox.DecodeConsumerSnapshot(data)
	if err != nil {
		t.Fatalf("restore consumer: %v", err)
	}
	if restored.ReceivedCount() != 1 || restored.Projection("task") != "task.reserved" {
		t.Fatalf("unexpected restored consumer %#v", restored.Snapshot())
	}
	if attempted, err := engine.RelayPending(restored, 0); err != nil || attempted != 1 {
		t.Fatalf("relay after consumer crash attempted=%d err=%v", attempted, err)
	}
	if restored.ReceivedCount() != 1 || restored.Projection("task") != "task.reserved" {
		t.Fatal("redelivery changed an already recovered consumer projection")
	}

	tampered := consumer.Snapshot()
	tampered.Received[0].Type = "task.confirmed"
	if _, err := outbox.RestoreConsumer(tampered); err == nil {
		t.Fatal("accepted consumer transition without reserve")
	}
	withUnknownField := strings.Replace(string(data), `"version":1`, `"version":1,"unexpected":true`, 1)
	if _, err := outbox.DecodeConsumerSnapshot([]byte(withUnknownField)); err == nil {
		t.Fatal("accepted unknown consumer snapshot field")
	}
}
