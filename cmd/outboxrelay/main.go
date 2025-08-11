package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jstar/outboxrelay/internal/outbox"
)

func write(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
func hashFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func main() {
	output := flag.String("output-dir", "outputs/demo", "directory for synthetic artifacts")
	flag.Parse()
	if err := os.MkdirAll(*output, 0o755); err != nil {
		panic(err)
	}
	engine := outbox.New()
	for _, command := range []outbox.Command{{Key: "reserve-1", TaskID: "task-alpha", Action: "reserve"}, {Key: "confirm-1", TaskID: "task-alpha", Action: "confirm"}, {Key: "reserve-2", TaskID: "task-beta", Action: "reserve"}, {Key: "compensate-2", TaskID: "task-beta", Action: "compensate"}} {
		if _, _, err := engine.Apply(command); err != nil {
			panic(err)
		}
	}
	snapshotPath := filepath.Join(*output, "snapshot.json")
	if err := write(snapshotPath, engine.Snapshot()); err != nil {
		panic(err)
	}
	snapshotData, err := os.ReadFile(snapshotPath)
	if err != nil {
		panic(err)
	}
	restored, err := outbox.DecodeSnapshot(snapshotData)
	if err != nil {
		panic(err)
	}
	consumer := outbox.NewConsumer()
	if _, err := restored.RelayPending(consumer, 0); err != nil {
		panic(err)
	}
	consumerSnapshotPath := filepath.Join(*output, "consumer_snapshot.json")
	if err := write(consumerSnapshotPath, consumer.Snapshot()); err != nil {
		panic(err)
	}
	consumerSnapshotData, err := os.ReadFile(consumerSnapshotPath)
	if err != nil {
		panic(err)
	}
	restoredConsumer, err := outbox.DecodeConsumerSnapshot(consumerSnapshotData)
	if err != nil {
		panic(err)
	}
	if err := write(filepath.Join(*output, "projection.json"), map[string]any{"taskAlpha": consumer.Projection("task-alpha"), "taskBeta": consumer.Projection("task-beta"), "received": consumer.ReceivedCount()}); err != nil {
		panic(err)
	}
	report := "# OutboxRelay synthetic demo\n\n- committed events: 4\n- snapshot recovery: strict JSON decode plus ledger reconstruction\n- consumer recovery: strict dedupe-ledger decode plus projection rebuild\n- delivered events: 4\n- recovered consumer events: 4\n- task-alpha projection: task.confirmed\n- task-beta projection: task.compensated\n- delivery contract: at-least-once relay with idempotent consumer projection\n\nNo network, customer task, queue, or production broker is involved.\n"
	if restoredConsumer.ReceivedCount() != consumer.ReceivedCount() {
		panic("consumer recovery lost event ids")
	}
	if err := os.WriteFile(filepath.Join(*output, "report.md"), []byte(report), 0o644); err != nil {
		panic(err)
	}
	files := []string{"snapshot.json", "consumer_snapshot.json", "projection.json", "report.md"}
	manifest := map[string]any{"schemaVersion": 1, "files": []map[string]string{}}
	for _, name := range files {
		digest, err := hashFile(filepath.Join(*output, name))
		if err != nil {
			panic(err)
		}
		manifest["files"] = append(manifest["files"].([]map[string]string), map[string]string{"name": name, "sha256": digest})
	}
	if err := write(filepath.Join(*output, "manifest.json"), manifest); err != nil {
		panic(err)
	}
	fmt.Printf("outboxrelay demo written to %s\n", *output)
}
