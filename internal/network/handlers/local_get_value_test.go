package handlers

import (
	"encoding/json"
	"testing"
	"time"

	"minikv/internal/network/common"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

func newLocalGetValueContext() *common.CommandContext {
	store := storage.NewStore()

	return &common.CommandContext{
		Store: store,
	}
}

func TestHandleLocalGetValue_InvalidCommand(t *testing.T) {
	ctx := newLocalGetValueContext()

	result := HandleLocalGetValue(ctx, "LOCAL_GET_VALUE")

	if result != "Usage: LOCAL_GET_VALUE key" {
		t.Fatalf("expected usage message, got %q", result)
	}
}

func TestHandleLocalGetValue_KeyNotFound(t *testing.T) {
	ctx := newLocalGetValueContext()

	result := HandleLocalGetValue(ctx, "LOCAL_GET_VALUE missing")

	if result != "NOT_FOUND" {
		t.Fatalf("expected NOT_FOUND, got %q", result)
	}
}

func TestHandleLocalGetValue_Success(t *testing.T) {
	ctx := newLocalGetValueContext()

	clock := make(vectorclock.VectorClock)
	clock["node1"] = 5

	value := storage.Value{
		Data:      "MiniKV",
		CreatedAt: time.Now().UTC().Truncate(time.Second),
		Clock:     clock,
	}

	ctx.Store.SetValue("project", value)

	result := HandleLocalGetValue(ctx, "LOCAL_GET_VALUE project")

	var decoded storage.Value

	if err := json.Unmarshal([]byte(result), &decoded); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if decoded.Data != value.Data {
		t.Fatalf("expected data %q, got %q", value.Data, decoded.Data)
	}

	if !decoded.CreatedAt.Equal(value.CreatedAt) {
		t.Fatalf("expected timestamp %v, got %v", value.CreatedAt, decoded.CreatedAt)
	}

	if decoded.Clock.Serialize() != value.Clock.Serialize() {
		t.Fatalf(
			"expected clock %q, got %q",
			value.Clock.Serialize(),
			decoded.Clock.Serialize(),
		)
	}
}
