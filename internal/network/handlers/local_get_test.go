package handlers

import (
	"strings"
	"testing"
	"time"

	"minikv/internal/network/common"
	"minikv/internal/storage"
	"minikv/internal/vectorclock"
)

func newLocalGetContext() *common.CommandContext {
	store := storage.NewStore()

	return &common.CommandContext{
		Store: store,
	}
}

func TestHandleLocalGet_InvalidCommand(t *testing.T) {
	ctx := newLocalGetContext()

	result := HandleLocalGet(ctx, "LOCAL_GET")

	if result != "Usage: LOCAL_GET key" {
		t.Fatalf("expected usage message, got %q", result)
	}
}

func TestHandleLocalGet_KeyNotFound(t *testing.T) {
	ctx := newLocalGetContext()

	result := HandleLocalGet(ctx, "LOCAL_GET missing")

	if result != "LOCAL_NOT_FOUND" {
		t.Fatalf("expected LOCAL_NOT_FOUND, got %q", result)
	}
}

func TestHandleLocalGet_Success(t *testing.T) {
	ctx := newLocalGetContext()

	clock := make(vectorclock.VectorClock)
	clock["node1"] = 2

	ctx.Store.SetValue("name", storage.Value{
		Data:      "Sahil",
		CreatedAt: time.Now(),
		Clock:     clock,
	})

	result := HandleLocalGet(ctx, "LOCAL_GET name")

	if !strings.Contains(result, "Value=Sahil") {
		t.Fatalf("expected value in response, got %q", result)
	}

	if !strings.Contains(result, "Clock="+clock.Serialize()) {
		t.Fatalf("expected serialized vector clock in response")
	}

	if !strings.Contains(result, "Timestamp=") {
		t.Fatalf("expected timestamp in response")
	}
}
