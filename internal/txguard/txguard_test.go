package txguard

import (
	"context"
	"testing"
)

func TestInTx_NotMarked(t *testing.T) {
	if InTx(context.Background()) {
		t.Fatal("background context must not be reported as in-tx")
	}
}

func TestInTx_NilContext(t *testing.T) {
	//nolint:staticcheck // exercising the nil-guard intentionally
	if InTx(nil) {
		t.Fatal("nil context must not be reported as in-tx")
	}
}

func TestMarkInTx_Marked(t *testing.T) {
	ctx := MarkInTx(context.Background())
	if !InTx(ctx) {
		t.Fatal("marked context must be reported as in-tx")
	}
}

func TestMarkInTx_NilContext(t *testing.T) {
	//nolint:staticcheck // MarkInTx must tolerate a nil parent
	ctx := MarkInTx(nil)
	if ctx == nil {
		t.Fatal("MarkInTx must never return a nil context")
	}
	if !InTx(ctx) {
		t.Fatal("MarkInTx(nil) must produce an in-tx context")
	}
}

func TestMarkInTx_Nested(t *testing.T) {
	ctx := MarkInTx(context.Background())
	nested := MarkInTx(ctx)
	if !InTx(nested) {
		t.Fatal("nested mark must remain in-tx")
	}
}

func TestMarkInTx_DoesNotLeakToParent(t *testing.T) {
	parent := context.Background()
	_ = MarkInTx(parent)
	if InTx(parent) {
		t.Fatal("marking a child must not mutate the parent context")
	}
}

func TestMarkInTx_ChildContextStillInTx(t *testing.T) {
	ctx := MarkInTx(context.Background())
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	if !InTx(child) {
		t.Fatal("a derived context must inherit the in-tx mark")
	}
}
