// Package txguard marks a context as running inside a bbolt write
// transaction so that outbound I/O boundaries can fail fast rather than
// silently holding the single global write lock across a network round-trip.
//
// bbolt is single-writer: db.Update takes a global write lock for the whole
// duration of the callback. Performing blocking I/O (an HTTP call to Jellyfin,
// for example) inside that callback stalls every other writer until the I/O
// completes. This package gives us a cheap, import-cycle-free way to detect
// that situation: WithTx-style wrappers mark the tx-scoped context, and the
// outbound clients check InTx at entry.
package txguard

import "context"

type inTxKey struct{}

// MarkInTx returns a child context flagged as running inside a write
// transaction. It is idempotent: marking an already-marked context returns an
// equivalent context.
func MarkInTx(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if InTx(ctx) {
		return ctx
	}
	return context.WithValue(ctx, inTxKey{}, true)
}

// InTx reports whether ctx was marked by MarkInTx, i.e. whether the current
// call is executing inside a bbolt write transaction.
func InTx(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, ok := ctx.Value(inTxKey{}).(bool)
	return ok && v
}
