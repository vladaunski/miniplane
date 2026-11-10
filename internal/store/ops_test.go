package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDeleteRules(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `TRUNCATE operations, vms`); err != nil {
		t.Fatal(err)
	}
	s := New(pool)

	op, err := s.CreateVM(ctx, "acme", "vm1", "k1")
	if err != nil {
		t.Fatal(err)
	}

	// Another tenant can't see or delete acme's VM.
	if _, err := s.DeleteVM(ctx, "evil", op.VMID, "k-evil"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant delete: got %v, want ErrNotFound", err)
	}

	// Delete while the create is still pending: clean conflict.
	if _, err := s.DeleteVM(ctx, "acme", op.VMID, "k2"); !errors.Is(err, ErrOperationInFlight) {
		t.Fatalf("delete during create: got %v, want ErrOperationInFlight", err)
	}

	// Once the create finishes, the delete is accepted.
	if err := s.Complete(ctx, Work{OpID: op.ID, VMID: op.VMID, Kind: "create"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeleteVM(ctx, "acme", op.VMID, "k2"); err != nil {
		t.Fatalf("delete after create: %v", err)
	}
}
