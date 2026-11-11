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
	finish(t, s)

	if _, err := s.DeleteVM(ctx, "acme", op.VMID, "k2"); err != nil {
		t.Fatalf("delete after create: %v", err)
	}
}

func TestRepairRecreatesMissingVM(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	op, err := s.CreateVM(ctx, "acme", "vm1", "k1")
	if err != nil {
		t.Fatal(err)
	}
	finish(t, s)

	ok, err := s.Repair(ctx, op.VMID, "create")
	if err != nil || !ok {
		t.Fatalf("Repair = %v, %v; want true, nil", ok, err)
	}
	var phase string
	var pending int
	if err := pool.QueryRow(ctx, `SELECT phase FROM vms`).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM operations WHERE status = 'pending'`).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if phase != "PENDING" || pending != 1 {
		t.Fatalf("phase=%s pending=%d, want PENDING and 1", phase, pending)
	}

	// The VM is no longer RUNNING, so a second repair has nothing to do.
	if ok, err := s.Repair(ctx, op.VMID, "create"); err != nil || ok {
		t.Fatalf("second Repair = %v, %v; want false, nil", ok, err)
	}
}

// finish claims the next runnable job and completes it, as a worker would.
func finish(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	w, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, w); err != nil {
		t.Fatal(err)
	}
}
