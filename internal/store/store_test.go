package store

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCreateVMIdempotentUnderConcurrency(t *testing.T) {
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

	const n = 50
	ids := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op, err := s.CreateVM(ctx, "acme", "vm1", "key-1")
			ids[i], errs[i] = op.ID, err
		}()
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("request %d failed: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("request %d got operation %s, want %s", i, ids[i], ids[0])
		}
	}

	var vms, ops int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM vms`).Scan(&vms); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM operations`).Scan(&ops); err != nil {
		t.Fatal(err)
	}
	if vms != 1 || ops != 1 {
		t.Fatalf("got %d vms and %d operations, want 1 and 1", vms, ops)
	}
}
