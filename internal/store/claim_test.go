package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), `TRUNCATE operations, vms`); err != nil {
		t.Fatal(err)
	}
	return New(pool), pool
}

func TestClaimNextNeverHandsOutAJobTwice(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()

	const jobs = 100
	for i := 0; i < jobs; i++ {
		if _, err := s.CreateVM(ctx, "acme", fmt.Sprintf("vm%d", i), fmt.Sprintf("k%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 10; w++ { // ten competing workers
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				work, err := s.ClaimNext(ctx)
				if errors.Is(err, ErrNoWork) {
					var pending int
					if err := pool.QueryRow(ctx,
						`SELECT count(*) FROM operations WHERE status = 'pending'`,
					).Scan(&pending); err != nil {
						t.Error(err)
						return
					}
					if pending == 0 {
						return
					}
					continue // lost a race, but work remains: try again
				}
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				claimed[work.OpID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	rows, err := pool.Query(ctx, `SELECT status, count(*) FROM operations GROUP BY status`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			t.Fatal(err)
		}
		t.Logf("status %s: %d", st, n)
	}

	if len(claimed) != jobs {
		t.Errorf("claimed %d distinct jobs, want %d", len(claimed), jobs)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Errorf("operation %s was claimed %d times", id, n)
		}
	}
}

func TestFailRetriesWithBackoffThenGivesUp(t *testing.T) {
	s, pool := testStore(t)
	ctx := context.Background()
	if _, err := s.CreateVM(ctx, "acme", "vm1", "k1"); err != nil {
		t.Fatal(err)
	}

	work, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, work.OpID, 2); err != nil {
		t.Fatal(err)
	}
	// First failure: back to pending, but not runnable yet (backoff).
	if _, err := s.ClaimNext(ctx); !errors.Is(err, ErrNoWork) {
		t.Fatalf("job claimable during backoff: %v", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE operations SET run_after = now()`); err != nil {
		t.Fatal(err)
	}
	work, err = s.ClaimNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, work.OpID, 2); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM operations`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("status = %q after max attempts, want failed", status)
	}
}

func TestAbandonedJobIsReclaimedAfterLeaseExpires(t *testing.T) {
	s, _ := testStore(t)
	s.Lease = 200 * time.Millisecond
	ctx := context.Background()
	if _, err := s.CreateVM(ctx, "acme", "vm1", "k1"); err != nil {
		t.Fatal(err)
	}
	first, err := s.ClaimNext(ctx) // this worker "dies" without finishing
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimNext(ctx); !errors.Is(err, ErrNoWork) {
		t.Fatalf("job claimed during a valid lease: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	second, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatalf("abandoned job was not reclaimed: %v", err)
	}
	if second.OpID != first.OpID {
		t.Fatalf("reclaimed %s, want %s", second.OpID, first.OpID)
	}
}

func TestHeartbeatKeepsTheLease(t *testing.T) {
	s, _ := testStore(t)
	s.Lease = 300 * time.Millisecond
	ctx := context.Background()
	if _, err := s.CreateVM(ctx, "acme", "vm1", "k1"); err != nil {
		t.Fatal(err)
	}
	work, err := s.ClaimNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		time.Sleep(100 * time.Millisecond)
		if err := s.Heartbeat(ctx, work.OpID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ClaimNext(ctx); !errors.Is(err, ErrNoWork) {
		t.Fatalf("job stolen from a live worker: %v", err)
	}
}
