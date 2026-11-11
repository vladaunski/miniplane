// Package store is the only code that talks to Postgres.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vladaunski/miniplane/internal/domain"
)

var (
	// ErrKeyReuse means the idempotency key was already used for a different request.
	ErrKeyReuse = errors.New("idempotency key reused with a different request")
	// ErrNameTaken means this tenant already has a VM with that name.
	ErrNameTaken = errors.New("vm name already exists for tenant")
)

// Operation is the receipt a client gets for a request: "ticket number + status".
type Operation struct {
	ID           string
	VMID         string
	Kind         string
	Status       string
	VMGeneration int64
}

// Store wraps the Postgres pool and holds all queries.
type Store struct {
	pool *pgxpool.Pool
	// Lease is how long a claim lasts without a heartbeat.
	Lease time.Duration
	// MaxAttempts is how many times an operation may be claimed before it fails for good.
	MaxAttempts int
}

// New returns a Store that uses the given connection pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, Lease: 30 * time.Second, MaxAttempts: 5}
}

// CreateVM records the intent "this VM should exist" and returns an Operation.
// It is idempotent: the same (tenant, key) always yields the same Operation.
func (s *Store) CreateVM(ctx context.Context, tenant domain.TenantID, name, key string) (Operation, error) {
	hash := requestHash(name)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Operation{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a successful Commit

	// Serialize requests with the same (tenant, key) for the rest of this transaction.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		string(tenant)+"/"+key); err != nil {
		return Operation{}, fmt.Errorf("lock key: %w", err)
	}

	// Has this key been seen before?
	var op Operation
	var storedHash string
	err = tx.QueryRow(ctx, `
		SELECT id::text, vm_id::text, kind, status, vm_generation, request_hash
		FROM operations
		WHERE tenant_id = $1 AND idempotency_key = $2`,
		string(tenant), key,
	).Scan(&op.ID, &op.VMID, &op.Kind, &op.Status, &op.VMGeneration, &storedHash)
	switch {
	case err == nil:
		if storedHash != hash {
			return Operation{}, ErrKeyReuse
		}
		return op, nil // retry of an earlier request: return the original receipt
	case !errors.Is(err, pgx.ErrNoRows):
		return Operation{}, fmt.Errorf("lookup key: %w", err)
	}

	// New request: record the desired state...
	var generation int64
	err = tx.QueryRow(ctx, `
		INSERT INTO vms (tenant_id, name, desired)
		VALUES ($1, $2, 'present')
		RETURNING id::text, generation`,
		string(tenant), name,
	).Scan(&op.VMID, &generation)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return Operation{}, ErrNameTaken
		}
		return Operation{}, fmt.Errorf("insert vm: %w", err)
	}

	// ...and the job that will make it real, in the same transaction.
	err = tx.QueryRow(ctx, `
		INSERT INTO operations (tenant_id, vm_id, kind, vm_generation, idempotency_key, request_hash)
		VALUES ($1, $2::uuid, 'create', $3, $4, $5)
		RETURNING id::text, kind, status, vm_generation`,
		string(tenant), op.VMID, generation, key, hash,
	).Scan(&op.ID, &op.Kind, &op.Status, &op.VMGeneration)
	if err != nil {
		return Operation{}, fmt.Errorf("insert operation: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Operation{}, fmt.Errorf("commit: %w", err)
	}
	return op, nil
}

func requestHash(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:])
}
