package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vladaunski/miniplane/internal/domain"
)

var (
	// ErrNotFound means the VM or operation doesn't exist for this tenant.
	ErrNotFound = errors.New("not found")
	// ErrOperationInFlight means the VM already has a pending or running operation.
	ErrOperationInFlight = errors.New("another operation is already in flight for this vm")
	// ErrNoWork means there are no pending operations to claim.
	ErrNoWork = errors.New("no pending operations")
)

// Work is what the worker needs to carry out one operation.
type Work struct {
	OpID     string
	VMID     string
	TenantID string
	Kind     string // "create" or "delete"
}

// isNotFound treats "no rows" and "malformed uuid" the same way: nothing to see here.
func isNotFound(err error) bool {
	if errors.Is(err, pgx.ErrNoRows) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "22P02" // invalid_text_representation
}

// DeleteVM records the intent "this VM should not exist" and returns an Operation.
// Same shape as CreateVM: idempotent per (tenant, key), one transaction.
func (s *Store) DeleteVM(ctx context.Context, tenant domain.TenantID, vmID, key string) (Operation, error) {
	hash := requestHash("delete:" + vmID)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Operation{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		string(tenant)+"/"+key); err != nil {
		return Operation{}, fmt.Errorf("lock key: %w", err)
	}

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
		return op, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return Operation{}, fmt.Errorf("lookup key: %w", err)
	}

	// Lock the VM row so two deletes of the same VM can't interleave.
	// Scoped by tenant: another tenant's VM looks exactly like a missing one.
	var generation int64
	var phase string
	err = tx.QueryRow(ctx, `
		SELECT generation, phase FROM vms
		WHERE id = $1::uuid AND tenant_id = $2
		FOR UPDATE`,
		vmID, string(tenant),
	).Scan(&generation, &phase)
	if isNotFound(err) || (err == nil && phase == string(domain.PhaseDeleted)) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("select vm: %w", err)
	}

	// The user changed their mind: new desired state, new generation.
	if err := tx.QueryRow(ctx, `
		UPDATE vms SET desired = 'absent', generation = generation + 1
		WHERE id = $1::uuid
		RETURNING generation`, vmID,
	).Scan(&generation); err != nil {
		return Operation{}, fmt.Errorf("update vm: %w", err)
	}

	op.VMID = vmID
	err = tx.QueryRow(ctx, `
		INSERT INTO operations (tenant_id, vm_id, kind, vm_generation, idempotency_key, request_hash)
		VALUES ($1, $2::uuid, 'delete', $3, $4, $5)
		RETURNING id::text, kind, status, vm_generation`,
		string(tenant), vmID, generation, key, hash,
	).Scan(&op.ID, &op.Kind, &op.Status, &op.VMGeneration)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "one_active_op_per_vm" {
			return Operation{}, ErrOperationInFlight
		}
		return Operation{}, fmt.Errorf("insert operation: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Operation{}, fmt.Errorf("commit: %w", err)
	}
	return op, nil
}

// GetOperation returns an operation, but only if it belongs to the tenant.
func (s *Store) GetOperation(ctx context.Context, tenant domain.TenantID, id string) (Operation, error) {
	var op Operation
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, vm_id::text, kind, status, vm_generation
		FROM operations
		WHERE id = $1::uuid AND tenant_id = $2`,
		id, string(tenant),
	).Scan(&op.ID, &op.VMID, &op.Kind, &op.Status, &op.VMGeneration)
	if isNotFound(err) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("get operation: %w", err)
	}
	return op, nil
}

// ClaimNext atomically takes the oldest runnable pending operation and marks it running.
// FOR UPDATE SKIP LOCKED means a row another worker is claiming right now is skipped,
// not waited for, so concurrent workers each get a different job.
func (s *Store) ClaimNext(ctx context.Context) (Work, error) {
	var w Work
	err := s.pool.QueryRow(ctx, `
		UPDATE operations SET status = 'running'
		WHERE id = (
			SELECT id FROM operations
			WHERE status = 'pending' AND run_after <= now()
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id::text, vm_id::text, tenant_id, kind`,
	).Scan(&w.OpID, &w.VMID, &w.TenantID, &w.Kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return Work{}, ErrNoWork
	}
	if err != nil {
		return Work{}, fmt.Errorf("claim: %w", err)
	}
	return w, nil
}

// Complete marks the operation done and moves the VM to its new phase, atomically.
func (s *Store) Complete(ctx context.Context, w Work) error {
	phase := domain.PhaseRunning
	if w.Kind == "delete" {
		phase = domain.PhaseDeleted
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`UPDATE operations SET status = 'done' WHERE id = $1::uuid`, w.OpID); err != nil {
		return fmt.Errorf("mark done: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE vms SET phase = $1 WHERE id = $2::uuid`, string(phase), w.VMID); err != nil {
		return fmt.Errorf("update phase: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Fail records a failed attempt. Below maxAttempts the operation goes back to pending;
// at maxAttempts it fails for good.
func (s *Store) Fail(ctx context.Context, opID string, maxAttempts int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE operations SET
			attempts  = attempts + 1,
			status    = CASE WHEN attempts + 1 >= $2 THEN 'failed' ELSE 'pending' END,
			run_after = now() + interval '1 second' * power(2, attempts) * (0.5 + random())
		WHERE id = $1::uuid`, opID, maxAttempts)
	if err != nil {
		return fmt.Errorf("record failure: %w", err)
	}
	return nil
}
