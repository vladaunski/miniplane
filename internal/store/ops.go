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
	OpID       string
	VMID       string
	TenantID   string
	Kind       string // "create" or "delete"
	Attempt    int    // fencing token: which claim this worker holds
	Generation int64
}

// VM is a snapshot of the VM row, for the reconciler.
type VM struct {
	ID         string
	Name       string
	Desired    string
	Generation int64
	Phase      string
}

// isNotFound treats "no rows" and "malformed uuid" the same way: nothing to see here.
func isNotFound(err error) bool {
	if errors.Is(err, pgx.ErrNoRows) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "22P02" // invalid_text_representation
}

// GetVM returns a VM, but only if it belongs to the tenant.
func (s *Store) GetVM(ctx context.Context, tenant domain.TenantID, id string) (VM, error) {
	var v VM
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, name, desired, generation, phase
		FROM vms WHERE id = $1::uuid AND tenant_id = $2`,
		id, string(tenant),
	).Scan(&v.ID, &v.Name, &v.Desired, &v.Generation, &v.Phase)
	if isNotFound(err) {
		return VM{}, ErrNotFound
	}
	if err != nil {
		return VM{}, fmt.Errorf("get vm: %w", err)
	}
	return v, nil
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
		UPDATE operations
		SET status = 'running',
		    attempts = attempts + 1,
		    lease_expires_at = now() + make_interval(secs => $1::float8)
		WHERE id = (
			SELECT id FROM operations
			WHERE (status = 'pending' AND run_after <= now())
			   OR (status = 'running' AND lease_expires_at < now())
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id::text, vm_id::text, tenant_id, kind, attempts, vm_generation`,
		s.Lease.Seconds(),
	).Scan(&w.OpID, &w.VMID, &w.TenantID, &w.Kind, &w.Attempt, &w.Generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return Work{}, ErrNoWork
	}
	if err != nil {
		return Work{}, fmt.Errorf("claim: %w", err)
	}
	return w, nil
}

// ErrLeaseLost means the operation is no longer running under this lease.
var ErrLeaseLost = errors.New("lease lost")

// Complete marks the operation done and updates the VM phase atomically,
// but only for the worker holding the current token.
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

	tag, err := tx.Exec(ctx, `
		UPDATE operations SET status = 'done', lease_expires_at = NULL
		WHERE id = $1::uuid AND attempts = $2 AND status = 'running'`,
		w.OpID, w.Attempt)
	if err != nil {
		return fmt.Errorf("mark done: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost // someone else owns this job now: discard our result
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

// Fail records a failed attempt. Below maxAttempts the job goes back to pending
// with backoff; at maxAttempts it fails for good. A stale worker gets ErrLeaseLost.
func (s *Store) Fail(ctx context.Context, w Work, maxAttempts int) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE operations SET
			status = CASE WHEN attempts >= $3 THEN 'failed' ELSE 'pending' END,
			run_after = now() + interval '1 second' * power(2, attempts - 1) * (0.5 + random()),
			lease_expires_at = NULL
		WHERE id = $1::uuid AND attempts = $2 AND status = 'running'`,
		w.OpID, w.Attempt, maxAttempts)
	if err != nil {
		return fmt.Errorf("record failure: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Heartbeat extends the lease, but only for the worker holding the current token.
func (s *Store) Heartbeat(ctx context.Context, w Work) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE operations
		SET lease_expires_at = now() + make_interval(secs => $3::float8)
		WHERE id = $1::uuid AND attempts = $2 AND status = 'running'`,
		w.OpID, w.Attempt, s.Lease.Seconds())
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// ListVMs returns every VM, for the reconciler.
func (s *Store) ListVMs(ctx context.Context) ([]domain.VM, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text, tenant_id, name, desired, generation, phase FROM vms`)
	if err != nil {
		return nil, fmt.Errorf("list vms: %w", err)
	}
	defer rows.Close()
	var out []domain.VM
	for rows.Next() {
		var id, tenant, name, desired, phase string
		var gen int64
		if err := rows.Scan(&id, &tenant, &name, &desired, &gen, &phase); err != nil {
			return nil, fmt.Errorf("scan vm: %w", err)
		}
		out = append(out, domain.VM{
			ID: domain.VMID(id), TenantID: domain.TenantID(tenant), Name: name,
			Desired: domain.Desired(desired), Generation: gen, Phase: domain.Phase(phase),
		})
	}
	return out, rows.Err()
}

// Repair enqueues a corrective operation ("create" or "delete") for a VM that has
// drifted from the node. It re-checks the VM's state inside the transaction, so a
// repair based on a stale read does nothing. It returns false if nothing was enqueued.
func (s *Store) Repair(ctx context.Context, vmID, kind string) (bool, error) {
	wantDesired, wantPhase := "present", "RUNNING"
	if kind == "delete" {
		wantDesired, wantPhase = "absent", "DELETED"
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var tenant string
	var gen int64
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, generation FROM vms
		WHERE id = $1::uuid AND desired = $2 AND phase = $3
		FOR UPDATE`, vmID, wantDesired, wantPhase).Scan(&tenant, &gen)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // the VM changed since the reconciler looked
	}
	if err != nil {
		return false, fmt.Errorf("select vm: %w", err)
	}

	// ON CONFLICT DO NOTHING: if an operation is already in flight, the partial
	// unique index rejects ours and we simply do nothing.
	var opID string
	err = tx.QueryRow(ctx, `
		INSERT INTO operations (tenant_id, vm_id, kind, vm_generation, idempotency_key, request_hash)
		VALUES ($1, $2::uuid, $3, $4, 'repair-' || gen_random_uuid()::text, 'repair')
		ON CONFLICT DO NOTHING
		RETURNING id::text`, tenant, vmID, kind, gen).Scan(&opID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insert repair operation: %w", err)
	}

	if kind == "create" {
		if _, err := tx.Exec(ctx, `UPDATE vms SET phase = 'PENDING' WHERE id = $1::uuid`, vmID); err != nil {
			return false, fmt.Errorf("reset phase: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}
