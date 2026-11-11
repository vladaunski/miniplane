# miniplane

A small VM-provisioning control plane written in Go.

A client asks for a VM and gets back an operation ticket immediately. A worker makes it real on a (simulated) node. A reconciler keeps checking that what the database says matches what the node actually has, and repairs the difference.

## Background

This project started from a talk on control and data planes for VM
provisioning at a Nebius lunch lecture. The design here is my own
reconstruction from that talk plus my own analysis; it is not
Nebius's implementation and has no affiliation with them.

## Architecture

```
                 ┌────────────┐   one transaction:    ┌──────────────┐
  client ──────▶ │    api     │   VM row + operation  │  PostgreSQL  │
  POST /vms      │ cmd/api    │ ────────────────────▶ │ vms          │
  202 + op id    └────────────┘                       │ operations   │
                                                      └──────┬───────┘
                                                             │ claim (SKIP LOCKED + lease)
                 ┌────────────────────────────┐              │ complete / fail / heartbeat
                 │  cmd/reconciler            │◀─────────────┘
                 │  worker loop  (every 1s)   │
                 │  drift loop   (every 10s)  │ ── PUT/DELETE /vms/{id} ──▶ ┌────────────┐
                 └────────────────────────────┘ ◀── GET /vms ─────────────  │ cmd/node   │
                                                                             │ (fake data │
                                                                             │  plane)    │
                                                                             └────────────┘
```

- **Control plane** (`api`, `reconciler`): decides what should exist. Stores desired state in Postgres.
- **Data plane** (`node`): does the work. Here it is an in-memory fake; the point is the control plane around it.
- The API never talks to the worker or the node. The database is the only thing they share.

### Request flow

1. `POST /vms` with an `Idempotency-Key`. In one transaction the API inserts the VM (`desired = present`) and a `create` operation, then returns `202 Accepted` with the operation.
2. A worker claims the operation, calls the node, and marks the operation `done` and the VM `RUNNING` in one transaction.
3. The client polls `GET /operations/{id}` until the status is `done`.
4. `DELETE /vms/{id}` works the same way: it flips `desired` to `absent`, bumps the generation and enqueues a `delete`.

### Design decisions

| Concern | Approach |
| --- | --- |
| Desired vs observed state | Each VM has a spec (`desired`, `generation`) and a status (`phase`). Users only change the spec. |
| Long-running operations | `202` plus a pollable operation resource, in the style of Google AIP-151. |
| Duplicate requests | Per-`(tenant, Idempotency-Key)` advisory lock, a unique constraint, and a request hash. Same key and same request returns the original operation. Same key and a different request returns `422`. |
| One change at a time per VM | Partial unique index `one_active_op_per_vm` on pending/running operations. A second concurrent change gets `409`. |
| Name reuse | Partial unique index on `(tenant_id, name)` where `phase <> 'DELETED'`, so a deleted name can be reused. |
| Multiple workers | Single-statement claim with `FOR UPDATE SKIP LOCKED`; each worker gets a different job. |
| Crashed workers | Leases (`lease_expires_at`, using the database clock) extended by a heartbeat goroutine. An expired lease makes the job claimable again. |
| Slow or zombie workers | The `attempts` counter is a fencing token. `Complete`, `Fail` and `Heartbeat` only succeed for the current attempt; otherwise `ErrLeaseLost` and the worker cancels its own node call. |
| Late commands at the node | The node remembers the highest generation it has seen per VM ID, including after deletion, and answers `409` to anything older. |
| Retries | Exponential backoff with jitter via `run_after`, up to 5 claims. A permanent node rejection (stale generation, tenant mismatch, bad request) skips the retries and fails at once. |
| Jobs that keep killing workers | A running job whose lease expires on its last allowed attempt is failed, not handed out again. |
| Failed creates | When a create fails for good, the VM moves to `FAILED` in the same transaction, so clients never see it as `PENDING` forever. A failed VM can still be deleted and its name reused. |
| Drift | The reconciler compares the node's VM list with the database (`domain.Drift`) and enqueues a corrective create or delete. It re-checks the VM under `FOR UPDATE` first, so a stale read does nothing. |
| Tenant isolation | Every query is scoped by tenant. Another tenant's VM looks the same as a missing one. |

### What happens when something fails

| Failure | Result |
| --- | --- |
| Client retries the same request | Same operation returned, nothing new created |
| Two clients race on the same name | One wins, the other gets `409` |
| Worker dies mid-job | Lease expires, another worker takes the job |
| Worker stalls, then wakes up after being replaced | Its `Complete` is rejected; its late node call is refused by generation |
| Node is down | Job retries with backoff; the reconciler skips its pass so it does not enqueue creates for every VM |
| Node rejects a command for good (stale generation, wrong tenant) | No retries: the operation fails immediately and a failed create marks the VM `FAILED` |
| A job keeps crashing its worker | After the last allowed attempt its lease expiry fails it instead of re-queuing it |
| VM vanishes from the node | Reconciler enqueues a create |
| VM left on the node after deletion | Reconciler enqueues a delete |
| Unknown VM on the node | Logged as an orphan, not deleted |

## API

All requests need `X-Tenant-ID`. Create and delete also need `Idempotency-Key`.

```sh
# create (returns 202 and an operation)
curl -i -X POST localhost:8080/vms \
  -H 'X-Tenant-ID: acme' -H 'Idempotency-Key: k1' \
  -d '{"name":"vm1"}'

# poll the operation: pending -> running -> done
curl localhost:8080/operations/<op-id> -H 'X-Tenant-ID: acme'

# inspect the vm
curl localhost:8080/vms/<vm-id> -H 'X-Tenant-ID: acme'

# delete
curl -i -X DELETE localhost:8080/vms/<vm-id> \
  -H 'X-Tenant-ID: acme' -H 'Idempotency-Key: k2'
```

| Status | Meaning |
| --- | --- |
| `202` | Accepted, operation returned |
| `400` | Invalid name, missing or oversized `Idempotency-Key` |
| `401` | Missing `X-Tenant-ID` |
| `404` | VM or operation not found for this tenant |
| `409` | Name taken, or another operation is already in flight for this VM |
| `422` | `Idempotency-Key` reused with a different request |

## Run it

Requirements: Go, Docker, `psql`.

```sh
docker compose -f deploy/docker-compose.yml up -d     # Postgres on localhost:5433, schema applied on first start
export DATABASE_URL=postgres://miniplane:dev@localhost:5433/miniplane

go run ./cmd/node          # fake data plane on :9001
go run ./cmd/reconciler    # worker + reconciler
go run ./cmd/api           # API on :8080
```

Watch it recover: create a VM, stop `cmd/node`, restart it (its memory is empty), and within about 10 seconds the reconciler enqueues a create and the VM comes back.

### Tests

The store tests run against a real Postgres and are skipped when `DATABASE_URL` is not set. They `TRUNCATE` the tables, so use a separate database and stop any running worker first:

```sh
docker compose -f deploy/docker-compose.yml exec db createdb -U miniplane miniplane_test
psql postgres://miniplane:dev@localhost:5433/miniplane_test -f deploy/schema.sql

DATABASE_URL=postgres://miniplane:dev@localhost:5433/miniplane_test make test
make check      # vet, golangci-lint, tests with -race, govulncheck
```

What the tests cover:

- 50 concurrent identical `CreateVM` calls produce exactly one operation.
- 10 competing workers claiming 100 jobs never receive the same job twice.
- Retry with backoff, then permanent failure at the attempt limit.
- A job whose workers keep dying is failed after its last attempt, and its VM becomes `FAILED`.
- A create that fails for good marks the VM `FAILED`; one with attempts left stays `PENDING`; a failed VM can be deleted.
- The node client classifies 400, 403 and 409 as permanent errors and 5xx as retryable.
- A job abandoned by a dead worker is reclaimed after its lease expires; a heartbeating worker keeps its job.
- A stale worker's `Complete` is rejected with `ErrLeaseLost`.
- Delete rules: cross-tenant delete looks like not found, delete during an in-flight create conflicts.
- A missing VM is recreated by `Repair`.
- The node refuses a late create for a VM that was deleted (stale generation).
- Table-driven tests for the pure decision logic in `internal/domain`.

## Layout

```
cmd/api          HTTP API (create, delete, get VM, get operation)
cmd/reconciler   worker loop and drift-repair loop
cmd/node         fake in-memory data plane
internal/domain  VM model and pure decisions (Drift), no I/O
internal/store   all SQL: intake, claim, lease, fencing, repair
internal/nodeclient  HTTP client for the node
deploy           docker-compose and schema.sql
```

## Known limitations

This is a learning project, specifically focused on various trade-offs:

- The database is a single point of failure for control operations (create, delete, reconcile). VMs that are already running are unaffected.
- Orphans on the node are reported, not deleted.
- The node is a fake that keeps VMs in memory. Its generation tombstones are unbounded and are lost on restart.
- `X-Tenant-ID` stands in for authentication. A real system would take the tenant from a verified token.

## License

See [LICENSE](LICENSE).