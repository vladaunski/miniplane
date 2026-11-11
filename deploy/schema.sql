CREATE TABLE vms (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   text NOT NULL,
    name        text NOT NULL,
    desired     text NOT NULL CHECK (desired IN ('present','absent')),
    generation  bigint NOT NULL DEFAULT 1,
    phase       text NOT NULL DEFAULT 'PENDING'
);

CREATE TABLE operations (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       text NOT NULL,
    vm_id           uuid NOT NULL REFERENCES vms(id),
    kind            text NOT NULL CHECK (kind IN ('create','delete')),
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','running','done','failed')),
    vm_generation   bigint NOT NULL,
    idempotency_key text NOT NULL,
    request_hash    text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    attempts        int         NOT NULL DEFAULT 0,
    run_after       timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz,
    UNIQUE (tenant_id, idempotency_key)
);

-- A name is only taken while the VM is alive, so deleted names can be reused.
CREATE UNIQUE INDEX vm_name_unique_live ON vms (tenant_id, name) WHERE phase <> 'DELETED';

-- At most one in-flight operation per VM, enforced by the database itself.
CREATE UNIQUE INDEX one_active_op_per_vm
    ON operations (vm_id) WHERE status IN ('pending','running');

-- Keeps the claim query fast: only unfinished operations are ever claimed.
CREATE INDEX operations_claimable ON operations (created_at) WHERE status IN ('pending','running');
