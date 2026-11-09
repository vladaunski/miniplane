CREATE TABLE vms (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   text NOT NULL,
    name        text NOT NULL,
    desired     text NOT NULL CHECK (desired IN ('present','absent')),
    generation  bigint NOT NULL DEFAULT 1,
    phase       text NOT NULL DEFAULT 'PENDING',
    UNIQUE (tenant_id, name)
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
    UNIQUE (tenant_id, idempotency_key)
);

-- At most one in-flight operation per VM, enforced by the database itself.
CREATE UNIQUE INDEX one_active_op_per_vm
    ON operations (vm_id) WHERE status IN ('pending','running');