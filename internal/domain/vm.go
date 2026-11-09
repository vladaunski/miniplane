// Package domain holds the VM model and the pure decision logic.
// It performs no I/O so it can be tested exhaustively.
package domain

// TenantID identifies the customer that owns a VM.
type TenantID string

// VMID identifies a single VM.
type VMID string

// Desired is what the user asked for (the spec).
type Desired string

// Allowed values for Desired.
const (
	DesiredPresent Desired = "present"
	DesiredAbsent  Desired = "absent"
)

// Phase is what the system last observed (the status).
type Phase string

// Allowed values for Phase.
const (
	PhasePending      Phase = "PENDING"
	PhaseProvisioning Phase = "PROVISIONING"
	PhaseRunning      Phase = "RUNNING"
	PhaseDeleting     Phase = "DELETING"
	PhaseDeleted      Phase = "DELETED"
)

// VM is one row of the source of truth. TenantID is mandatory:
// there is no VM without an owner.
type VM struct {
	ID                 VMID
	TenantID           TenantID
	Name               string
	Desired            Desired
	Generation         int64 // bumped on every user change
	Phase              Phase
	ObservedGeneration int64 // the generation the reconciler last acted on
}
