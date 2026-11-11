package domain

// Action is what the reconciler should do next for a VM.
type Action int

// Possible Actions. ActionNone is the zero value, so "do nothing" is the safe default.
const (
	ActionNone Action = iota
	ActionCreate
	ActionDelete
)

// NextAction compares desired state with the last known phase.
// It is a pure function: same input, same output, no side effects.
func NextAction(vm VM) Action {
	switch {
	case vm.Desired == DesiredPresent &&
		(vm.Phase == PhasePending || vm.Phase == PhaseProvisioning):
		// PROVISIONING means "in flight": we re-issue the create.
		// That's safe only because the node treats create as idempotent.
		return ActionCreate
	case vm.Desired == DesiredAbsent && vm.Phase != PhaseDeleted:
		return ActionDelete
	default:
		return ActionNone
	}
}

// Drift compares what the database says with what the node reports and returns the repair needed.
func Drift(vm VM, onNode bool) Action {
	switch {
	case vm.Desired == DesiredPresent && vm.Phase == PhaseRunning && !onNode:
		return ActionCreate
	case vm.Desired == DesiredAbsent && vm.Phase == PhaseDeleted && onNode:
		return ActionDelete
	default:
		return ActionNone
	}
}
