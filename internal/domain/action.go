package domain

// Action is what the reconciler should do next for a VM.
type Action int

// Possible Actions. ActionNone is the zero value, so "do nothing" is the safe default.
const (
	ActionNone Action = iota
	ActionCreate
	ActionDelete
)

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
