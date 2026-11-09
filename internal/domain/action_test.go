package domain

import "testing"

func TestNextAction(t *testing.T) {
	tests := []struct {
		name string
		vm   VM
		want Action
	}{
		{"new vm needs creating", VM{Desired: DesiredPresent, Phase: PhasePending}, ActionCreate},
		{"in-flight create is retried", VM{Desired: DesiredPresent, Phase: PhaseProvisioning}, ActionCreate},
		{"running and wanted: nothing to do", VM{Desired: DesiredPresent, Phase: PhaseRunning}, ActionNone},
		{"running but unwanted: delete", VM{Desired: DesiredAbsent, Phase: PhaseRunning}, ActionDelete},
		{"deleted while still creating: delete wins", VM{Desired: DesiredAbsent, Phase: PhaseProvisioning}, ActionDelete},
		{"already deleted: nothing to do", VM{Desired: DesiredAbsent, Phase: PhaseDeleted}, ActionNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NextAction(tc.vm); got != tc.want {
				t.Errorf("NextAction() = %v, want %v", got, tc.want)
			}
		})
	}
}
