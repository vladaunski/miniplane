package domain

import "testing"

func TestDrift(t *testing.T) {
	tests := []struct {
		name   string
		vm     VM
		onNode bool
		want   Action
	}{
		{"running and on node: fine", VM{Desired: DesiredPresent, Phase: PhaseRunning}, true, ActionNone},
		{"running but missing on node: recreate", VM{Desired: DesiredPresent, Phase: PhaseRunning}, false, ActionCreate},
		{"still pending: the queue handles it", VM{Desired: DesiredPresent, Phase: PhasePending}, false, ActionNone},
		{"deleted but still on node: delete", VM{Desired: DesiredAbsent, Phase: PhaseDeleted}, true, ActionDelete},
		{"deleted and gone: fine", VM{Desired: DesiredAbsent, Phase: PhaseDeleted}, false, ActionNone},
		{"delete in flight: the queue handles it", VM{Desired: DesiredAbsent, Phase: PhaseRunning}, true, ActionNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Drift(tc.vm, tc.onNode); got != tc.want {
				t.Errorf("Drift() = %v, want %v", got, tc.want)
			}
		})
	}
}
