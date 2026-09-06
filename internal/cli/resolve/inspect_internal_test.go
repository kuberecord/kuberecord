/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resolve

import "testing"

// The two predicates behind a notice's weight, tested against the chain records
// rather than against the sentences those chains produce.
//
// They are worth their own table even though internal/cli drives whole
// resolutions through them: the end-to-end cases can only reach the states a
// fixture cluster can be put into, and these have to be right for the states it
// cannot — a chain that failed, a chain nobody walked, a step added later.

// TestAnsweringStepReadsTheChainsOwnRecord.
func TestAnsweringStepReadsTheChainsOwnRecord(t *testing.T) {
	tests := []struct {
		name  string
		chain []ChainStep
		want  string
	}{
		{
			name: "the step that answered",
			chain: []ChainStep{
				{Step: stepClusterIDFlag, Outcome: StepSilent},
				{Step: stepContextMapping, Outcome: StepAnswered},
				{Step: stepOperatorDeployment, Outcome: StepNotReached},
			},
			want: stepContextMapping,
		},
		{
			// A chain that failed has no answer, and reporting the failing step as
			// one would put a weight on a notice that is never printed.
			name: "a chain that failed answered nothing",
			chain: []ChainStep{
				{Step: stepClusterIDFlag, Outcome: StepSilent},
				{Step: stepOperatorDeployment, Outcome: StepFailed},
			},
			want: "",
		},
		{
			// The state Inspect leaves behind when it declines to question the
			// backend: nothing failed and nothing answered.
			name: "a withheld last step answered nothing",
			chain: []ChainStep{
				{Step: stepSink, Outcome: StepWithheld},
			},
			want: "",
		},
		{name: "a chain nobody walked", chain: nil, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := answeringStep(tc.chain); got != tc.want {
				t.Errorf("answeringStep = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestClusterIDRoutineSeparatesStatedFromInferred.
//
// The line between the two is the whole decision: an identity the user stated is
// their own words handed back, and an identity the tool worked out is the one
// that can be silently wrong — it does not fail a query, it returns another
// cluster's history looking exactly like an answer.
func TestClusterIDRoutineSeparatesStatedFromInferred(t *testing.T) {
	answered := func(step string) []ChainStep {
		return []ChainStep{{Step: step, Outcome: StepAnswered}}
	}

	tests := []struct {
		name string
		step string
		want bool
	}{
		{name: "stated outright", step: stepClusterIDFlag, want: true},
		{name: "stated once for this context", step: stepContextMapping, want: true},
		{name: "read off a Deployment the tool found", step: stepOperatorDeployment, want: false},
		{name: "taken from whatever the sink holds", step: stepSink, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := clusterIDRoutine(answered(tc.step)); got != tc.want {
				t.Errorf("clusterIDRoutine(%s) = %t, want %t", tc.step, got, tc.want)
			}
		})
	}

	// A chain with no answer prints no identity notice, so the weight is never
	// consulted — but it must not report the unresolved state as the ordinary
	// one, or a later caller reading it would recede a line about nothing.
	if clusterIDRoutine(nil) {
		t.Error("an unresolved identity chain reports itself as a routine resolution")
	}
}

// TestOnlyDiscoveryIsARoutineOrigin.
//
// Every step of the backend chain except discovery is something overriding what
// the cluster itself points at, which is the case the notice is worth reading.
// The empty Origin is the chain refused before its first step; it prints no
// notice, and answering "routine" for it would be claiming an ordinary
// resolution happened.
func TestOnlyDiscoveryIsARoutineOrigin(t *testing.T) {
	for origin, want := range map[Origin]bool{
		OriginDiscovered: true,
		OriginProfile:    false,
		OriginSinkFlag:   false,
		OriginSourceFlag: false,
		Origin(""):       false,
	} {
		if got := origin.routine(); got != want {
			t.Errorf("Origin(%q).routine() = %t, want %t", origin, got, want)
		}
	}
}
