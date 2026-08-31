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

package cli_test

import (
	"testing"

	"k8s.io/klog/v2"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// kubeconfigPath is a fixture carrying two contexts, one with a namespace and
// one without, and no credentials of any kind.
const kubeconfigPath = "testdata/kubeconfig"

// TestNamespaceResolution covers the whole of kubectl's namespace precedence, in
// the order kubectl applies it.
//
// Going through ToRawKubeConfigLoader().Namespace() rather than reading the
// --namespace flag is what buys this. Reading the flag would make every
// `-n`-less command mean "default" for the many engineers whose kubeconfig
// context sets a namespace — a silent change of subject rather than an error.
func TestNamespaceResolution(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "the current context's namespace",
			args: []string{"--kubeconfig", kubeconfigPath},
			want: "from-the-kubeconfig",
		},
		{
			name: "an explicit -n wins",
			args: []string{"--kubeconfig", kubeconfigPath, "-n", "from-the-flag"},
			want: "from-the-flag",
		},
		{
			name: "the long spelling behaves the same",
			args: []string{"--kubeconfig", kubeconfigPath, "--namespace", "from-the-flag"},
			want: "from-the-flag",
		},
		{
			name: "--context selects a different context's namespace",
			args: []string{
				"--kubeconfig", kubeconfigPath,
				"--context", "kuberecord-test-no-namespace",
			},
			want: "default",
		},
		{
			name: "a context with no namespace falls back to default",
			args: []string{
				"--kubeconfig", kubeconfigPath,
				"--context", "kuberecord-test-no-namespace",
				"-n", "explicit",
			},
			want: "explicit",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			io, _, _ := streams()
			root, flags := cli.NewRootCommand(options.StandaloneName, io)
			if err := root.PersistentFlags().Parse(test.args); err != nil {
				t.Fatalf("parse %q: %v", test.args, err)
			}

			got, err := flags.Namespace()
			if err != nil {
				t.Fatalf("Namespace(): %v", err)
			}
			if got != test.want {
				t.Errorf("Namespace() = %q, want %q", got, test.want)
			}
		})
	}
}

// TestApplyVerbosityDrivesKlog asserts that -v reaches the logger the Kubernetes
// client libraries actually use.
//
// This is the reason the flag is klog-shaped rather than a local counter: an
// engineer types `-v 6` to see the API requests client-go is making, and a -v
// that only this package understood would leave them staring at silence.
func TestApplyVerbosityDrivesKlog(t *testing.T) {
	// klog's verbosity is process-global. Putting it back is what keeps this
	// test from changing the meaning of every test that runs after it.
	t.Cleanup(func() {
		if err := options.ApplyVerbosity(0); err != nil {
			t.Errorf("restore klog verbosity: %v", err)
		}
	})

	if enabled := klog.V(4); enabled.Enabled() {
		t.Fatal("klog is already verbose before the test set it, so this proves nothing")
	}
	if err := options.ApplyVerbosity(4); err != nil {
		t.Fatalf("options.ApplyVerbosity(4): %v", err)
	}
	if raised := klog.V(4); !raised.Enabled() {
		t.Error("-v 4 did not raise klog's verbosity, so client-go diagnostics stay silent")
	}
	if tooHigh := klog.V(5); tooHigh.Enabled() {
		t.Error("-v 4 enabled level 5, so the level is not being honoured")
	}

	if err := options.ApplyVerbosity(0); err != nil {
		t.Fatalf("options.ApplyVerbosity(0): %v", err)
	}
	if lowered := klog.V(4); lowered.Enabled() {
		t.Error("verbosity could not be lowered again")
	}
}

// TestGlobalFlagDefaults pins what an invocation with no flags means.
func TestGlobalFlagDefaults(t *testing.T) {
	io, _, _ := streams()
	_, flags := cli.NewRootCommand(options.StandaloneName, io)

	if flags.Output != options.OutputTable {
		t.Errorf("default --output = %q, want %q", flags.Output, options.OutputTable)
	}
	if flags.Color != options.ColorAuto {
		t.Errorf("default --color = %q, want %q", flags.Color, options.ColorAuto)
	}
	if flags.ClusterID != "" {
		t.Errorf("default --cluster-id = %q, want empty so Task 11.2 can resolve it", flags.ClusterID)
	}
	if flags.Verbosity != 0 {
		t.Errorf("default -v = %d, want 0", flags.Verbosity)
	}
	if flags.ConfigFlags == nil {
		t.Fatal("the kubeconfig flag surface is absent")
	}
}
