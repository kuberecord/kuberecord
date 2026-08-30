//go:build unix

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

// Signal delivery is asserted on Unix only. The wiring is one call to
// signal.NotifyContext and is identical on every platform, but os/signal does not
// deliver SIGTERM on Windows and there is no portable way to raise SIGINT against
// one's own process there — so a test written to run everywhere would be a test
// that passes vacuously on the platform it was widened for.

package cli

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestInterruptContextCancelsOnSignal asserts the wiring the whole cancellation
// story rests on.
//
// Without it, the guard's context, the engine's early exit and the exit code would
// all be correct and unreachable: nothing would ever cancel. Delivering a real
// signal is the only way to assert that, and it is safe in this order — the
// handler is installed before the signal is raised, so the default disposition
// never gets it and the test binary is not killed.
func TestInterruptContextCancelsOnSignal(t *testing.T) {
	ctx, stop := interruptContext(context.Background())
	defer stop()

	process, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("finding this process: %v", err)
	}
	if err := process.Signal(os.Interrupt); err != nil {
		t.Fatalf("raising SIGINT: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("SIGINT did not cancel the context; a cold scan would keep listing and decompressing " +
			"after the user pressed Ctrl-C, which is the whole reason this context exists")
	}
	if err := context.Cause(ctx); err == nil {
		t.Error("the cancelled context reports no cause")
	}
}
