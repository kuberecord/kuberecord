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

// These tests deliberately carry no build tag, unlike the harness itself: the
// profiles are the contract between docs/PERFORMANCE.md and `make bench-load`,
// and a profile that no longer parses (or one whose numbers drifted below the
// acceptance criteria) must fail in `make test` rather than an hour into a
// benchmark run nobody starts until release day.
package loadgen

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestShippedProfilesLoad checks every shipped profile parses, validates, and
// declares the load the Phase 2 acceptance criteria name.
func TestShippedProfilesLoad(t *testing.T) {
	tests := []struct {
		name          string
		minObjects    int
		minRate       int
		minKinds      int
		wantDeletes   bool
		wantPassRate  float64
		wantPassP99Ms float64
	}{
		{
			name:          ProfileSmall,
			minObjects:    500,
			minRate:       10,
			minKinds:      1,
			wantPassP99Ms: 10,
		},
		{
			name:          ProfileMedium,
			minObjects:    5000,
			minRate:       100,
			minKinds:      1,
			wantPassP99Ms: 10,
		},
		{
			// The Phase 2 acceptance criteria, restated as a test: ≥20,000
			// watched objects, ≥500 sustained updates/sec, mixed GVKs, churn
			// deletes in the mix, and the pass criteria the run is judged by.
			name:          ProfileMassive,
			minObjects:    20000,
			minRate:       500,
			minKinds:      2,
			wantDeletes:   true,
			wantPassRate:  500,
			wantPassP99Ms: 10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := LoadProfile(ProfilesDir, tc.name)
			if err != nil {
				t.Fatalf("LoadProfile(%q): %v", tc.name, err)
			}
			if p.Objects < tc.minObjects {
				t.Errorf("objects = %d, want at least %d", p.Objects, tc.minObjects)
			}
			if p.Rate < tc.minRate {
				t.Errorf("rate = %d, want at least %d", p.Rate, tc.minRate)
			}
			if len(p.Kinds) < tc.minKinds {
				t.Errorf("kinds = %v, want at least %d", p.Kinds, tc.minKinds)
			}
			if tc.wantDeletes && p.DeleteRatio <= 0 {
				t.Errorf("deleteRatio = %v, want a positive churn-delete fraction", p.DeleteRatio)
			}
			if p.Pass.MinRecordsPerSec < tc.wantPassRate {
				t.Errorf("pass.minRecordsPerSec = %v, want at least %v", p.Pass.MinRecordsPerSec, tc.wantPassRate)
			}
			if tc.wantPassP99Ms > 0 && p.Pass.MaxEnqueueBlockP99Ms > tc.wantPassP99Ms {
				t.Errorf("pass.maxEnqueueBlockP99Ms = %v, want no looser than %v",
					p.Pass.MaxEnqueueBlockP99Ms, tc.wantPassP99Ms)
			}
			if p.Description == "" {
				t.Error("description is empty; a published envelope needs to say what shape it describes")
			}
			if _, err := resolveKinds(p.Kinds); err != nil {
				t.Errorf("resolveKinds(%v): %v", p.Kinds, err)
			}
		})
	}
}

// TestShippedProfilesAreExactlyTheDirectory guards against the two ways the set
// can drift: a shipped profile deleted (a `make bench-load PROFILE=…` in the docs
// that no longer runs) or an experiment committed by accident (a fourth envelope
// nobody published).
func TestShippedProfilesAreExactlyTheDirectory(t *testing.T) {
	entries, err := os.ReadDir(ProfilesDir)
	if err != nil {
		t.Fatalf("read %s: %v", ProfilesDir, err)
	}
	var found []string
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		found = append(found, entry.Name()[:len(entry.Name())-len(".json")])
	}
	if len(found) != len(ShippedProfiles) {
		t.Errorf("profiles directory holds %v, want exactly %v", found, ShippedProfiles)
	}
	for _, want := range ShippedProfiles {
		if !slices.Contains(found, want) {
			t.Errorf("shipped profile %q is missing from %s", want, ProfilesDir)
		}
	}
}

// TestLoadProfileRejectsBadFiles covers the failure modes that would otherwise
// publish an envelope for a load nobody applied.
func TestLoadProfileRejectsBadFiles(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		body    string
		wantErr string
	}{
		{
			name:    "missing file",
			file:    "absent",
			wantErr: "read load profile",
		},
		{
			name: "unknown field",
			file: "typo",
			body: `{"name":"typo","objects":1,"rate":1,"duration":"1s","concurrency":1,` +
				`"kinds":["ConfigMap"],"payload_bytes":10}`,
			wantErr: "unknown field",
		},
		{
			name:    "name does not match file",
			file:    "mislabelled",
			body:    `{"name":"other","objects":1,"rate":1,"duration":"1s","concurrency":1,"kinds":["ConfigMap"]}`,
			wantErr: "declares name",
		},
		{
			name:    "numeric duration",
			file:    "numeric-duration",
			body:    `{"name":"numeric-duration","objects":1,"rate":1,"duration":60,"concurrency":1,"kinds":["ConfigMap"]}`,
			wantErr: "duration must be a Go duration string",
		},
		{
			name:    "zero rate",
			file:    "zero-rate",
			body:    `{"name":"zero-rate","objects":1,"rate":0,"duration":"1s","concurrency":1,"kinds":["ConfigMap"]}`,
			wantErr: "rate must be positive",
		},
		{
			name:    "unknown kind",
			file:    "unknown-kind",
			body:    `{"name":"unknown-kind","objects":1,"rate":1,"duration":"1s","concurrency":1,"kinds":["Secret"]}`,
			wantErr: "not churnable",
		},
		{
			name: "repeated kind",
			file: "repeated-kind",
			body: `{"name":"repeated-kind","objects":2,"rate":1,"duration":"1s","concurrency":1,` +
				`"kinds":["ConfigMap","ConfigMap"]}`,
			wantErr: "must not repeat",
		},
		{
			name: "delete ratio out of range",
			file: "bad-ratio",
			body: `{"name":"bad-ratio","objects":1,"rate":1,"duration":"1s","concurrency":1,` +
				`"kinds":["ConfigMap"],"deleteRatio":1.5}`,
			wantErr: "deleteRatio must be in [0,1]",
		},
	}

	dir := t.TempDir()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.body != "" {
				path := filepath.Join(dir, tc.file+".json")
				if err := os.WriteFile(path, []byte(tc.body), 0o600); err != nil {
					t.Fatalf("write fixture: %v", err)
				}
			}
			_, err := LoadProfile(dir, tc.file)
			if err == nil {
				t.Fatalf("LoadProfile(%q) succeeded, want error containing %q", tc.file, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("LoadProfile(%q) error = %q, want it to contain %q", tc.file, err, tc.wantErr)
			}
		})
	}
}

// TestOverridesResolution pins the precedence a published envelope depends on:
// the profile file is the baseline, an environment twin overrides it, and an
// explicit flag overrides both. Getting this backwards would silently attribute
// one load's numbers to another profile's name.
func TestOverridesResolution(t *testing.T) {
	base := Profile{
		Name: "base", Objects: 100, Rate: 10, PayloadBytes: 1024,
		Duration: Duration(10 * time.Second), DeleteRatio: 0, Concurrency: 4,
		Kinds: []string{"ConfigMap"},
	}

	t.Run("unset overrides change nothing", func(t *testing.T) {
		got, err := NoOverrides().Apply(base)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got.String() != base.String() {
			t.Errorf("profile changed under no overrides:\n got %s\nwant %s", got, base)
		}
	})

	t.Run("zero is a real value for the sentinel-guarded fields", func(t *testing.T) {
		o := NoOverrides()
		o.DeleteRatio = 0
		o.PayloadBytes = 0
		got, err := o.Apply(Profile{
			Name: "base", Objects: 100, Rate: 10, PayloadBytes: 4096,
			Duration: Duration(time.Second), DeleteRatio: 0.5, Concurrency: 4,
			Kinds: []string{"ConfigMap"},
		})
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got.DeleteRatio != 0 {
			t.Errorf("deleteRatio = %v, want the explicit 0 to have been applied", got.DeleteRatio)
		}
		if got.PayloadBytes != 0 {
			t.Errorf("payloadBytes = %d, want the explicit 0 to have been applied", got.PayloadBytes)
		}
	})

	t.Run("env twins override the file", func(t *testing.T) {
		env := map[string]string{
			"LOADGEN_OBJECTS":       "7",
			"LOADGEN_RATE":          "70",
			"LOADGEN_PAYLOAD_BYTES": "77",
			"LOADGEN_DURATION":      "7s",
			"LOADGEN_DELETE_RATIO":  "0.7",
			"LOADGEN_CONCURRENCY":   "7",
			"LOADGEN_KINDS":         "ConfigMap, Deployment",
		}
		o, err := OverridesFromEnv(func(key string) string { return env[key] })
		if err != nil {
			t.Fatalf("OverridesFromEnv: %v", err)
		}
		got, err := o.Apply(base)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got.Objects != 7 || got.Rate != 70 || got.PayloadBytes != 77 ||
			got.Duration.Duration() != 7*time.Second || got.DeleteRatio != 0.7 || got.Concurrency != 7 {
			t.Errorf("env overrides not applied: %s", got)
		}
		if len(got.Kinds) != 2 || got.Kinds[0] != "ConfigMap" || got.Kinds[1] != "Deployment" {
			t.Errorf("kinds = %v, want [ConfigMap Deployment] (whitespace trimmed)", got.Kinds)
		}
	})

	t.Run("flag overrides win over env", func(t *testing.T) {
		env := map[string]string{"LOADGEN_RATE": "70"}
		envOverrides, err := OverridesFromEnv(func(key string) string { return env[key] })
		if err != nil {
			t.Fatalf("OverridesFromEnv: %v", err)
		}
		withEnv, err := envOverrides.Apply(base)
		if err != nil {
			t.Fatalf("Apply env: %v", err)
		}
		flagged := NoOverrides()
		flagged.Rate = 700
		got, err := flagged.Apply(withEnv)
		if err != nil {
			t.Fatalf("Apply flags: %v", err)
		}
		if got.Rate != 700 {
			t.Errorf("rate = %d, want the flag's 700 to beat the env's 70", got.Rate)
		}
	})

	t.Run("malformed env is reported, not ignored", func(t *testing.T) {
		env := map[string]string{"LOADGEN_RATE": "500/s"}
		if _, err := OverridesFromEnv(func(key string) string { return env[key] }); err == nil {
			t.Error("OverridesFromEnv accepted LOADGEN_RATE=500/s; a silently ignored override " +
				"publishes an envelope for load nobody applied")
		}
	})

	t.Run("an override that breaks the profile is rejected", func(t *testing.T) {
		o := NoOverrides()
		o.Rate = 0
		if _, err := o.Apply(base); err == nil {
			t.Error("Apply accepted rate=0")
		}
		o = NoOverrides()
		o.Kinds = []string{"Secret"}
		if _, err := o.Apply(base); err == nil {
			t.Error("Apply accepted a non-churnable kind")
		}
	})
}

// TestChurnKindsBuildValidObjects checks each churnable kind produces an object
// the pipeline can key and the apiserver would accept: identity in the right
// place, the revision marker present, and the payload actually sized.
func TestChurnKindsBuildValidObjects(t *testing.T) {
	const payload = 1024
	for _, name := range churnableKinds() {
		t.Run(name, func(t *testing.T) {
			kind := churnKinds[name]
			obj := kind.build("default", "obj-1", 3, payload)

			if got := obj.GetKind(); got != name {
				t.Errorf("kind = %q, want %q", got, name)
			}
			if got := obj.GroupVersionKind(); got != kind.GVK {
				t.Errorf("GVK = %v, want %v", got, kind.GVK)
			}
			if obj.GetNamespace() != "default" || obj.GetName() != "obj-1" {
				t.Errorf("identity = %s/%s, want default/obj-1", obj.GetNamespace(), obj.GetName())
			}
			if got := obj.GetAnnotations()[revisionAnnotation]; got != "3" {
				t.Errorf("revision annotation = %q, want %q", got, "3")
			}

			// The filler has to actually be in the serialized object, wherever
			// this kind keeps it, or the profile's payload knob measures nothing.
			marshalled, err := obj.MarshalJSON()
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if len(marshalled) < payload {
				t.Errorf("serialized object is %d bytes, want at least the %d-byte payload",
					len(marshalled), payload)
			}

			// A mutation must change the object, or every churn event would be
			// deduplicated by hash and the run would measure nothing.
			before := string(marshalled)
			setRevision(obj, 4)
			after, err := obj.MarshalJSON()
			if err != nil {
				t.Fatalf("marshal after setRevision: %v", err)
			}
			if before == string(after) {
				t.Error("setRevision left the object unchanged; every churn event would dedup away")
			}
			if got := obj.GetAnnotations()[revisionAnnotation]; got != "4" {
				t.Errorf("revision annotation after setRevision = %q, want %q", got, "4")
			}
		})
	}
}

// TestPlanObjectsSpreadsAcrossKinds checks the object pool is split evenly and
// that names cannot collide across kinds — a collision would make one kind's
// delete-and-recreate look like another kind's reincarnation.
func TestPlanObjectsSpreadsAcrossKinds(t *testing.T) {
	kinds, err := resolveKinds([]string{"ConfigMap", "Deployment", "ServiceAccount"})
	if err != nil {
		t.Fatalf("resolveKinds: %v", err)
	}

	objects := planObjects(kinds, 30)
	if len(objects) != 30 {
		t.Fatalf("planObjects returned %d objects, want 30", len(objects))
	}

	perKind := map[string]int{}
	names := map[string]struct{}{}
	for _, target := range objects {
		perKind[target.kind.GVK.Kind]++
		if _, seen := names[target.name]; seen {
			t.Errorf("duplicate object name %q", target.name)
		}
		names[target.name] = struct{}{}
	}
	for _, kind := range kinds {
		if got := perKind[kind.GVK.Kind]; got != 10 {
			t.Errorf("%s got %d objects, want an even 10", kind.GVK.Kind, got)
		}
	}
}
