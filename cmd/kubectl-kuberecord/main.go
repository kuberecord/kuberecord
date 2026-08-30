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

// Command kubectl-kuberecord is kuberecord's command-line client.
//
// One package produces both shipped binaries. Installed by krew it is
// kubectl-kuberecord and answers to `kubectl kuberecord`; downloaded directly it
// is kuberecord and answers to itself. Which one it is, it learns from argv[0]
// (see cli.InvocationName), so the two never drift into different help texts or
// different flags.
//
// This is not the operator. cmd/main.go is, the Dockerfile references it by that
// path, and the two binaries share no runtime — the CLI is a client of the frozen
// schema and of the read-plane contract, and may not import the operator's
// packages at all (D20, enforced by depguard and by deps_test.go).
//
// Everything worth testing lives in internal/cli, which returns an exit code
// rather than calling os.Exit. This file is the only place that exits.
package main

import (
	"os"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli"
)

func main() {
	streams := genericiooptions.IOStreams{
		In:     os.Stdin,
		Out:    os.Stdout,
		ErrOut: os.Stderr,
	}
	os.Exit(cli.Run(os.Args, streams))
}
