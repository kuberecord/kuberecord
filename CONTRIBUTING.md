# Contributing to kuberecord

Thanks for being here. kuberecord is a Kubernetes operator written in **Go** and
packaged with **Helm**, and it holds itself to one standard above the rest:
what it records must be exactly what happened. Changes are reviewed with that in
mind.

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

**Found a security vulnerability? Do not open an issue.** Follow
[`SECURITY.md`](SECURITY.md) instead.

## Where to start

- **Bugs and features** — open an issue first using one of the
  [templates](.github/ISSUE_TEMPLATE). For anything that changes a CRD field, the
  ClickHouse schema, the S3 object format or an operator flag, please agree the
  shape in an issue before writing code: those surfaces carry stability promises
  ([`docs/RELEASING.md`](docs/RELEASING.md#versioning-policy)), and a rejected PR
  against them is nobody's good afternoon.
- **Small fixes** — typos, a clearer error message, a missing test — need no
  issue. Send the PR.

## Prerequisites

- **Go 1.25+** (`go.mod` is authoritative)
- **Docker** (or another `CONTAINER_TOOL`) for images and the suites that need a
  real backend
- **[kind]** for the e2e, chaos and quickstart suites

Everything else — `kustomize`, `controller-gen`, `setup-envtest`, `helm`,
`kubeconform`, `promtool`, `golangci-lint` — is bootstrapped into `bin/` by the
Makefile. There is nothing to install by hand.

```sh
git clone https://github.com/kuberecord/kuberecord && cd kuberecord
make build
```

## The development loop

```sh
make build test lint      # what CI runs first: build, unit + envtest suite, golangci-lint
```

`make test` is the gate. It regenerates manifests and deepcopy code, runs `gofmt`
and `go vet`, fetches the envtest binaries and then runs the whole suite.

While iterating on one package, `go test ./...` (or `go test ./internal/pipeline/...`
for a single package) is faster. One caveat: the envtest-backed suites under
`api/v1alpha1/` start a real test API server and need `KUBEBUILDER_ASSETS` set, which
`make test` does for you — a bare `go test ./...` will fail those packages until
you have run `make setup-envtest` and exported it. Everything else runs standalone.

Anything touching goroutines, mutexes or channels must carry a `-race` test:

```sh
go test -race ./internal/pipeline/...
```

The heavier suites are opt-in, and worth running when your change touches what
they cover:

```sh
make test-integration   # real dockerized ClickHouse and MinIO — any sink change
make test-e2e           # acceptance suite on kind — controller or RBAC changes
make test-chaos         # failure modes: outages, SIGKILL, saturation
make verify-packaging   # Helm chart and dist/install.yaml — install-path changes
make quickstart         # the ten-minute evaluation path, end to end
```

[`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) explains every target and what each
suite proves. `make help` lists them all.

## What we expect of a change

- **`make build test lint` is clean.** golangci-lint runs with the repository's
  own configuration (`.golangci.yml`); do not add nolint directives without a
  comment saying why.
- **No silent errors.** No `_ =` on a fallible call. Anomalies log at `Error`
  level with kind, namespace and name.
- **Exported symbols carry doc comments that explain *why*,** matching the depth
  of the code around them.
- **Generated artifacts are generated, never hand-edited.** If you touched an API
  type, RBAC marker or the chart, run:

  ```sh
  make manifests generate build-installer helm-sync
  ```

  and commit the result. `git status --short` must be empty afterwards.
- **Documentation is tested.** `test/docs` runs under `make test` and checks that
  every relative link in a published page resolves and that no page tells a reader
  to use configuration that has been removed. If your change alters behaviour a
  document describes, update the document in the same PR.
- **User-visible changes get a changelog entry** under `[Unreleased]` in
  [`CHANGELOG.md`](CHANGELOG.md). That file *is* the release notes, so write it for
  someone upgrading, not for the reviewer.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/): a type, an
optional scope, and an imperative summary.

```
feat(api): add spec.compression to S3Sink
fix(pipeline): commit the job when a rotation flush fails
docs: correct the DuckDB partition-pruning recipe
```

Types in use here: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `chore`,
`build`, `ci`. Mark a breaking change with `!` after the type or scope
(`feat(api)!: …`) and explain the migration in the body — while kuberecord is
pre-1.0 a minor release may break, but every break must be documented.

No CI job rewrites your history; this is a review expectation, and it is what
makes the changelog cheap to write.

## Opening a pull request

1. Branch from `main` and keep the PR focused — one behavioural change, plus the
   tests and documentation it needs.
2. Fill in the [pull request template](.github/PULL_REQUEST_TEMPLATE.md), and link
   the issue it closes (`Closes #123`).
3. Make sure CI is green. Every workflow that runs on your PR is listed in
   [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md#ci); a red one is a request for
   changes from the machine.
4. Expect review comments about invariants rather than style — whether a write can
   block the hot path, whether a commit still fires exactly once, whether a
   degraded sink degrades *visibly*. Those questions are the reason this project
   works.

## License

kuberecord is licensed under the [Apache License 2.0](LICENSE). By contributing,
you agree that your contributions are licensed under it.

[kind]: https://kind.sigs.k8s.io/
