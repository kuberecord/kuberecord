## Description

<!-- What does this PR change, and why? If it changes behaviour, say what the
     behaviour was before and what it is now. -->

Closes #

## Type of change

<!-- Tick everything that applies. -->

- [ ] Bug fix (a non-breaking change that fixes an issue)
- [ ] New feature (a non-breaking change that adds functionality)
- [ ] Breaking change (existing behaviour, a flag, an API field or a default changes)
- [ ] Documentation update
- [ ] Other (refactor, test, build or CI change)

## Checklist

- [ ] `make test` passes locally (`-race` where the change touches concurrency)
- [ ] `make lint` is clean
- [ ] Generated artifacts are regenerated, not hand-edited — `make manifests generate build-installer helm-sync`, and `git status --short` is empty
- [ ] Documentation is updated where this change makes it wrong
- [ ] `CHANGELOG.md` has an `[Unreleased]` entry, if this is user-visible
- [ ] The related issue is linked above

<!-- Breaking change? Describe the migration an existing user has to perform,
     and make sure the changelog entry says the same thing. -->
