#!/usr/bin/env bash
#
# Print the CHANGELOG.md section for one release version.
#
#   hack/changelog-section.sh v0.1.0 [path/to/CHANGELOG.md]
#
# Why this exists as a script rather than as a line of YAML in the release
# workflow: it is the release gate (Task 3.5). A tag whose notes nobody wrote
# publishes an empty GitHub Release, and the person who finds that out is a
# stranger deciding whether to trust the project. Failing the release instead is
# the whole point, so the rule lives somewhere it can be unit-tested and run by
# hand before tagging.
#
# What it prints is what becomes the GitHub Release body, so CHANGELOG.md is the
# release notes rather than a summary of them.
#
# Exit codes: 0 a section was found, 1 the version has none, 2 the arguments or
# the changelog itself are unusable. The two failure modes are distinct because
# only the first is a missing-notes problem a maintainer fixes by writing notes.
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: hack/changelog-section.sh <version> [changelog]

  <version>    the release, with or without a leading "v" (v0.1.0, 0.1.0,
               v0.2.0-rc.1). A prerelease with no section of its own falls back
               to the section of the version it is a candidate for.
  [changelog]  defaults to CHANGELOG.md beside this script's repository root.
EOF
}

case "${1:-}" in
-h | --help)
	usage
	exit 0
	;;
esac

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
	usage >&2
	exit 2
fi

raw_version="$1"
script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
changelog="${2:-$script_dir/../CHANGELOG.md}"

# Build metadata is not part of a section heading, and the leading "v" is a tag
# convention rather than part of the semantic version Keep a Changelog writes.
version="${raw_version#v}"
version="${version%%+*}"

if ! printf '%s' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'; then
	echo "changelog-section.sh: '$raw_version' is not a semantic version (want vX.Y.Z or vX.Y.Z-prerelease)" >&2
	exit 2
fi

if [ ! -f "$changelog" ]; then
	echo "changelog-section.sh: no changelog at $changelog" >&2
	exit 2
fi

# extract prints the body of the `## [<version>]` section: everything up to the
# next level-two heading, with leading and trailing blank lines dropped so the
# output can be pasted anywhere without a stray gap. `## Unreleased` and any
# other unbracketed heading can never match, which is what stops an unreleased
# section from satisfying the gate.
#
# A link-reference definition at column zero also ends a section. Keep a Changelog
# puts the compare links for every version in one block at the bottom of the file,
# which is inside the oldest section by position and belongs to none of them by
# meaning — pasting it into a GitHub Release body would show it as literal text.
# The consequence is that sections must use inline links, which is what the
# repository does and what test/release asserts.
extract() {
	awk -v want="$1" '
		/^## / {
			if (found) { exit }
			heading = $0
			sub(/^##[ \t]*\[/, "", heading)
			close_idx = index(heading, "]")
			if (close_idx > 1 && substr(heading, 1, close_idx - 1) == want) { found = 1 }
			next
		}
		found && /^\[[^]]+\]:[ \t]/ { exit }
		found {
			if ($0 ~ /^[ \t]*$/) { if (seen) { pending = pending "\n" } ; next }
			printf "%s%s\n", pending, $0
			pending = ""
			seen = 1
		}
	' "$changelog"
}

section="$(extract "$version")"

# A prerelease is a candidate *for* a version, so its notes are that version's
# notes. Requiring a section per release candidate would either duplicate the
# whole section or tempt somebody into writing a placeholder, and a placeholder is
# exactly what the gate exists to keep out of a release.
if [ -z "$section" ]; then
	case "$version" in
	*-*)
		base="${version%%-*}"
		section="$(extract "$base")"
		if [ -n "$section" ]; then
			echo "changelog-section.sh: $raw_version has no section of its own; using [$base], which it is a prerelease of" >&2
		fi
		;;
	esac
fi

if [ -z "$section" ]; then
	present="$(grep -Eo '^##[ \t]*\[[^]]+\]' "$changelog" | sed -E 's/^##[ \t]*\[(.*)\]$/\1/' | tr '\n' ' ' || true)"
	{
		echo "changelog-section.sh: $(basename "$changelog") has no section for $raw_version."
		echo "  Add a '## [$version] - YYYY-MM-DD' section, with the Keep a Changelog groups under it,"
		echo "  in the commit the tag points at. A release whose notes nobody wrote is one a stranger"
		echo "  cannot read, and this is the last point at which that is still cheap to fix."
		echo "  Sections present: ${present:-none}"
	} >&2
	exit 1
fi

printf '%s\n' "$section"
