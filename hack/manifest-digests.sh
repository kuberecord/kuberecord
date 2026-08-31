#!/usr/bin/env bash
#
# Print "<url> <sha256>" for every download a distribution manifest names (Task 12.2).
#
#   hack/manifest-digests.sh dist/release/kuberecord.yaml
#   hack/manifest-digests.sh dist/release/kuberecord.rb
#
# The krew manifest and the Homebrew formula are written by two generators, in two
# syntaxes, and both make the same promise: this URL serves bytes that hash to
# this. Verifying that promise needs the pairs back out of the finished document —
# out of the file that will actually be published, not out of the variables it was
# built from — and doing that in one place is what stops the two verifications
# from drifting apart.
#
# It reads the generated shape rather than parsing YAML or Ruby: both documents
# come from this repository, both are checked against this extractor by the test
# suite, and a real parser for either would be a dependency bought to read a file
# we wrote. What it will not do is guess — a URL with no digest beside it is an
# error, because a silently dropped pair is a download nothing checks.
#
# Exit codes: 0 pairs were printed, 2 the file is unusable or names no downloads.
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: hack/manifest-digests.sh <krew-manifest.yaml|formula.rb>

Prints one "<url> <sha256>" line per download, in the order the file names them.
EOF
}

case "${1:-}" in
-h | --help)
	usage
	exit 0
	;;
esac

if [ "$#" -ne 1 ]; then
	usage >&2
	exit 2
fi

document="$1"
if [ ! -f "$document" ]; then
	echo "manifest-digests.sh: no such file: $document" >&2
	exit 2
fi

# The two shapes, as their generators emit them, matched by their exact leading
# text. Anchoring on the indentation as well as the key is what keeps a `uri:`
# inside a description block from being read as a download.
#
# Prefix matching rather than a regular expression with a capture group: capturing
# needs gawk's three-argument match(), and macOS ships the one-true-awk, which
# does not have it. A maintainer generating a release on a laptop is the common
# case, so the portable spelling is the only one worth having.
case "$document" in
*.yaml | *.yml)
	url_prefix='    uri: '
	sha_prefix='    sha256: '
	quoted=0
	;;
*.rb)
	url_prefix='      url "'
	sha_prefix='      sha256 "'
	quoted=1
	;;
*)
	echo "manifest-digests.sh: $document is neither a .yaml krew manifest nor a .rb formula" >&2
	exit 2
	;;
esac

# awk exits 2 on a broken pairing, which `set -e` turns into this script's exit
# status: a url with no digest beside it is not a smaller answer, it is a download
# nothing checks.
pairs="$(
	awk -v up="$url_prefix" -v sp="$sha_prefix" -v quoted="$quoted" '
		function value(line, prefix,   v) {
			v = substr(line, length(prefix) + 1)
			if (quoted) { sub(/"$/, "", v) }
			return v
		}
		index($0, up) == 1 {
			if (pending != "") {
				printf "manifest-digests.sh: %s is named with no sha256 beside it\n", pending > "/dev/stderr"
				exit 2
			}
			pending = value($0, up)
			next
		}
		index($0, sp) == 1 {
			if (pending == "") {
				printf "manifest-digests.sh: a sha256 (%s) appears with no url before it\n", value($0, sp) > "/dev/stderr"
				exit 2
			}
			print pending, value($0, sp)
			pending = ""
			next
		}
		END {
			if (pending != "") {
				printf "manifest-digests.sh: %s is named with no sha256 beside it\n", pending > "/dev/stderr"
				exit 2
			}
		}
	' "$document"
)"

if [ -z "$pairs" ]; then
	echo "manifest-digests.sh: $document names no downloads, so reading it proved nothing." >&2
	echo "  Either it was generated empty or its shape moved and this extractor did not." >&2
	exit 2
fi

printf '%s\n' "$pairs"
