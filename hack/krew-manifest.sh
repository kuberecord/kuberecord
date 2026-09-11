#!/usr/bin/env bash
#
# Render the krew plugin manifest for one release from .krew.yaml (Task 12.2,
# Task 18.8).
#
#   hack/krew-manifest.sh <version> <repo> <archive-dir> <os/arch>=<archive>...
#
# krew is how a kubectl user discovers a plugin, and what it consumes is one
# document naming a download URL and its sha256 for every platform. The digests
# are the whole of its integrity story: krew refuses an archive whose bytes do not
# hash to what the manifest says, so a manifest is either generated from the
# artifacts or it is a list of numbers somebody typed. A typed digest is wrong
# exactly once, and it stays wrong until a stranger's install fails on a platform
# nobody here runs.
#
# So this hashes the archives it is pointed at. It reads no checksums file — the
# release has one, and the point of computing these independently is that the two
# then have to agree (see `make release-krew-verify`), which is a real check rather
# than a copy.
#
# What it does *not* do any more is describe the manifest. Since Task 18.8 the one
# description of it is `.krew.yaml` at the repository root, and this renders that
# (D43). The reason is that a second reader appeared: rajatjindal/krew-release-bot
# opens the kubernetes-sigs/krew-index pull request on every tag push, and it
# renders the same template — so a generator that also described the manifest
# would be two descriptions of one artefact, drifting silently, which is the class
# `TestRBACParityWithKustomize` exists to catch elsewhere in this repository.
#
# The template is a Go template using the bot's two helpers, and this implements
# them:
#
#   {{ .TagName }}                          the release tag, substituted anywhere.
#   {{addURIAndSha "<url>" .TagName }}      one `uri:` line and one `sha256:` line.
#
# The accepted language is exactly those two forms and nothing else. Anything else
# between `{{` and `}}` is refused by name rather than passed through, because the
# failure a lenient renderer produces is a manifest this repository publishes and
# the bot renders differently — and nobody would find out until krew-index carried
# a document nothing here had ever printed.
#
# The one deviation from the bot worth knowing: its `addURIAndSha` *downloads* the
# URL to hash it, and this hashes the local archive of that name. That is the
# right way round here — the asset attached to a release must describe the
# archives that release built, not whatever the internet is serving — and the two
# claims are reconciled at tag time by `release-krew-verify-published`, which
# fetches every URL and checks it against this document.
#
# The platform-to-archive pairing is passed in rather than derived, because the
# archive naming convention lives in the Makefile (`cli-archive`) and a second copy
# of it here would keep emitting plausible file names after the archives were
# renamed. krew would then publish URLs that 404, and nothing in the release would
# have failed. Since Task 18.8 that pairing does a second job: the template names
# its own archives, so every pair must be claimed by exactly one platform block and
# every block must name a pair. A platform the release builds and the template
# omits, or the reverse, is refused — which is the parity check between the two
# files, performed on every pull request by `make release-krew-verify`.
#
# Exit codes: 0 a manifest was printed, 2 the arguments, the template or the
# archives are unusable. There is no "worked but incompletely" outcome — a manifest
# missing a platform is a manifest that silently stops shipping to those users.
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: hack/krew-manifest.sh <version> <repo> <archive-dir> <os/arch>=<archive>...

  <version>      the release tag, v-prefixed (v0.3.0, v0.3.0-rc.1).
  <repo>         <owner>/<name> on GitHub, which is where the URIs point.
  <archive-dir>  the directory the archives were packaged into.
  <os/arch>=…    one pair per platform, naming the archive that carries it:
                 linux/amd64=kuberecord_v0.3.0_linux_amd64.tar.gz

Environment:
  KREW_TEMPLATE  the manifest's definition. Defaults to .krew.yaml beside this
                 script's repository root, which is the file the krew release
                 bot reads too; override it only to render a fixture.
EOF
}

case "${1:-}" in
-h | --help)
	usage
	exit 0
	;;
esac

if [ "$#" -lt 4 ]; then
	usage >&2
	exit 2
fi

version="$1"
repo="$2"
archive_dir="$3"
shift 3
pairs=("$@")

# The template is found relative to this script rather than to the caller's
# working directory: `make release-krew-manifest` runs from the repository root,
# but a maintainer debugging one platform should not have to.
template="${KREW_TEMPLATE:-$(cd "$(dirname "$0")/.." && pwd)/.krew.yaml}"
# What the messages below call it. The path above is absolute so that it resolves
# from anywhere, and an absolute path is not what a reader standing in the
# repository root wants read back to them.
template_name="${template#"$PWD/"}"

if ! printf '%s' "$version" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'; then
	echo "krew-manifest.sh: '$version' is not a v-prefixed semantic version" >&2
	exit 2
fi

if ! printf '%s' "$repo" | grep -Eq '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'; then
	echo "krew-manifest.sh: '$repo' is not an <owner>/<name> repository" >&2
	exit 2
fi

if [ ! -d "$archive_dir" ]; then
	echo "krew-manifest.sh: no such directory: $archive_dir" >&2
	exit 2
fi

if [ ! -f "$template" ]; then
	echo "krew-manifest.sh: $template_name does not exist, so there is nothing to render." >&2
	echo "  .krew.yaml at the repository root is the manifest's one definition, and the" >&2
	echo "  krew release bot reads the same file on every tag push." >&2
	exit 2
fi

for pair in "${pairs[@]}"; do
	case "$pair" in
	*/*=*) ;;
	*)
		echo "krew-manifest.sh: '$pair' is not an <os>/<arch>=<archive> pair" >&2
		exit 2
		;;
	esac
done

# platform_of prints the platform whose archive is named, or nothing. The pairs
# are searched rather than indexed because macOS ships bash 3.2, which has no
# associative arrays, and five platforms is not a data structure problem.
platform_of() {
	local want="$1" pair
	for pair in "${pairs[@]}"; do
		if [ "${pair#*=}" = "$want" ]; then
			printf '%s' "${pair%%=*}"
			return 0
		fi
	done
	return 1
}

# sha256sum is GNU and ships on every Linux runner; shasum ships with macOS. The
# release runs on the first and a maintainer rehearses on either, so both spellings
# are here rather than one plus a surprise.
sha256_of() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | cut -d' ' -f1
	else
		shasum -a 256 "$1" | cut -d' ' -f1
	fi
}

# `.TagName` is substituted over the whole template first, which is what the Go
# template does too: the tag inside an `addURIAndSha` URL is rendered by the helper
# with the same value, so one pass over the text is the same document.
if ! rendered_template="$(sed -e "s|{{[[:space:]]*\.TagName[[:space:]]*}}|$version|g" "$template")"; then
	echo "krew-manifest.sh: could not read $template_name" >&2
	exit 2
fi

# The bot's helper, spelled as a bash regexp. The URL is a double-quoted Go string
# literal, so it cannot itself contain a double quote — which is what makes
# `[^"]+` an exact match for the argument rather than an approximation of one.
helper_pattern='^([[:space:]]*)\{\{[[:space:]]*addURIAndSha[[:space:]]+"([^"]+)"[[:space:]]+\.TagName[[:space:]]*\}\}[[:space:]]*$'

# The whole document is built before any of it is printed, so that a missing
# archive fails with nothing on stdout. A manifest truncated halfway is a manifest
# a caller might still redirect into a file.
rendered=''
consumed=''
platforms_seen=0
line_number=0

while IFS= read -r line || [ -n "$line" ]; do
	line_number=$((line_number + 1))

	if [[ "$line" =~ $helper_pattern ]]; then
		indent="${BASH_REMATCH[1]}"
		url="${BASH_REMATCH[2]}"

		# The bot writes the `sha256:` line with four spaces of indentation
		# hardcoded (pkg/source/template.go), so a helper at any other indent
		# renders as valid YAML here and as broken YAML there — one template,
		# two documents, which is the whole thing this file exists to prevent.
		if [ "$indent" != "    " ]; then
			echo "krew-manifest.sh: $template_name line $line_number indents addURIAndSha by" >&2
			echo "  ${#indent} spaces. It must be exactly four: the release bot hardcodes four on the" >&2
			echo "  sha256 line it emits, so any other indent renders differently there than here." >&2
			exit 2
		fi

		archive="${url##*/}"
		expected_prefix="https://github.com/$repo/releases/download/$version/"
		if [ "$url" != "$expected_prefix$archive" ]; then
			echo "krew-manifest.sh: $template_name line $line_number points at" >&2
			echo "    $url" >&2
			echo "  which is not a $version asset of $repo. Published, that is an install command" >&2
			echo "  that serves somebody another release — or another project." >&2
			exit 2
		fi

		if ! platform="$(platform_of "$archive")"; then
			echo "krew-manifest.sh: $template_name names $archive, which is not one of the archives" >&2
			echo "  this release builds. krew would publish a URL that 404s for exactly the users" >&2
			echo "  on that platform, and nothing else in the release would have failed." >&2
			exit 2
		fi

		case " $consumed " in
		*" $platform "*)
			echo "krew-manifest.sh: $template_name names $archive twice. krew picks the first" >&2
			echo "  selector that matches, so the second block is a platform nobody would ever" >&2
			echo "  be served." >&2
			exit 2
			;;
		esac
		consumed="$consumed $platform"

		path="$archive_dir/$archive"
		if [ ! -f "$path" ]; then
			echo "krew-manifest.sh: $platform names $path, which does not exist." >&2
			echo "  Run 'make release-cli' first. A manifest generated without an archive would" >&2
			echo "  publish a URL that 404s for exactly the users on that platform." >&2
			exit 2
		fi

		digest="$(sha256_of "$path")"
		if ! printf '%s' "$digest" | grep -Eq '^[0-9a-f]{64}$'; then
			echo "krew-manifest.sh: hashing $path produced '$digest', which is not a sha256." >&2
			exit 2
		fi

		# Spelled the way the bot spells it: the `uri:` line takes the
		# template's indentation and the `sha256:` line takes the four spaces
		# the helper hardcodes. They are the same four here, and asserting that
		# above is what keeps them the same.
		rendered="$rendered$indent"'uri: '"$url"'
    sha256: '"$digest"'
'
		platforms_seen=$((platforms_seen + 1))
		continue
	fi

	case "$line" in
	*'{{'*)
		echo "krew-manifest.sh: $template_name line $line_number uses a template action this" >&2
		echo "  renderer does not implement:" >&2
		echo "    $line" >&2
		echo "  The manifest has one definition and two readers, so it may only use what both" >&2
		echo "  of them provide: {{ .TagName }} and {{addURIAndSha \"<url>\" .TagName }}, the" >&2
		echo "  latter indented by four spaces." >&2
		exit 2
		;;
	esac

	rendered="$rendered$line
"
done <<EOF
$rendered_template
EOF

if [ "$platforms_seen" -eq 0 ]; then
	echo "krew-manifest.sh: $template_name declares no platforms, so the manifest would install" >&2
	echo "  nowhere. Every platform needs an addURIAndSha line." >&2
	exit 2
fi

# And the other direction. The template names its own archives now, so a platform
# this release builds and the template forgot is not a smaller manifest — it is a
# release that quietly stops shipping to those users.
for pair in "${pairs[@]}"; do
	platform="${pair%%=*}"
	case " $consumed " in
	*" $platform "*) ;;
	*)
		echo "krew-manifest.sh: $template_name has no entry for $platform, which this release" >&2
		echo "  builds (${pair#*=})." >&2
		echo "  krew installs nothing on a platform the manifest omits, and it says so to the" >&2
		echo "  user rather than to us." >&2
		exit 2
		;;
	esac
done

printf '%s' "$rendered"
