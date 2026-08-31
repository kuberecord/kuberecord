#!/usr/bin/env bash
#
# Print the krew plugin manifest for one release (Task 12.2).
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
# The platform-to-archive pairing is passed in rather than derived, because the
# archive naming convention lives in the Makefile (`cli-archive`) and a second copy
# of it here would keep emitting plausible file names after the archives were
# renamed. krew would then publish URLs that 404, and nothing in the release would
# have failed.
#
# Exit codes: 0 a manifest was printed, 2 the arguments or the archives are
# unusable. There is no "worked but incompletely" outcome — a manifest missing a
# platform is a manifest that silently stops shipping to those users.
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: hack/krew-manifest.sh <version> <repo> <archive-dir> <os/arch>=<archive>...

  <version>      the release tag, v-prefixed (v0.3.0, v0.3.0-rc.1).
  <repo>         <owner>/<name> on GitHub, which is where the URIs point.
  <archive-dir>  the directory the archives were packaged into.
  <os/arch>=…    one pair per platform, naming the archive that carries it:
                 linux/amd64=kuberecord_v0.3.0_linux_amd64.tar.gz
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

# The plugin's name is not a choice: krew requires the manifest's file name, the
# `metadata.name` in it and the part of the binary name after `kubectl-` to be the
# same string, and kubectl requires the binary to be `kubectl-<name>`.
plugin_name='kuberecord'
plugin_binary="kubectl-$plugin_name"

# The Homebrew tap is a repository of its own under the same owner, and `brew`
# addresses it by owner rather than by repository name — `kuberecord/tap`, not
# `kuberecord/homebrew-tap`. The caveats below name it, so the owner is taken
# apart here rather than assembled wrongly there.
owner="${repo%%/*}"

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

# The per-platform blocks are built first, so that a missing archive fails before
# any of the document has been printed. A manifest truncated halfway is a manifest
# a caller might still redirect into a file.
platforms=''
for pair in "$@"; do
	case "$pair" in
	*/*=*) ;;
	*)
		echo "krew-manifest.sh: '$pair' is not an <os>/<arch>=<archive> pair" >&2
		exit 2
		;;
	esac

	platform="${pair%%=*}"
	archive="${pair#*=}"
	os="${platform%%/*}"
	arch="${platform##*/}"

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

	# Windows binaries carry the extension, and krew installs the file it is told
	# to: a `bin` naming the extensionless name would install a plugin Windows
	# cannot execute.
	if [ "$os" = "windows" ]; then
		binary="$plugin_binary.exe"
	else
		binary="$plugin_binary"
	fi

	# `files` is narrowed to the plugin binary and the licence on purpose. The
	# archive also carries the standalone `kuberecord` name, which is the same
	# bytes; letting krew copy the default (everything) would put a second
	# sixty-megabyte copy of one binary in every user's krew store for a name krew
	# cannot invoke anyway.
	platforms="$platforms$(
		cat <<EOF
  - selector:
      matchLabels:
        os: $os
        arch: $arch
    uri: https://github.com/$repo/releases/download/$version/$archive
    sha256: $digest
    bin: $binary
    files:
    - from: $binary
      to: .
    - from: LICENSE
      to: .
EOF
	)
"
done

if [ -z "$platforms" ]; then
	echo "krew-manifest.sh: no platforms were given, so the manifest would install nowhere" >&2
	exit 2
fi

# shortDescription is what `kubectl krew search` prints in a column, so krew-index
# review asks for something short and without a trailing full stop. It is the
# root command's own Short string, which is where a reader meets the same sentence.
cat <<EOF
# Generated by hack/krew-manifest.sh for $version. Do not edit by hand: the
# digests below are computed from the release archives, and an edited copy is a
# copy that has stopped describing them.
#
# This file is published as a release asset, covered by checksums.txt and by the
# keyless cosign signature over it, and submitted to kubernetes-sigs/krew-index.
apiVersion: krew.googlecontainertools.github.com/v1alpha2
kind: Plugin
metadata:
  name: $plugin_name
spec:
  version: $version
  homepage: https://github.com/$repo
  shortDescription: Query recorded Kubernetes state changes
  description: |
    kuberecord answers questions about recorded Kubernetes state changes — who
    changed what, when, and what the object looked like before — without needing
    the cluster the change happened in to still exist.

    A kuberecord operator streams cluster state to a sink; this plugin is the
    read side of it.

      kubectl kuberecord timeline deploy/checkout -n payments
      kubectl kuberecord diff deploy/checkout -n payments --since 1h
      kubectl kuberecord get deploy/checkout -n payments --at 2026-08-28T13:00:00Z
      kubectl kuberecord blame deploy/checkout -n payments

    An empty answer is never presented on its own. Every command consults the
    watch-scope log first, so "nothing changed" and "nothing was watching" are
    reported as the different facts they are.

    Reference: https://github.com/$repo/blob/$version/docs/CLI.md
  caveats: |
    This installs the kubectl plugin only.

    The same build also ships under the name 'kuberecord', which runs on its own
    against an object-store archive with no cluster at all — an auditor's copy.
    Install that from the Homebrew tap or from the release archive:

      brew install $owner/tap/$plugin_name

    The plugin reads what a kuberecord operator has already recorded. With no
    operator running and no --source pointing at an archive one wrote, there is
    nothing to query yet:

      https://github.com/$repo/blob/$version/README.md
  platforms:
$platforms
EOF
