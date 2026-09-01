#!/usr/bin/env bash
#
# Print the Homebrew formula for one release (Task 12.2).
#
#   hack/homebrew-formula.sh <version> <repo> <archive-dir> <os/arch>=<archive>...
#
# The formula is a binary formula: it downloads the release archive for the
# platform brew is running on and installs both names out of it. It is generated
# from the archives for the same reason the krew manifest is — the sha256 is the
# only thing standing between a user and whatever the URL happens to serve, and a
# digest nobody computed is a digest nobody can be sure of.
#
# It takes the same <os/arch>=<archive> pairs the krew manifest does, and ignores
# the ones brew has no platform for rather than silently narrowing the release:
# Homebrew runs on macOS and Linux, so the Windows archive has no place here and
# saying so out loud is the difference between a decision and an omission.
#
# Exit codes: 0 a formula was printed, 2 the arguments or the archives are
# unusable.
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: hack/homebrew-formula.sh <version> <repo> <archive-dir> <os/arch>=<archive>...

  <version>      the release tag, v-prefixed (v0.3.0, v0.3.0-rc.1).
  <repo>         <owner>/<name> on GitHub, which is where the URLs point.
  <archive-dir>  the directory the archives were packaged into.
  <os/arch>=…    one pair per platform, naming the archive that carries it. The
                 four brew platforms are required; anything else is ignored.
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

# Homebrew versions are plain semver: the `v` is a git tag convention, and a
# formula carrying it would make `brew info` read `v0.3.0` where every other
# formula reads a number.
brew_version="${version#v}"
standalone_name='kuberecord'
plugin_binary='kubectl-kuberecord'

if ! printf '%s' "$version" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'; then
	echo "homebrew-formula.sh: '$version' is not a v-prefixed semantic version" >&2
	exit 2
fi

if ! printf '%s' "$repo" | grep -Eq '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'; then
	echo "homebrew-formula.sh: '$repo' is not an <owner>/<name> repository" >&2
	exit 2
fi

if [ ! -d "$archive_dir" ]; then
	echo "homebrew-formula.sh: no such directory: $archive_dir" >&2
	exit 2
fi

sha256_of() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | cut -d' ' -f1
	else
		shasum -a 256 "$1" | cut -d' ' -f1
	fi
}

# The four blocks brew needs, filled from the pairs and checked afterwards. A
# variable per platform rather than an associative array: bash 3.2 is what macOS
# ships, and a maintainer generating this on their laptop is the common case.
darwin_arm64_archive='' darwin_amd64_archive=''
linux_arm64_archive='' linux_amd64_archive=''

for pair in "$@"; do
	case "$pair" in
	*/*=*) ;;
	*)
		echo "homebrew-formula.sh: '$pair' is not an <os>/<arch>=<archive> pair" >&2
		exit 2
		;;
	esac

	platform="${pair%%=*}"
	archive="${pair#*=}"

	case "$platform" in
	darwin/arm64) darwin_arm64_archive="$archive" ;;
	darwin/amd64) darwin_amd64_archive="$archive" ;;
	linux/arm64) linux_arm64_archive="$archive" ;;
	linux/amd64) linux_amd64_archive="$archive" ;;
	*)
		echo "homebrew-formula.sh: $platform has no Homebrew equivalent; skipping it." >&2
		;;
	esac
done

# resolve prints "<url> <sha256>" for one platform, and refuses to print a
# half-answer. Homebrew installs whichever block matches the machine it is on, so
# a missing block is not a smaller formula — it is a formula that fails for one
# quarter of its users and works perfectly for whoever tested it.
resolve() {
	local platform="$1" archive="$2" path digest

	if [ -z "$archive" ]; then
		echo "homebrew-formula.sh: no archive was given for $platform, which brew installs on." >&2
		echo "  The formula would fail for every user on that platform and for nobody else," >&2
		echo "  so it is not generated at all." >&2
		exit 2
	fi

	path="$archive_dir/$archive"
	if [ ! -f "$path" ]; then
		echo "homebrew-formula.sh: $platform names $path, which does not exist." >&2
		echo "  Run 'make release-cli' first." >&2
		exit 2
	fi

	digest="$(sha256_of "$path")"
	if ! printf '%s' "$digest" | grep -Eq '^[0-9a-f]{64}$'; then
		echo "homebrew-formula.sh: hashing $path produced '$digest', which is not a sha256." >&2
		exit 2
	fi

	printf '%s %s\n' "https://github.com/$repo/releases/download/$version/$archive" "$digest"
}

# Assigned before being split, deliberately. `resolve` runs in a subshell inside a
# command substitution, so its `exit 2` cannot end this script directly — but a
# plain assignment takes the substitution's exit status, and `set -e` then does.
# Feeding the substitution straight into `read` would swallow the failure and
# print a formula with an empty URL in it.
darwin_arm64="$(resolve darwin/arm64 "$darwin_arm64_archive")"
darwin_amd64="$(resolve darwin/amd64 "$darwin_amd64_archive")"
linux_arm64="$(resolve linux/arm64 "$linux_arm64_archive")"
linux_amd64="$(resolve linux/amd64 "$linux_amd64_archive")"

read -r darwin_arm64_url darwin_arm64_sha <<<"$darwin_arm64"
read -r darwin_amd64_url darwin_amd64_sha <<<"$darwin_amd64"
read -r linux_arm64_url linux_arm64_sha <<<"$linux_arm64"
read -r linux_amd64_url linux_amd64_sha <<<"$linux_amd64"

cat <<EOF
# Generated by hack/homebrew-formula.sh for $version. Do not edit by hand: the
# digests below are computed from the release archives, and an edited copy is a
# copy that has stopped describing them. The release workflow replaces this file
# in the tap on every stable tag.
class Kuberecord < Formula
  desc "Query recorded Kubernetes state changes"
  homepage "https://github.com/$repo"
  version "$brew_version"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "$darwin_arm64_url"
      sha256 "$darwin_arm64_sha"
    end

    on_intel do
      url "$darwin_amd64_url"
      sha256 "$darwin_amd64_sha"
    end
  end

  on_linux do
    on_arm do
      url "$linux_arm64_url"
      sha256 "$linux_arm64_sha"
    end

    on_intel do
      url "$linux_amd64_url"
      sha256 "$linux_amd64_sha"
    end
  end

  # Both names come out of one archive because they are one build: "$standalone_name"
  # runs standalone against an object-store archive with no cluster, and
  # "$plugin_binary" is what makes "kubectl kuberecord …" work. kubectl finds a
  # plugin by file name alone, so the second name is not a convenience.
  def install
    bin.install "$standalone_name"
    bin.install "$plugin_binary"
  end

  # The one check that catches a formula pointing at the wrong tag's archive: the
  # version is stamped into the binary at release time, so a stale URL reports a
  # version that is not this formula's.
  test do
    assert_match "$standalone_name v#{version}", shell_output("#{bin}/$standalone_name version")
    assert_match "$standalone_name v#{version}", shell_output("#{bin}/$plugin_binary version")
  end
end
EOF
