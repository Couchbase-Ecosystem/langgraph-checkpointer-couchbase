#!/usr/bin/env bash
# Prints the release notes for a v* tag: GitHub's generated notes for the pull requests merged
# since the previous v* tag, grouped by .github/release.yml.
#
# Usage: GH_REPO=owner/repo scripts/release-notes.sh <tag> [<target-ref>]
#   <tag>         the release tag, e.g. v0.1.1. It does not have to exist yet.
#   <target-ref>  the commit the release covers; defaults to the tag if it exists, else HEAD.
# Needs full git history (fetch-depth: 0) and an authenticated `gh`.
set -euo pipefail

tag="${1:?usage: $0 <tag> [<target-ref>]}"
repo="${GH_REPO:?set GH_REPO=owner/repo}"
if [[ -n "${2:-}" ]]; then
  target="$2"
elif git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  target="$tag"
else
  target=HEAD
fi
target_sha="$(git rev-parse "$target^{commit}")"

# The newest v* tag reachable from the target, other than the tag being released. Passing it
# explicitly keeps GitHub from picking an unrelated tag as the starting point.
previous="$(git tag --list 'v*' --merged "$target_sha" --sort=-v:refname | grep -vxF "$tag" | head -1 || true)"

args=(-f "tag_name=$tag" -f "target_commitish=$target_sha")
[[ -n "$previous" ]] && args+=(-f "previous_tag_name=$previous")
gh api "repos/$repo/releases/generate-notes" "${args[@]}" --jq .body
