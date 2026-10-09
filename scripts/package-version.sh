#!/usr/bin/env bash
# Prints the package version from langgraph_checkpointer_couchbase/__about__.py, which hatch reads
# as the dynamic version in pyproject.toml.
#
# Usage: scripts/package-version.sh [--next]
#   --next  print the version a placeholder draft should use when this one is already tagged:
#           the stable version after a prerelease (2.1.0rc1 -> 2.1.0), else the next patch.
set -euo pipefail

about="$(dirname "$0")/../langgraph_checkpointer_couchbase/__about__.py"
version="$(sed -nE 's/^__version__[[:space:]]*=[[:space:]]*["'\'']([^"'\'']+)["'\''].*/\1/p' "$about")"
if [[ -z "$version" ]]; then
  echo "No __version__ found in $about" >&2
  exit 1
fi

if [[ "${1:-}" == "--next" ]]; then
  if [[ ! "$version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(.*)$ ]]; then
    echo "Cannot work out the next version after '$version'" >&2
    exit 1
  fi
  major="${BASH_REMATCH[1]}" minor="${BASH_REMATCH[2]}" patch="${BASH_REMATCH[3]}"
  [[ -z "${BASH_REMATCH[4]}" ]] && patch=$((patch + 1))
  version="$major.$minor.$patch"
fi
echo "$version"
