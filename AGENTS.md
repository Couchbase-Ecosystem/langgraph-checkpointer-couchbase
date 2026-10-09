# Agent Notes

- Packaging uses hatch/pip (`pyproject.toml`, `hatchling`). Do not add a lockfile or switch package managers.
- PR validation is `.github/workflows/ci.yaml`: it starts a Couchbase container, creates the bucket/scope, creates the recommended indexes, runs `pytest tests/test_checkpointer.py`, then `hatch build`.
- Local validation needs a Couchbase cluster with an existing bucket and scope (see "Running the Tests" in `README.md`). The saver creates its own collections but not indexes; without the README indexes, sequential-scan reads can miss just-written checkpoints and tests flake.
- `tests/agent_e2e_test.py` is the README agent flow and needs `OPENAI_API_KEY`; it is not run in CI.

## Release Process

Maintainers release this package in two steps:

1. A PR bumps `__version__` in `langgraph_checkpointer_couchbase/__about__.py`.
2. After that PR merges, a maintainer runs the **Publish Package to PyPI** workflow (`.github/workflows/release.yaml`) from the Actions tab on `main`. It builds with hatch, publishes to PyPI, then publishes the draft GitHub release `v<__version__>` (creating the tag on the released commit). Pushing a `v*` tag also triggers it; the tag must match `__version__`.

Agents prepare step 1 only. Do not create tags or releases, and do not dispatch the release or release-notes workflows.

### Release notes

| Piece | What it does |
| --- | --- |
| `.github/workflows/draft-release.yml` | On every push to `main`, regenerates a draft GitHub release for the next version. If `__version__` is already tagged, the draft is named after a placeholder (the next patch, or the stable version after a pre-release) until the version-bump PR merges. Hand edits to the draft are overwritten on the next merge, so edit it just before releasing. |
| `.github/workflows/release.yaml` | Publishes the draft after the PyPI upload, keeping any hand edits. If there is no draft, it creates the release with generated notes. |
| `.github/workflows/release-notes.yml` | Run by hand with an existing `v*` tag to regenerate that release's notes, for example to backfill a release published without notes or after fixing PR labels. It replaces hand edits. |
| `scripts/release-notes.sh` | Builds the notes: GitHub's generated notes for the PRs merged since the previous `v*` tag. `scripts/package-version.sh` reads `__version__`. |
| `.github/release.yml` | Groups the notes by PR label. PRs labelled `skip-changelog` are left out; unlabelled PRs go under "Other changes". |
| `.github/workflows/label-pull-requests.yml` | Labels PRs from their conventional-commit title and changed paths (`.github/labeler.yml`). It only adds labels, so a label corrected by hand stays. |

| Label | Use for | Applied automatically from |
| --- | --- | --- |
| `breaking-change` | Public API, supported Python, or dependency changes users must act on | `!` in the title or a `BREAKING CHANGE:` footer |
| `enhancement` | New functionality | `feat:` title |
| `bug` | Fixes | `fix:` title |
| `testing` | Test suite changes | `test:` title, `tests/**` |
| `ci` | Workflows and release automation | `ci:`/`build:` title, `.github/**`, `scripts/release-notes.sh` |
| `documentation` | Documentation only | `docs:` title, `*.md` |
| `dependencies` | Dependency updates | `deps:` title or a `deps` scope, `pyproject.toml` |
| `skip-changelog` | Chores that should not appear in the notes | `(release)` scope |

The path rules are read from the base branch, so a change to `.github/labeler.yml` only applies to PR events after it is merged.

### Version bumps in dependency upgrade PRs

Any PR that changes `dependencies` or `requires-python` in `pyproject.toml`, or changes code to follow a dependency upgrade, must bump `__version__` in the same PR. The maintainer can then approve, merge, and run the release workflow without a separate bump PR.

- Check the current release first (`https://pypi.org/pypi/langgraph-checkpointer-couchbase/json`) and bump from that version. Never reuse a published version.
- Pick the level with semver:
  - **Major** (`X.0.0`): the release stops working for some current users, for example raising `requires-python`, moving a dependency floor across a major version, or changing public API in an incompatible way.
  - **Minor** (`x.Y.0`): new backward-compatible features.
  - **Patch** (`x.y.Z`): backward-compatible dependency floor raises and bug fixes.
- For a major bump, fence the incompatibility so users get a clear error instead of a confusing failure: declare it in `pyproject.toml` (`requires-python`, dependency floors, classifiers) and, where install metadata cannot enforce it, add a runtime check with a message that names the requirement.
- Put `Make new Release vX.Y.Z` at the start of the PR title so maintainers can tell release PRs apart (for example `Make new Release v1.0.10: bump langgraph floor`).
- In the PR body, state the old and new version, the bump level and the reason for it, and that a maintainer must run the release workflow after merging.

PRs that only touch CI, docs, or tests do not need a version bump.
