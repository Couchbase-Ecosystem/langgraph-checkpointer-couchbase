# Agent Notes

- Packaging uses hatch/pip (`pyproject.toml`, `hatchling`). Do not add a lockfile or switch package managers.
- PR validation is `.github/workflows/ci.yaml`: it starts a Couchbase container, creates the bucket/scope, creates the recommended indexes, runs `pytest tests/test_checkpointer.py`, then `hatch build`.
- Local validation needs a Couchbase cluster with an existing bucket and scope (see "Running the Tests" in `README.md`). The saver creates its own collections but not indexes; without the README indexes, sequential-scan reads can miss just-written checkpoints and tests flake.
- `tests/agent_e2e_test.py` is the README agent flow and needs `OPENAI_API_KEY`; it is not run in CI.
- Releases publish to PyPI from `.github/workflows/release.yaml` on `v*` tags or manual dispatch. Do not create tags, releases, or dispatch that workflow without maintainer approval. The version lives in `langgraph_checkpointer_couchbase/__about__.py`.
