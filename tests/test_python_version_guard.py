"""The package refuses to import on Python versions older than 3.10."""
import importlib
import sys

import pytest

import langgraph_checkpointer_couchbase


def test_import_fails_with_clear_error_on_old_python(monkeypatch):
    monkeypatch.setattr(sys, "version_info", (3, 9, 18, "final", 0))
    with pytest.raises(ImportError, match="requires Python 3.10 or newer"):
        importlib.reload(langgraph_checkpointer_couchbase)


def test_import_succeeds_on_supported_python():
    importlib.reload(langgraph_checkpointer_couchbase)
    assert langgraph_checkpointer_couchbase.CouchbaseSaver
