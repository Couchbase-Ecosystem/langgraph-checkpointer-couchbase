"""Offline tests for the document and query layout shared by both savers.

These need no Couchbase cluster.
"""
import pytest
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from langgraph_checkpointer_couchbase import _documents as documents
from langgraph_checkpointer_couchbase.utils import _encode_binary

SERDE = JsonPlusSerializer()
KEYSPACE = "`b`.`s`.`checkpoints`"


def config(checkpoint_id=None, ns=""):
    configurable = {"thread_id": "t1", "checkpoint_ns": ns}
    if checkpoint_id:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


def make_checkpoint(step):
    checkpoint = empty_checkpoint()
    checkpoint["channel_values"] = {"step": step}
    return checkpoint


def test_checkpoint_document_round_trips_through_tuple():
    parent = make_checkpoint(1)
    child = make_checkpoint(2)

    key, doc, next_config = documents.checkpoint_document(
        SERDE, config(parent["id"]), child, {"source": "loop", "step": 2}
    )

    assert key == f"t1::::{child['id']}"
    assert next_config == config(child["id"])
    assert doc["parent_checkpoint_id"] == parent["id"]
    assert (doc["thread_id"], doc["checkpoint_ns"], doc["checkpoint_id"]) == ("t1", "", child["id"])

    restored = documents.checkpoint_tuple_from_document(SERDE, doc, [("task-1", "items", "a")])
    assert restored.config == config(child["id"])
    assert restored.checkpoint["channel_values"] == {"step": 2}
    assert restored.metadata == {"source": "loop", "step": 2}
    assert restored.parent_config == config(parent["id"])
    assert restored.pending_writes == [("task-1", "items", "a")]


def test_root_checkpoint_has_no_parent_config():
    checkpoint = make_checkpoint(1)
    _, doc, _ = documents.checkpoint_document(SERDE, config(), checkpoint, {})
    assert documents.checkpoint_tuple_from_document(SERDE, doc).parent_config is None


@pytest.mark.parametrize("stored", [None, ""])
def test_document_without_metadata_decodes_to_none(stored):
    # Both savers used to differ here: the async saver returned None, the sync
    # saver raised while base64-decoding None.
    _, doc, _ = documents.checkpoint_document(SERDE, config(), make_checkpoint(1), {})
    doc["metadata"] = stored
    assert documents.checkpoint_tuple_from_document(SERDE, doc).metadata is None


def test_legacy_document_without_metadata_type_is_read_as_json():
    _, doc, _ = documents.checkpoint_document(SERDE, config(), make_checkpoint(1), {})
    del doc["metadata_type"]
    doc["metadata"] = _encode_binary(b'{"source": "input"}')
    assert documents.checkpoint_tuple_from_document(SERDE, doc).metadata == {"source": "input"}


def test_write_documents_keys_and_round_trip():
    cfg = config("c1", ns="sub")
    docs = list(documents.write_documents(SERDE, cfg, [("items", "a"), ("items", {"x": 1})], "task-1"))

    assert [key for key, _ in docs] == ["t1::sub::c1::task-1::0", "t1::sub::c1::task-1::1"]
    assert [doc["idx"] for _, doc in docs] == [0, 1]
    assert [documents.pending_write_from_document(SERDE, doc) for _, doc in docs] == [
        ("task-1", "items", "a"),
        ("task-1", "items", {"x": 1}),
    ]


def test_pending_write_without_task_id_is_skipped():
    assert documents.pending_write_from_document(SERDE, {}) is None


def test_get_checkpoint_query_latest_and_by_id():
    query, params = documents.get_checkpoint_query(config(), "b", "s", "checkpoints")
    assert query == f"SELECT * FROM {KEYSPACE} WHERE thread_id = $1 AND checkpoint_ns = $2 ORDER BY checkpoint_id DESC LIMIT 1"
    assert params == ["t1", ""]

    query, params = documents.get_checkpoint_query(config("c1"), "b", "s", "checkpoints")
    assert query == f"SELECT * FROM {KEYSPACE} WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3 ORDER BY checkpoint_id DESC LIMIT 1"
    assert params == ["t1", "", "c1"]


def test_get_checkpoint_query_defaults_missing_namespace():
    _, params = documents.get_checkpoint_query({"configurable": {"thread_id": "t1"}}, "b", "s", "checkpoints")
    assert params == ["t1", ""]


def test_pending_writes_query_targets_checkpoint():
    doc = {"thread_id": "t1", "checkpoint_ns": "sub", "checkpoint_id": "c1"}
    query, params = documents.pending_writes_query(doc, "b", "s", "checkpoint_writes")
    assert query == "SELECT * FROM `b`.`s`.`checkpoint_writes` WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3"
    assert params == ["t1", "sub", "c1"]


def test_list_checkpoints_query_numbers_parameters_in_order():
    query, params = documents.list_checkpoints_query(
        config(), "b", "s", "checkpoints",
        filter={"source": "loop"}, before=config("c9"), limit=5,
    )
    assert query == (
        f"SELECT * FROM {KEYSPACE} WHERE 1=1"
        " AND thread_id = $1 AND checkpoint_ns = $2"
        " AND metadata.source = $3"
        " AND checkpoint_id < $4"
        " ORDER BY checkpoint_id DESC LIMIT 5"
    )
    assert params == ["t1", "", "loop", "c9"]


def test_list_checkpoints_query_without_config():
    query, params = documents.list_checkpoints_query(None, "b", "s", "checkpoints")
    assert query == f"SELECT * FROM {KEYSPACE} WHERE 1=1 ORDER BY checkpoint_id DESC"
    assert params == []


def test_list_checkpoints_query_rejects_non_integer_limit():
    with pytest.raises(ValueError):
        documents.list_checkpoints_query(None, "b", "s", "checkpoints", limit="1; DROP")
