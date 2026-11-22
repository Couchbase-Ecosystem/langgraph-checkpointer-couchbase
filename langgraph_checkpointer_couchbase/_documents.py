"""Couchbase document and query layout shared by the sync and async savers.

``CouchbaseSaver`` and ``AsyncCouchbaseSaver`` differ only in how they talk to
Couchbase. Everything else lives here: the document keys, the document shape,
the SQL++ statements, and turning query rows back into ``CheckpointTuple``
values. Keeping it in one place stops the two savers from drifting apart, and
lets the layout be tested without a cluster.
"""
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
)
from langgraph.checkpoint.serde.base import SerializerProtocol

from .utils import _decode_binary, _encode_binary

# Serialization type assumed for metadata written before ``metadata_type`` was stored.
DEFAULT_METADATA_TYPE = "json"

Query = Tuple[str, List[Any]]
PendingWrite = Tuple[str, str, Any]


def _keyspace(bucket_name: str, scope_name: str, collection_name: str) -> str:
    return f"`{bucket_name}`.`{scope_name}`.`{collection_name}`"


def get_checkpoint_query(
    config: RunnableConfig, bucket_name: str, scope_name: str, collection_name: str
) -> Query:
    """Query for the checkpoint named by ``config``, or the thread's latest one."""
    thread_id = config["configurable"]["thread_id"]
    checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
    checkpoint_id = get_checkpoint_id(config)

    query = f"SELECT * FROM {_keyspace(bucket_name, scope_name, collection_name)} WHERE thread_id = $1 AND checkpoint_ns = $2"
    params: List[Any] = [thread_id, checkpoint_ns]
    if checkpoint_id:
        query += " AND checkpoint_id = $3"
        params.append(checkpoint_id)
    query += " ORDER BY checkpoint_id DESC LIMIT 1"
    return query, params


def pending_writes_query(
    checkpoint_doc: Dict[str, Any], bucket_name: str, scope_name: str, collection_name: str
) -> Query:
    """Query for the writes stored against a checkpoint document."""
    query = f"SELECT * FROM {_keyspace(bucket_name, scope_name, collection_name)} WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3"
    params = [
        checkpoint_doc["thread_id"],
        checkpoint_doc["checkpoint_ns"],
        checkpoint_doc["checkpoint_id"] or "",
    ]
    return query, params


def list_checkpoints_query(
    config: Optional[RunnableConfig],
    bucket_name: str,
    scope_name: str,
    collection_name: str,
    *,
    filter: Optional[Dict[str, Any]] = None,
    before: Optional[RunnableConfig] = None,
    limit: Optional[int] = None,
) -> Query:
    """Query for checkpoints, newest first."""
    query = f"SELECT * FROM {_keyspace(bucket_name, scope_name, collection_name)} WHERE 1=1"
    params: List[Any] = []

    if config is not None:
        query += " AND thread_id = $1 AND checkpoint_ns = $2"
        params.extend([config["configurable"]["thread_id"], config["configurable"].get("checkpoint_ns", "")])

    if filter:
        for key, value in filter.items():
            query += f" AND metadata.{key} = ${len(params) + 1}"
            params.append(value)

    if before is not None:
        query += f" AND checkpoint_id < ${len(params) + 1}"
        params.append(before["configurable"]["checkpoint_id"])

    query += " ORDER BY checkpoint_id DESC"

    if limit is not None:
        query += f" LIMIT {int(limit)}"

    return query, params


def checkpoint_document(
    serde: SerializerProtocol,
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
) -> Tuple[str, Dict[str, Any], RunnableConfig]:
    """Build the key and document for a checkpoint, and the config ``put`` returns."""
    thread_id = config["configurable"]["thread_id"]
    checkpoint_ns = config["configurable"]["checkpoint_ns"]
    checkpoint_id = checkpoint["id"]

    type_, serialized_checkpoint = serde.dumps_typed(checkpoint)
    if serialized_checkpoint:
        serialized_checkpoint = _encode_binary(serialized_checkpoint)

    metadata_type, metadata_bytes = serde.dumps_typed(metadata)
    serialized_metadata = _encode_binary(metadata_bytes) if metadata_bytes else None

    doc = {
        "parent_checkpoint_id": config["configurable"].get("checkpoint_id"),
        "type": type_,
        "checkpoint": serialized_checkpoint,
        "metadata": serialized_metadata,
        "metadata_type": metadata_type,
        "thread_id": thread_id,
        "checkpoint_ns": checkpoint_ns,
        "checkpoint_id": checkpoint_id,
    }
    key = f"{thread_id}::{checkpoint_ns}::{checkpoint_id}"
    next_config: RunnableConfig = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
        }
    }
    return key, doc, next_config


def write_documents(
    serde: SerializerProtocol,
    config: RunnableConfig,
    writes: Sequence[Tuple[str, Any]],
    task_id: str,
) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """Yield the key and document for each pending write."""
    thread_id = config["configurable"]["thread_id"]
    checkpoint_ns = config["configurable"]["checkpoint_ns"]
    checkpoint_id = config["configurable"]["checkpoint_id"]

    for idx, (channel, value) in enumerate(writes):
        type_, serialized_value = serde.dumps_typed(value)
        key = f"{thread_id}::{checkpoint_ns}::{checkpoint_id}::{task_id}::{idx}"
        yield key, {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
            "task_id": task_id,
            "idx": idx,
            "channel": channel,
            "type": type_,
            "value": _encode_binary(serialized_value),
        }


def pending_write_from_document(serde: SerializerProtocol, doc: Dict[str, Any]) -> Optional[PendingWrite]:
    """Decode a pending-write document, or return None if it has no ``task_id``."""
    if "task_id" not in doc:
        return None
    return (
        doc["task_id"],
        doc["channel"],
        serde.loads_typed((doc["type"], _decode_binary(doc["value"]))),
    )


def checkpoint_tuple_from_document(
    serde: SerializerProtocol,
    doc: Dict[str, Any],
    pending_writes: Optional[List[PendingWrite]] = None,
) -> CheckpointTuple:
    """Decode a checkpoint document into a ``CheckpointTuple``.

    Documents without stored metadata decode with ``metadata=None``.
    """
    configurable = {
        "thread_id": doc["thread_id"],
        "checkpoint_ns": doc["checkpoint_ns"],
    }
    checkpoint = serde.loads_typed((doc["type"], _decode_binary(doc["checkpoint"])))
    metadata = (
        serde.loads_typed((doc.get("metadata_type", DEFAULT_METADATA_TYPE), _decode_binary(doc["metadata"])))
        if doc.get("metadata")
        else None
    )
    parent_checkpoint_id = doc.get("parent_checkpoint_id")
    return CheckpointTuple(
        {"configurable": {**configurable, "checkpoint_id": doc["checkpoint_id"]}},
        checkpoint,
        metadata,
        {"configurable": {**configurable, "checkpoint_id": parent_checkpoint_id}} if parent_checkpoint_id else None,
        pending_writes,
    )
