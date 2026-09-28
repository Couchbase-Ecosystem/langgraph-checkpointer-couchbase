"""Checkpointer integration tests against a live Couchbase cluster.

These tests do not need an LLM. They exercise CouchbaseSaver and
AsyncCouchbaseSaver directly and through a minimal LangGraph StateGraph.

Required environment variables: CB_CLUSTER, CB_USERNAME, CB_PASSWORD,
CB_BUCKET, CB_SCOPE. The bucket and scope must already exist; the saver
creates its collections. Create the indexes listed in the README so reads
see checkpoints written immediately before.
"""
import operator
import os
import uuid
from typing import Annotated, TypedDict

import pytest
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.graph import END, START, StateGraph

from langgraph_checkpointer_couchbase import AsyncCouchbaseSaver, CouchbaseSaver

REQUIRED_ENV = ["CB_CLUSTER", "CB_USERNAME", "CB_PASSWORD", "CB_BUCKET", "CB_SCOPE"]

pytestmark = pytest.mark.skipif(
    any(not os.getenv(name) for name in REQUIRED_ENV),
    reason=f"Couchbase connection not configured ({', '.join(REQUIRED_ENV)})",
)


def conn_info():
    return dict(
        cb_conn_str=os.environ["CB_CLUSTER"],
        cb_username=os.environ["CB_USERNAME"],
        cb_password=os.environ["CB_PASSWORD"],
        bucket_name=os.environ["CB_BUCKET"],
        scope_name=os.environ["CB_SCOPE"],
    )


def new_thread_config():
    return {"configurable": {"thread_id": f"test-{uuid.uuid4()}", "checkpoint_ns": ""}}


def make_checkpoint(step):
    checkpoint = empty_checkpoint()
    checkpoint["channel_values"] = {"step": step}
    checkpoint["channel_versions"] = {"step": step}
    return checkpoint


class State(TypedDict):
    items: Annotated[list, operator.add]


def build_graph(checkpointer):
    builder = StateGraph(State)
    builder.add_node("first", lambda state: {"items": ["first"]})
    builder.add_node("second", lambda state: {"items": ["second"]})
    builder.add_edge(START, "first")
    builder.add_edge("first", "second")
    builder.add_edge("second", END)
    return builder.compile(checkpointer=checkpointer)


def test_sync_put_get_list_and_writes():
    with CouchbaseSaver.from_conn_info(**conn_info()) as saver:
        config = new_thread_config()
        assert saver.get_tuple(config) is None

        first = make_checkpoint(1)
        first_config = saver.put(config, first, {"source": "input", "step": 1}, {})
        second = make_checkpoint(2)
        second_config = saver.put(first_config, second, {"source": "loop", "step": 2}, {})
        saver.put_writes(second_config, [("items", "a"), ("items", "b")], "task-1")

        latest = saver.get_tuple(config)
        assert latest.config["configurable"]["checkpoint_id"] == second["id"]
        assert latest.checkpoint["channel_values"] == {"step": 2}
        assert latest.metadata == {"source": "loop", "step": 2}
        assert latest.parent_config["configurable"]["checkpoint_id"] == first["id"]
        assert sorted(latest.pending_writes) == [("task-1", "items", "a"), ("task-1", "items", "b")]

        by_id = saver.get_tuple(first_config)
        assert by_id.checkpoint["id"] == first["id"]
        assert by_id.parent_config is None

        listed = list(saver.list(config))
        assert [t.checkpoint["id"] for t in listed] == [second["id"], first["id"]]
        assert [t.checkpoint["id"] for t in saver.list(config, limit=1)] == [second["id"]]
        assert [t.checkpoint["id"] for t in saver.list(config, before=second_config)] == [first["id"]]


@pytest.mark.asyncio
async def test_async_put_get_list_and_writes():
    async with AsyncCouchbaseSaver.from_conn_info(**conn_info()) as saver:
        config = new_thread_config()
        assert await saver.aget_tuple(config) is None

        first = make_checkpoint(1)
        first_config = await saver.aput(config, first, {"source": "input", "step": 1}, {})
        second = make_checkpoint(2)
        second_config = await saver.aput(first_config, second, {"source": "loop", "step": 2}, {})
        await saver.aput_writes(second_config, [("items", "a")], "task-1")

        latest = await saver.aget_tuple(config)
        assert latest.config["configurable"]["checkpoint_id"] == second["id"]
        assert latest.checkpoint["channel_values"] == {"step": 2}
        assert latest.metadata == {"source": "loop", "step": 2}
        assert latest.parent_config["configurable"]["checkpoint_id"] == first["id"]
        assert latest.pending_writes == [("task-1", "items", "a")]

        listed = [t async for t in saver.alist(config)]
        assert [t.checkpoint["id"] for t in listed] == [second["id"], first["id"]]
        limited = [t async for t in saver.alist(config, limit=1)]
        assert [t.checkpoint["id"] for t in limited] == [second["id"]]


def test_sync_graph_persists_state():
    with CouchbaseSaver.from_conn_info(**conn_info()) as saver:
        graph = build_graph(saver)
        config = new_thread_config()

        result = graph.invoke({"items": ["start"]}, config)
        assert result == {"items": ["start", "first", "second"]}

        state = graph.get_state(config)
        assert state.values == {"items": ["start", "first", "second"]}
        assert len(list(graph.get_state_history(config))) >= 3

        # A second run on the same thread continues from the persisted state.
        result = graph.invoke({"items": ["again"]}, config)
        assert result["items"] == ["start", "first", "second", "again", "first", "second"]


@pytest.mark.asyncio
async def test_async_graph_persists_state():
    async with AsyncCouchbaseSaver.from_conn_info(**conn_info()) as saver:
        graph = build_graph(saver)
        config = new_thread_config()

        result = await graph.ainvoke({"items": ["start"]}, config)
        assert result == {"items": ["start", "first", "second"]}

        state = await graph.aget_state(config)
        assert state.values == {"items": ["start", "first", "second"]}
        history = [s async for s in graph.aget_state_history(config)]
        assert len(history) >= 3
