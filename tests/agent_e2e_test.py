import asyncio
from typing import Literal
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langchain.agents import create_agent
from langgraph_checkpointer_couchbase import CouchbaseSaver, AsyncCouchbaseSaver
from dotenv import load_dotenv
import os

load_dotenv()

@tool
def get_weather(city: Literal["nyc", "sf"]):
    """Use this to get weather information."""
    if city == "nyc":
        return "It might be cloudy in nyc"
    elif city == "sf":
        return "It's always sunny in sf"
    else:
        raise AssertionError("Unknown city")


tools = [get_weather]
model = ChatOpenAI(model="gpt-5-mini", temperature=0)


def syncTest():
    with CouchbaseSaver.from_conn_info(
        cb_conn_str=os.getenv("CB_CLUSTER") or "couchbase://localhost",
        cb_username=os.getenv("CB_USERNAME") or "test",
        cb_password=os.getenv("CB_PASSWORD") or "Tester@123",
        bucket_name=os.getenv("CB_BUCKET") or "b1",
        scope_name=os.getenv("CB_SCOPE") or "s1",
    ) as checkpointer:
        graph = create_agent(
            model,
            tools=tools,
            checkpointer=checkpointer,
        )
        config = {"configurable": {"thread_id": "1"}}
        res = graph.invoke({"messages": [("human", "what's the weather in sf")]}, config)
        
        latest_checkpoint = checkpointer.get(config)
        latest_checkpoint_tuple = checkpointer.get_tuple(config)
        checkpoint_tuples = list(checkpointer.list(config))

        print("=== Sync Test Results ===")
        print(f"Response: {res}")
        print(f"Latest checkpoint: {latest_checkpoint}")
        print(f"Latest checkpoint tuple: {latest_checkpoint_tuple}")
        print(f"All checkpoint tuples: {checkpoint_tuples}")


async def asyncTest():
    async with AsyncCouchbaseSaver.from_conn_info(
        cb_conn_str=os.getenv("CB_CLUSTER") or "couchbase://localhost",
        cb_username=os.getenv("CB_USERNAME") or "test",
        cb_password=os.getenv("CB_PASSWORD") or "Tester@123",
        bucket_name=os.getenv("CB_BUCKET") or "b1",
        scope_name=os.getenv("CB_SCOPE") or "s1",
    ) as checkpointer:
        graph = create_agent(
            model,
            tools=tools,
            checkpointer=checkpointer,
        )
        config = {"configurable": {"thread_id": "2"}}
        res = await graph.ainvoke(
            {"messages": [("human", "what's the weather in nyc")]}, config
        )

        latest_checkpoint = await checkpointer.aget(config)
        latest_checkpoint_tuple = await checkpointer.aget_tuple(config)
        checkpoint_tuples = [c async for c in checkpointer.alist(config)]

        print("=== Async Test Results ===")
        print(f"Response: {res}")
        print(f"Latest checkpoint: {latest_checkpoint}")
        print(f"Latest checkpoint tuple: {latest_checkpoint_tuple}")
        print(f"All checkpoint tuples: {checkpoint_tuples}")


if __name__ == "__main__":
    print("Running sync test...")
    syncTest()
    print("\nRunning async test...")
    asyncio.run(asyncTest())
    print("\nAll tests completed!")
