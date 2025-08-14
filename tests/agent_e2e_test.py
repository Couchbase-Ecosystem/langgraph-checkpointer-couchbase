import asyncio
from typing import Literal
from langchain_core.runnables import ConfigurableField
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
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
model = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)

def syncTest():
    with CouchbaseSaver.from_conn_info(
        cb_conn_str=os.getenv("CB_CLUSTER") or "couchbase://localhost",
        cb_username=os.getenv("CB_USERNAME") or "Administrator",
        cb_password=os.getenv("CB_PASSWORD") or "password",
        bucket_name=os.getenv("CB_BUCKET") or "test",
        scope_name=os.getenv("CB_SCOPE") or "langgraph",
    ) as checkpointer:
        graph = create_react_agent(model, tools=tools, checkpointer=checkpointer)
        config = {"configurable": {"thread_id": "1"}}
        res = graph.invoke({"messages": [("human", "what's the weather in sf")]}, config)
        
        latest_checkpoint = checkpointer.get(config)
        latest_checkpoint_tuple = checkpointer.get_tuple(config)
        checkpoint_tuples = list(checkpointer.list(config))

        print(latest_checkpoint)
        print(latest_checkpoint_tuple)
        print(checkpoint_tuples)

async def asyncTest():
    async with AsyncCouchbaseSaver.from_conn_info(
        cb_conn_str=os.getenv("CB_CLUSTER") or "couchbase://localhost",
        cb_username=os.getenv("CB_USERNAME") or "Administrator",
        cb_password=os.getenv("CB_PASSWORD") or "password",
        bucket_name=os.getenv("CB_BUCKET") or "test",
        scope_name=os.getenv("CB_SCOPE") or "langgraph",
    ) as checkpointer:
        graph = create_react_agent(model, tools=tools, checkpointer=checkpointer)
        config = {"configurable": {"thread_id": "2"}}
        res = await graph.ainvoke(
            {"messages": [("human", "what's the weather in nyc")]}, config
        )

        latest_checkpoint = await checkpointer.aget(config)
        latest_checkpoint_tuple = await checkpointer.aget_tuple(config)
        checkpoint_tuples = [c async for c in checkpointer.alist(config)]

        print(latest_checkpoint)
        print(latest_checkpoint_tuple)
        print(checkpoint_tuples)

if __name__ == "__main__":
    syncTest()
    asyncio.run(asyncTest())