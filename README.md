# LangGraph Checkpoint Couchbase

A Couchbase implementation of the LangGraph `CheckpointSaver` interface that enables persisting agent state and conversation history in a Couchbase database.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

This package provides a seamless way to persist LangGraph agent states in Couchbase, enabling:
- State persistence across application restarts
- Retrieval of historical conversation steps
- Continued conversations from previous checkpoints
- Both synchronous and asynchronous interfaces

## Installation

```bash
pip install langgraph-checkpointer-couchbase
```

## Requirements

- Python 3.8+
- Couchbase Server (7.0+ recommended)
- LangGraph 0.3.22+
- LangChain OpenAI 0.3.11+

## Prerequisites

- A running Couchbase cluster
- A bucket created for storing checkpoints
- Appropriate credentials with read/write access

## Quick Start

First, set up your agent tools and model:

```python
from typing import Literal
from langchain_openai import ChatOpenAI

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
```

### Synchronous Usage

```python
import os
from langgraph_checkpointer_couchbase import CouchbaseSaver
from langgraph.graph import create_react_agent

with CouchbaseSaver.from_conn_info(
        cb_conn_str=os.getenv("CB_CLUSTER") or "couchbase://localhost",
        cb_username=os.getenv("CB_USERNAME") or "Administrator",
        cb_password=os.getenv("CB_PASSWORD") or "password",
        bucket_name=os.getenv("CB_BUCKET") or "test",
        scope_name=os.getenv("CB_SCOPE") or "langgraph",
    ) as checkpointer:
    # Create the agent with checkpointing
    graph = create_react_agent(model, tools=tools, checkpointer=checkpointer)
    
    # Configure with a unique thread ID
    config = {"configurable": {"thread_id": "1"}}
    
    # Run the agent
    res = graph.invoke({"messages": [("human", "what's the weather in sf")]}, config)
    
    # Retrieve checkpoints
    latest_checkpoint = checkpointer.get(config)
    latest_checkpoint_tuple = checkpointer.get_tuple(config)
    checkpoint_tuples = list(checkpointer.list(config))

    print(latest_checkpoint)
    print(latest_checkpoint_tuple)
    print(checkpoint_tuples)
```

### Asynchronous Usage

```python
import os
from acouchbase.cluster import Cluster as ACluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
from langgraph_checkpointer_couchbase import AsyncCouchbaseSaver
from langgraph.graph import create_react_agent

auth = PasswordAuthenticator(
    os.getenv("CB_USERNAME") or "Administrator",
    os.getenv("CB_PASSWORD") or "password",
)
options = ClusterOptions(auth)
cluster = await ACluster.connect(os.getenv("CB_CLUSTER") or "couchbase://localhost", options)

bucket_name = os.getenv("CB_BUCKET") or "test"
scope_name = os.getenv("CB_SCOPE") or "langgraph"

async with AsyncCouchbaseSaver.from_cluster(
        cluster=cluster,
        bucket_name=bucket_name,
        scope_name=scope_name,
    ) as checkpointer:
    # Create the agent with checkpointing
    graph = create_react_agent(model, tools=tools, checkpointer=checkpointer)
    
    # Configure with a unique thread ID
    config = {"configurable": {"thread_id": "2"}}
    
    # Run the agent asynchronously
    res = await graph.ainvoke(
        {"messages": [("human", "what's the weather in nyc")]}, config
    )

    # Retrieve checkpoints asynchronously
    latest_checkpoint = await checkpointer.aget(config)
    latest_checkpoint_tuple = await checkpointer.aget_tuple(config)
    checkpoint_tuples = [c async for c in checkpointer.alist(config)]

    print(latest_checkpoint)
    print(latest_checkpoint_tuple)
    print(checkpoint_tuples)

# Close the cluster when done
await cluster.close()
```

## Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| CB_CLUSTER | Couchbase connection string | couchbase://localhost |
| CB_USERNAME | Username for Couchbase | Administrator |
| CB_PASSWORD | Password for Couchbase | password |
| CB_BUCKET | Bucket to store checkpoints | test |
| CB_SCOPE | Scope within bucket | langgraph |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
---

## 📢 Support Policy

We truly appreciate your interest in this project!  
This project is **community-maintained**, which means it's **not officially supported** by our support team.

If you need help, have found a bug, or want to contribute improvements, the best place to do that is right here — by [opening a GitHub issue](https://github.com/Couchbase-Ecosystem/langgraph-checkpointer-couchbase/issues).  
Our support portal is unable to assist with requests related to this project, so we kindly ask that all inquiries stay within GitHub.

Your collaboration helps us all move forward together — thank you!
