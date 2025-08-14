from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Dict, Optional, Sequence, Tuple
import logging

from langchain_core.runnables import RunnableConfig
from acouchbase.cluster import Cluster as ACluster
from acouchbase.bucket import Bucket as ABucket
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, QueryOptions, UpsertOptions
from couchbase.exceptions import CollectionAlreadyExistsException

from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
)
from .utils import _encode_binary, _decode_binary

logger = logging.getLogger(__name__)

class AsyncCouchbaseSaver(BaseCheckpointSaver):
    """A checkpoint saver that stores checkpoints in a Couchbase database."""

    cluster: ACluster
    bucket: ABucket
    def __init__(
        self,
        cluster: ACluster,
        bucket_name: str,
        scope_name: str,
        checkpoints_collection_name: str = "checkpoints",
        checkpoint_writes_collection_name: str = "checkpoint_writes"
    ) -> None:
        super().__init__()
        self.cluster = cluster
        self.bucket_name = bucket_name
        self.scope_name = scope_name
        self.bucket = self.cluster.bucket(bucket_name)
        self.scope = self.bucket.scope(scope_name)
        self.checkpoints_collection_name = checkpoints_collection_name
        self.checkpoint_writes_collection_name = checkpoint_writes_collection_name

    async def create_collections(self):
        """Create collections in the Couchbase bucket if they do not exist."""

        collection_manager = self.bucket.collections()
        try:
            await collection_manager.create_collection(self.scope_name, self.checkpoints_collection_name)
        except CollectionAlreadyExistsException as _:
            pass
        except Exception as e:
            logger.exception("Error creating collections")
            raise e
        finally:
            self.checkpoints_collection =  self.bucket.scope(self.scope_name).collection(self.checkpoints_collection_name)
        
        try:
            await collection_manager.create_collection(self.scope_name, self.checkpoint_writes_collection_name)
        except CollectionAlreadyExistsException as _:
            pass
        except Exception as e:
            logger.exception("Error creating collections")
            raise e
        finally:
            self.checkpoint_writes_collection = self.bucket.scope(self.scope_name).collection(self.checkpoint_writes_collection_name)

    @classmethod
    @asynccontextmanager
    async def from_conn_info(
        cls, *, cb_conn_str :str, cb_username: str, cb_password: str, bucket_name: str, scope_name: str, checkpoints_collection_name: str = "checkpoints", checkpoint_writes_collection_name: str = "checkpoint_writes"
    ) -> AsyncIterator["AsyncCouchbaseSaver"]:
        """Create a AsyncCouchbaseSaver from connection information.
        
        This allows reusing an existing cluster connection rather than creating a new one.
        
        Args:
            cb_conn_str: Connection string for the Couchbase cluster
            cb_username: Username for the Couchbase cluster
            cb_password: Password for the Couchbase cluster
            bucket_name: Name of the bucket to use
            scope_name: Name of the scope within the bucket
            checkpoints_collection_name: Name of the collection to store checkpoints
            checkpoint_writes_collection_name: Name of the collection to store checkpoint writes
            
        Yields:
            AsyncCouchbaseSaver: An instance of the AsyncCouchbaseSaver
        """        
        cluster = None
        try:
            auth = PasswordAuthenticator(cb_username, cb_password)
            options = ClusterOptions(auth)
            cluster = await ACluster.connect(cb_conn_str, options)
            
            cls.cluster = cluster
            cls.bucket_name = bucket_name
            cls.scope_name = scope_name

            bucket = cluster.bucket(bucket_name)
            await bucket.on_connect()

            saver = AsyncCouchbaseSaver(
                cluster,
                bucket_name,
                scope_name,
                checkpoints_collection_name,
                checkpoint_writes_collection_name,
            )

            await saver.create_collections()
            
            yield saver
        finally:
            if cluster:
                await cluster.close()


    @classmethod
    @asynccontextmanager
    async def from_cluster(
        cls, *, cluster: ACluster, bucket_name: str, scope_name: str, checkpoints_collection_name: str = "checkpoints", checkpoint_writes_collection_name: str = "checkpoint_writes"
    ) -> AsyncIterator["AsyncCouchbaseSaver"]:
        """Create a AsyncCouchbaseSaver from an existing cluster connection.
        
        This allows reusing an existing cluster connection rather than creating a new one.
        
        Args:
            cluster: An existing Couchbase Cluster connection
            bucket_name: Name of the bucket to use
            scope_name: Name of the scope within the bucket
            checkpoints_collection_name: Name of the collection to store checkpoints
            checkpoint_writes_collection_name: Name of the collection to store checkpoint writes
            
        Yields:
            AsyncCouchbaseSaver: An instance of the AsyncCouchbaseSaver
        """        
        
        bucket = cluster.bucket(bucket_name)
        await bucket.on_connect()

        saver = AsyncCouchbaseSaver(
            cluster,
            bucket_name,
            scope_name,
            checkpoints_collection_name,
            checkpoint_writes_collection_name,
        )

        await saver.create_collections()
        
        yield saver

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        """Get a checkpoint tuple from the database asynchronously.

        This method retrieves a checkpoint tuple from the Couchbase database based on the
        provided config. If the config contains a "checkpoint_id" key, the checkpoint with
        the matching thread ID and checkpoint ID is retrieved. Otherwise, the latest checkpoint
        for the given thread ID is retrieved.

        Args:
            config (RunnableConfig): The config to use for retrieving the checkpoint.

        Returns:
            Optional[CheckpointTuple]: The retrieved checkpoint tuple, or None if no matching checkpoint was found.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id = get_checkpoint_id(config)

        if checkpoint_id:
            query = f'SELECT * FROM `{self.bucket_name}`.`{self.scope_name}`.`{self.checkpoints_collection_name}` WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3 ORDER BY checkpoint_id DESC LIMIT 1'
            query_params = [thread_id, checkpoint_ns, checkpoint_id]
        else:
            query = f'SELECT * FROM `{self.bucket_name}`.`{self.scope_name}`.`{self.checkpoints_collection_name}` WHERE thread_id = $1 AND checkpoint_ns = $2 ORDER BY checkpoint_id DESC LIMIT 1'
            query_params = [thread_id, checkpoint_ns]

        result = self.cluster.query(query, QueryOptions(positional_parameters=query_params))

        async for row in result:
            doc = row[self.checkpoints_collection_name]
            config_values = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": doc["checkpoint_id"],
            }
            checkpoint = self.serde.loads_typed((doc["type"], _decode_binary(doc["checkpoint"])))

            serialized_writes_query = f'SELECT * FROM `{self.bucket_name}`.`{self.scope_name}`.`{self.checkpoint_writes_collection_name}` WHERE thread_id = $1 AND checkpoint_ns = $2 AND checkpoint_id = $3'
            serialized_writes_params = [thread_id, checkpoint_ns, doc["checkpoint_id"] or ""]
            
            serialized_writes_result = self.cluster.query(serialized_writes_query, QueryOptions(positional_parameters=serialized_writes_params))

            pending_writes = []
            async for write_doc in serialized_writes_result:
                checkpoint_writes = write_doc.get(self.checkpoint_writes_collection_name, {})
                if "task_id" not in checkpoint_writes:
                    logger.warning("'task_id' is not present in checkpoint_writes")
                else:
                    pending_writes.append(
                        (
                            checkpoint_writes["task_id"],
                            checkpoint_writes["channel"],
                            self.serde.loads_typed((checkpoint_writes["type"], _decode_binary(checkpoint_writes["value"]))),
                        )
                    )

            return CheckpointTuple(
                {"configurable": config_values},
                checkpoint,
                self.serde.loads(_decode_binary(doc["metadata"])),
                (
                    {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": doc["parent_checkpoint_id"],
                        }
                    }
                    if doc.get("parent_checkpoint_id")
                    else None
                ),
                pending_writes,
            )

    async def alist(
            self,
            config: Optional[RunnableConfig],
            *,
            filter: Optional[Dict[str, Any]] = None,
            before: Optional[RunnableConfig] = None,
            limit: Optional[int] = None,
        ) -> AsyncIterator[CheckpointTuple]:
        """List checkpoints from the database asynchronously.

        This method retrieves a list of checkpoint tuples from the Couchbase database based
        on the provided config. The checkpoints are ordered by checkpoint ID in descending order (newest first).

        Args:
            config (Optional[RunnableConfig]): The config to use for listing the checkpoints.
            filter (Optional[Dict[str, Any]]): Additional filtering criteria for metadata. Defaults to None.
            before (Optional[RunnableConfig]): If provided, only checkpoints before the specified checkpoint ID are returned. Defaults to None.
            limit (Optional[int]): The maximum number of checkpoints to return. Defaults to None.

        Yields:
            AsyncIterator[CheckpointTuple]: An asynchronous iterator of checkpoint tuples.
        """

        query = f"SELECT * FROM `{self.bucket_name}`.`{self.scope_name}`.`{self.checkpoints_collection_name}` WHERE 1=1"
        query_params = []

        if config is not None:
            query += " AND thread_id = $1 AND checkpoint_ns = $2"
            query_params.extend([config["configurable"]["thread_id"], config["configurable"].get("checkpoint_ns", "")])

        if filter:
            for key, value in filter.items():
                query += f" AND metadata.{key} = ${len(query_params) + 1}"
                query_params.append(value)

        if before is not None:
            query += f" AND checkpoint_id < ${len(query_params) + 1}"
            query_params.append(before["configurable"]["checkpoint_id"])

        query += " ORDER BY checkpoint_id DESC"

        if limit is not None:
            query += f" LIMIT {limit}"

        result = self.cluster.query(query, QueryOptions(positional_parameters=query_params))

        async for row in result:
            doc = row[self.checkpoints_collection_name]
            checkpoint_data = _decode_binary(doc["checkpoint"])
            checkpoint = self.serde.loads_typed((doc["type"], checkpoint_data))
            yield CheckpointTuple(
                {
                    "configurable": {
                        "thread_id": doc["thread_id"],
                        "checkpoint_ns": doc["checkpoint_ns"],
                        "checkpoint_id": doc["checkpoint_id"],
                    }
                },
                checkpoint,
                self.serde.loads(_decode_binary(doc["metadata"])),
                (
                    {
                        "configurable": {
                            "thread_id": doc["thread_id"],
                            "checkpoint_ns": doc["checkpoint_ns"],
                            "checkpoint_id": doc["parent_checkpoint_id"],
                        }
                    }
                    if doc.get("parent_checkpoint_id")
                    else None
                ),
            )

    async def aput(
            self,
            config: RunnableConfig,
            checkpoint: Checkpoint,
            metadata: CheckpointMetadata,
            new_versions: ChannelVersions,
        ) -> RunnableConfig:
        """Save a checkpoint to the database asynchronously.

        This method saves a checkpoint to the Couchbase database. The checkpoint is associated
        with the provided config and its parent config (if any).

        Args:
            config (RunnableConfig): The config to associate with the checkpoint.
            checkpoint (Checkpoint): The checkpoint to save.
            metadata (CheckpointMetadata): Additional metadata to save with the checkpoint.
            new_versions (ChannelVersions): New channel versions as of this write.

        Returns:
            RunnableConfig: Updated configuration after storing the checkpoint.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        checkpoint_id = checkpoint["id"]
        type_, serialized_checkpoint = self.serde.dumps_typed(checkpoint)
        if serialized_checkpoint:
            serialized_checkpoint = _encode_binary(serialized_checkpoint)
            
        metadata = self.serde.dumps(metadata)
        if metadata:
            metadata = _encode_binary(metadata)
        
        doc = {
            "parent_checkpoint_id": config["configurable"].get("checkpoint_id"),
            "type": type_,
            "checkpoint": serialized_checkpoint,
            "metadata": metadata,
            "thread_id" : thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
        }

        upsert_key = f"{thread_id}::{checkpoint_ns}::{checkpoint_id}"

        # ensure bucket connected (idempotent)
        await self.bucket.on_connect()
        collection = self.bucket.scope(self.scope_name).collection(self.checkpoints_collection_name)
        await collection.upsert(upsert_key, (doc), UpsertOptions(timeout=timedelta(seconds=5)))

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
        }

    async def aput_writes(
            self,
            config: RunnableConfig,
            writes: Sequence[Tuple[str, Any]],
            task_id: str,
        ) -> None:
        """Store intermediate writes linked to a checkpoint asynchronously.

        This method saves intermediate writes associated with a checkpoint to the Couchbase database.

        Args:
            config (RunnableConfig): Configuration of the related checkpoint.
            writes (Sequence[Tuple[str, Any]]): List of writes to store, each as (channel, value) pair.
            task_id (str): Identifier for the task creating the writes.
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_ns = config["configurable"]["checkpoint_ns"]
        checkpoint_id = config["configurable"]["checkpoint_id"]

        await self.bucket.on_connect()
        collection = self.bucket.scope(self.scope_name).collection(self.checkpoint_writes_collection_name)

        for idx, (channel, value) in enumerate(writes):
            upsert_key = f"{thread_id}::{checkpoint_ns}::{checkpoint_id}::{task_id}::{idx}"
            type_, serialized_value = self.serde.dumps_typed(value)
            
            serialized_value = _encode_binary(serialized_value)
            
            doc = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "task_id": task_id,
                "idx": idx,
                "channel": channel,
                "type": type_,
                "value": serialized_value,
            }
            await collection.upsert(upsert_key, (doc), UpsertOptions(timeout=timedelta(seconds=5)))