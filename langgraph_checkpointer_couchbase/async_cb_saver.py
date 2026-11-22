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
from couchbase.n1ql import QueryScanConsistency

from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)

from . import _documents as documents
from ._documents import DEFAULT_METADATA_TYPE  # noqa: F401  (kept importable from this module)

logger = logging.getLogger(__name__)

# Default timeout for database operations
DEFAULT_TIMEOUT = timedelta(seconds=5)

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

    def _query(self, statement: str, params: list):
        return self.cluster.query(
            statement,
            QueryOptions(positional_parameters=params, scan_consistency=QueryScanConsistency.REQUEST_PLUS),
        )

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
        async for row in self._query(*documents.get_checkpoint_query(
            config, self.bucket_name, self.scope_name, self.checkpoints_collection_name
        )):
            doc = row[self.checkpoints_collection_name]

            pending_writes = []
            async for write_row in self._query(*documents.pending_writes_query(
                doc, self.bucket_name, self.scope_name, self.checkpoint_writes_collection_name
            )):
                write = documents.pending_write_from_document(
                    self.serde, write_row.get(self.checkpoint_writes_collection_name, {})
                )
                if write is None:
                    logger.warning("'task_id' is not present in checkpoint_writes")
                else:
                    pending_writes.append(write)

            return documents.checkpoint_tuple_from_document(self.serde, doc, pending_writes)

        return None

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
        async for row in self._query(*documents.list_checkpoints_query(
            config, self.bucket_name, self.scope_name, self.checkpoints_collection_name,
            filter=filter, before=before, limit=limit,
        )):
            yield documents.checkpoint_tuple_from_document(self.serde, row[self.checkpoints_collection_name])

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
        key, doc, next_config = documents.checkpoint_document(self.serde, config, checkpoint, metadata)

        # ensure bucket connected (idempotent)
        await self.bucket.on_connect()
        collection = self.bucket.scope(self.scope_name).collection(self.checkpoints_collection_name)
        await collection.upsert(key, doc, UpsertOptions(timeout=DEFAULT_TIMEOUT))
        return next_config

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
        await self.bucket.on_connect()
        collection = self.bucket.scope(self.scope_name).collection(self.checkpoint_writes_collection_name)

        for key, doc in documents.write_documents(self.serde, config, writes, task_id):
            await collection.upsert(key, doc, UpsertOptions(timeout=DEFAULT_TIMEOUT))
