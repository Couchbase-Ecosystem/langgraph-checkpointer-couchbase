import sys

if sys.version_info < (3, 10):
    raise ImportError(
        "langgraph-checkpointer-couchbase 2.x requires Python 3.10 or newer "
        f"(found {sys.version_info[0]}.{sys.version_info[1]}). "
        "Upgrade Python, or pin langgraph-checkpointer-couchbase<2 to stay on an older release."
    )

from .async_cb_saver import AsyncCouchbaseSaver
from .couchbase_saver import CouchbaseSaver

__all__ = ["CouchbaseSaver", "AsyncCouchbaseSaver"]

# --- Package-level telemetry (non-blocking, fire-and-forget) ---
import langgraph_checkpointer_couchbase.telemetry  # noqa: F401, E402