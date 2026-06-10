"""
example feed transport (jsonl-tail) — Cell Architecture v1.

One feed-handler process per SIM account owns ALL TradeStation SSE bar
subscriptions for that account's fleet and appends every bar event VERBATIM to
per-key JSONL segment files under ``%LOCALAPPDATA%/example/feed/<account>/``. The
JSONL audit mirror IS the transport (replayable, debuggable; no sockets, no
server): cells poll-tail the files via ``FeedTailStreamClient``, a drop-in
replacement for ``TradeStationStreamClient`` on the bar-data path.

Modules
-------
- ``keys``        stream-key encoding (single shared source, writer + reader)
- ``protocol``    on-disk wire protocol (line format, torn-line rules, atomic IO)
- ``mirror``      ``MirrorWriter`` — handler-side segment/manifest writer
- ``tail_client`` ``FeedTailStreamClient`` — cell-side tailer + feed exceptions
- ``handler``     ``FeedHandler`` — per-account SSE ingest + publish
"""

from nautilus_tradestation.feed.handler import FeedHandler
from nautilus_tradestation.feed.keys import stream_key
from nautilus_tradestation.feed.mirror import MirrorWriter
from nautilus_tradestation.feed.tail_client import (
    FeedGapError,
    FeedHandlerDeadError,
    FeedProxyError,
    FeedSubscribeTimeoutError,
    FeedTailStreamClient,
)

__all__ = [
    "FeedGapError",
    "FeedHandler",
    "FeedHandlerDeadError",
    "FeedProxyError",
    "FeedSubscribeTimeoutError",
    "FeedTailStreamClient",
    "MirrorWriter",
    "stream_key",
]
