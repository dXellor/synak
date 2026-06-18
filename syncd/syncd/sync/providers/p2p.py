"""
P2P sync provider.

Each node connects to every peer in its list and pulls files it is missing
or behind on (gossip-style). The connecting node is always the initiator;
the listening node serves requests. Both sides connect to each other on
their own sync cycles so the propagation is eventually bidirectional.

Academic grounding:
  - Vector clocks (Parker et al. 1983) for causal ordering
  - Bayou-style gossip (Terry et al. 1995) for propagation
  - Dirty-set tracking (Ramsey & Csirmaz 2001)
  - CRDT idempotence properties (Shapiro et al. 2011)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
import uuid
from typing import Any

from syncd.sync.base import SyncContext, ProviderStatus
from syncd.sync.file_index import FileEntry
from syncd.sync.sync_engine import Action, reconcile
from syncd.sync import protocol as proto
from syncd.sync.providers._common import BaseSyncProvider, _close_writer

logger = logging.getLogger(__name__)


def _port_for_pair(pair_id: str) -> int:
    """Derive a stable port in 30000-65535 from a pair id."""
    return 30000 + (int(hashlib.sha256(pair_id.encode()).hexdigest(), 16) % 35536)


def _parse_peer(peer: str, default_port: int) -> tuple[str, int]:
    """Parse 'host' or 'host:port'. Bare hostnames use default_port."""
    if ":" in peer:
        host, port_str = peer.rsplit(":", 1)
        return host, int(port_str)
    return peer, default_port


class P2PProvider(BaseSyncProvider):
    NAME = "p2p"
    SCHEMA: dict[str, Any] = {
        "type": "object",
        "properties": {
            "peers": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Peer addresses as 'host' or 'host:port'. Port defaults to the pair-derived port.",
            },
            "port": {
                "type": "integer",
                "description": "Explicit listen port. Overrides the pair-derived port.",
            },
            "node_id": {"type": "string"},
            "conflict_strategy": {
                "type": "string",
                "enum": ["last-write-wins", "keep-both"],
            },
            "sync_deletes": {
                "type": "boolean",
                "description": "Whether to propagate remote deletions. Default true.",
            },
            "verify_interval": {
                "type": "integer",
                "description": "Seconds between integrity verify passes. 0 or absent = disabled.",
            },
            "verify_sleep": {
                "type": "number",
                "description": "Seconds between per-file hashes during a verify pass. Default 0.1.",
            },
        },
        "required": ["peers"],
        "additionalProperties": False,
    }

    def __init__(self) -> None:
        super().__init__()
        self._port: int = 0

    async def start(self, context: SyncContext) -> None:
        self._context = context
        cfg = context.provider_config
        self._node_id = cfg.get("node_id") or str(uuid.uuid4())[:8]
        self._conflict_strategy = cfg.get("conflict_strategy", "last-write-wins")

        await self._init_index(context)

        self._port = cfg.get("port") or _port_for_pair(context.pair_id)
        self._server = await asyncio.start_server(
            self._handle_connection, "0.0.0.0", self._port,
            limit=proto.READER_LIMIT,
        )
        self._state = "idle"
        self._task = asyncio.create_task(
            self._sync_loop(), name=f"p2p-{context.pair_id}"
        )
        self._watch_task = asyncio.create_task(
            self._watch_loop(), name=f"p2p-watch-{context.pair_id}"
        )
        self._start_verify_if_configured(context)
        logger.info("P2P node %r listening on port %d", self._node_id, self._port)

    async def trigger(self) -> None:
        asyncio.create_task(self._run_sync_round())

    async def status(self) -> ProviderStatus:
        return ProviderStatus(
            pair_id=self._context.pair_id if self._context else "",
            state=self._state,
            last_sync=self._last_sync,
            error=self._error,
            extra={"node_id": self._node_id},
        )

    # --- sync loop ---

    async def _sync_loop(self) -> None:
        ctx = self._context
        assert ctx is not None
        interval = ctx.interval if ctx.interval > 0 else 30
        while True:
            await self._paused.wait()
            await self._run_sync_round()
            await asyncio.sleep(interval)

    async def _run_sync_round(self) -> None:
        ctx = self._context
        assert ctx is not None and self._index is not None
        self._state = "syncing"
        self._error = ""
        try:
            for peer_addr in ctx.provider_config.get("peers", []):
                try:
                    await self._sync_with_peer(peer_addr)
                except ConnectionRefusedError:
                    logger.warning("Peer %s unreachable", peer_addr)
                except Exception:
                    logger.exception("Sync with peer %s failed", peer_addr)
            self._last_sync = time.time()
            self._state = "idle"
        except asyncio.CancelledError:
            self._state = "stopped"
            raise
        except Exception as e:
            self._error = str(e)
            self._state = "error"
            logger.exception("P2P sync round failed for pair %r", ctx.pair_id)

    # --- initiator side (connecting to a peer) ---

    async def _sync_with_peer(self, peer_addr: str) -> None:
        assert self._index is not None and self._context is not None
        host, port = _parse_peer(peer_addr, self._port)
        reader, writer = await asyncio.open_connection(host, port, limit=proto.READER_LIMIT)
        try:
            await proto.send_message(writer, proto.hello_msg(self._node_id, self._index.all_entries_dict()))

            msg = await proto.read_message(reader)
            if not msg or msg["type"] != "HELLO":
                return
            remote_index = {p: FileEntry.from_dict(e) for p, e in msg.get("index", {}).items()}

            await self._apply_remote_deletions(remote_index, peer_addr)

            # Phase 1: pull files we need from peer
            for path in self._compute_needed(remote_index):
                await proto.send_message(writer, proto.get_file_msg(path))
                resp = await proto.read_message(reader)
                if resp and resp["type"] in ("FILE_DATA", "FILE_DATA_STREAM"):
                    entry = FileEntry.from_dict(resp["entry"])
                    if resp["type"] == "FILE_DATA_STREAM":
                        abs_path = os.path.join(self._context.local, entry.path)
                        await proto.recv_stream_to_disk(reader, resp["size"], abs_path)
                        await asyncio.to_thread(self._index.apply_remote, entry, None)
                    else:
                        content = proto.decode_content(resp.get("content", ""))
                        await asyncio.to_thread(self._index.apply_remote, entry, content)
                    logger.info("Pulled %r from peer %s", path, peer_addr)

            await proto.send_message(writer, proto.sync_done_msg())

            # Phase 2: push files peer is missing or behind on
            remote_live_by_checksum = {
                e.checksum: p
                for p, e in remote_index.items()
                if not e.deleted and e.checksum
            }
            renames: dict[str, str] = {}
            for path, entry in self._index.all_entries().items():
                if entry.deleted or not entry.checksum:
                    continue
                old = remote_live_by_checksum.get(entry.checksum)
                if old and old != path:
                    our_old = self._index.get(old)
                    if our_old and our_old.deleted:
                        renames[path] = old

            for path, local_entry in self._index.all_entries().items():
                if local_entry.deleted:
                    continue
                remote_entry = remote_index.get(path)
                if (remote_entry is None
                        or remote_entry.deleted
                        or reconcile(remote_entry, local_entry, self._node_id) == Action.ACCEPT_REMOTE):
                    if path in renames:
                        await proto.send_message(writer, proto.rename_msg(renames[path], path, local_entry.to_dict()))
                    else:
                        abs_path = os.path.join(self._context.local, path)
                        if os.path.exists(abs_path):
                            await proto.send_file_data(writer, path, abs_path, local_entry.to_dict())

            await proto.send_message(writer, proto.ack_msg(self._node_id))
            await asyncio.to_thread(self._index.save)
        finally:
            await _close_writer(writer)

    # --- listener side (accepting connections from peers) ---

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        assert self._index is not None and self._context is not None
        peer = writer.get_extra_info("peername")
        try:
            msg = await proto.read_message(reader)
            if not msg or msg["type"] != "HELLO":
                return
            peer_index = {p: FileEntry.from_dict(e) for p, e in msg.get("index", {}).items()}

            await proto.send_message(writer, proto.hello_msg(self._node_id, self._index.all_entries_dict()))
            await self._apply_remote_deletions(peer_index, str(peer))

            # Phase 1: serve GET_FILE requests until SYNC_DONE
            while True:
                req = await proto.read_message(reader)
                if not req:
                    return
                if req["type"] == "SYNC_DONE":
                    break
                if req["type"] == "GET_FILE":
                    await self._serve_file(writer, req["path"])

            # Phase 2: accept FILE_DATA pushes until ACK
            while True:
                req = await proto.read_message(reader)
                if not req or req["type"] == "ACK":
                    break
                if req["type"] == "FILE_DATA":
                    content = proto.decode_content(req.get("content", ""))
                    await self._apply_incoming_file(FileEntry.from_dict(req["entry"]), content, str(peer))
                elif req["type"] == "FILE_DATA_STREAM":
                    entry = FileEntry.from_dict(req["entry"])
                    await self._apply_incoming_stream(reader, entry, req["size"], str(peer))
                elif req["type"] == "RENAME_FILE":
                    await self._apply_rename(req["from"], req["to"],
                                             FileEntry.from_dict(req["entry"]), str(peer))

            await asyncio.to_thread(self._index.save)
        except Exception:
            logger.exception("Error handling peer connection from %s", peer)
        finally:
            await _close_writer(writer)
