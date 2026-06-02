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
import logging
import os
import time
import uuid
from typing import Any

from syncd.sync.base import SyncContext, SyncProvider, ProviderStatus
from syncd.sync.file_index import FileEntry, FileIndex
from syncd.sync.sync_engine import Action, reconcile, resolve_last_write_wins, resolve_keep_both
from syncd.sync import protocol as proto

logger = logging.getLogger(__name__)


class P2PProvider(SyncProvider):
    NAME = "p2p"
    SCHEMA: dict[str, Any] = {
        "type": "object",
        "properties": {
            "peers": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of peer addresses as host:port",
            },
            "port": {"type": "integer", "description": "TCP port this node listens on"},
            "node_id": {"type": "string"},
            "conflict_strategy": {
                "type": "string",
                "enum": ["last-write-wins", "keep-both"],
            },
        },
        "required": ["peers", "port"],
        "additionalProperties": False,
    }

    def __init__(self) -> None:
        self._context: SyncContext | None = None
        self._state = "stopped"
        self._last_sync: float = 0.0
        self._error: str = ""
        self._task: asyncio.Task | None = None
        self._server: asyncio.Server | None = None
        self._paused = asyncio.Event()
        self._paused.set()
        self._index: FileIndex | None = None
        self._node_id: str = ""
        self._conflict_strategy: str = "last-write-wins"

    async def start(self, context: SyncContext) -> None:
        self._context = context
        cfg = context.provider_config
        self._node_id = cfg.get("node_id") or str(uuid.uuid4())[:8]
        self._conflict_strategy = cfg.get("conflict_strategy", "last-write-wins")

        self._index = FileIndex(context.local, self._node_id)
        await asyncio.to_thread(self._index.load)
        await asyncio.to_thread(self._index.scan)
        await asyncio.to_thread(self._index.save)

        port: int = cfg["port"]
        self._server = await asyncio.start_server(
            self._handle_connection, "0.0.0.0", port
        )
        self._state = "idle"
        self._task = asyncio.create_task(
            self._sync_loop(), name=f"p2p-{context.pair_id}"
        )
        logger.info("P2P node %r listening on port %d", self._node_id, port)

    async def stop(self) -> None:
        self._state = "stopped"
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def pause(self) -> None:
        self._paused.clear()
        self._state = "paused"

    async def resume(self) -> None:
        self._paused.set()
        self._state = "idle"

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
            dirty = await asyncio.to_thread(self._index.scan)
            if dirty:
                await asyncio.to_thread(self._index.save)
                logger.debug("Dirty set for pair %r: %s", ctx.pair_id, dirty)

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
        assert self._index is not None
        host, port_str = peer_addr.rsplit(":", 1)
        reader, writer = await asyncio.open_connection(host, int(port_str))
        try:
            index_dict = {p: e.to_dict() for p, e in self._index.all_entries().items()}
            await proto.send_message(writer, proto.hello_msg(self._node_id, index_dict))

            msg = await proto.read_message(reader)
            if not msg or msg["type"] != "HELLO":
                return
            remote_index = {
                p: FileEntry.from_dict(e) for p, e in msg.get("index", {}).items()
            }

            # Phase 1: pull files we need from peer
            needed = self._compute_needed(remote_index)
            for path in needed:
                await proto.send_message(writer, proto.get_file_msg(path))
                resp = await proto.read_message(reader)
                if resp and resp["type"] == "FILE_DATA":
                    content = proto.decode_content(resp["content"])
                    entry = FileEntry.from_dict(resp["entry"])
                    await asyncio.to_thread(self._index.apply_remote, entry, content)
                    logger.info("Pulled %r from peer %s", path, peer_addr)

            await proto.send_message(writer, proto.sync_done_msg())

            # Phase 2: push files peer is missing or behind on
            for path, local_entry in self._index.all_entries().items():
                if local_entry.deleted:
                    continue
                remote_entry = remote_index.get(path)
                should_push = (
                    remote_entry is None
                    or remote_entry.deleted
                    or local_entry.get_clock(self._node_id).happens_before(
                        remote_entry.get_clock(self._node_id)
                    ) is False
                    and reconcile(remote_entry, local_entry, self._node_id) == Action.ACCEPT_REMOTE
                )
                if should_push:
                    abs_path = os.path.join(self._context.local, path)
                    if os.path.exists(abs_path):
                        with open(abs_path, "rb") as f:
                            content = f.read()
                        await proto.send_message(
                            writer,
                            proto.file_data_msg(path, content, local_entry.to_dict()),
                        )

            await proto.send_message(writer, proto.ack_msg(self._node_id))
            await asyncio.to_thread(self._index.save)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    def _compute_needed(self, remote_index: dict[str, FileEntry]) -> list[str]:
        assert self._index is not None
        needed = []
        for path, remote_entry in remote_index.items():
            if remote_entry.deleted:
                continue
            local_entry = self._index.get(path)
            if local_entry is None or local_entry.deleted:
                needed.append(path)
                continue
            action = reconcile(local_entry, remote_entry, self._node_id)
            if action == Action.ACCEPT_REMOTE:
                needed.append(path)
            elif action == Action.CONFLICT:
                resolved = self._resolve_conflict(local_entry, remote_entry, path)
                if resolved == Action.ACCEPT_REMOTE:
                    needed.append(path)
        return needed

    def _resolve_conflict(
        self, local: FileEntry, remote: FileEntry, path: str
    ) -> Action:
        logger.warning(
            "Conflict on %r for pair %r — strategy: %r",
            path, self._context.pair_id if self._context else "?", self._conflict_strategy,
        )
        if self._conflict_strategy == "keep-both":
            assert self._context is not None
            return resolve_keep_both(local, remote, self._node_id, self._context.local)
        return resolve_last_write_wins(local, remote)

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

            index_dict = {p: e.to_dict() for p, e in self._index.all_entries().items()}
            await proto.send_message(writer, proto.hello_msg(self._node_id, index_dict))

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
                    content = proto.decode_content(req["content"])
                    entry = FileEntry.from_dict(req["entry"])
                    local_entry = self._index.get(entry.path)
                    action = (
                        Action.ACCEPT_REMOTE
                        if local_entry is None
                        else reconcile(local_entry, entry, self._node_id)
                    )
                    if action == Action.ACCEPT_REMOTE:
                        await asyncio.to_thread(self._index.apply_remote, entry, content)
                        logger.info("Accepted push of %r from peer %s", entry.path, peer)
                    elif action == Action.CONFLICT:
                        resolved = self._resolve_conflict(local_entry, entry, entry.path)
                        if resolved == Action.ACCEPT_REMOTE:
                            await asyncio.to_thread(self._index.apply_remote, entry, content)

            await asyncio.to_thread(self._index.save)
        except Exception:
            logger.exception("Error handling peer connection from %s", peer)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _serve_file(self, writer: asyncio.StreamWriter, path: str) -> None:
        assert self._index is not None and self._context is not None
        entry = self._index.get(path)
        if entry is None or entry.deleted:
            return
        abs_path = os.path.join(self._context.local, path)
        try:
            with open(abs_path, "rb") as f:
                content = f.read()
            await proto.send_message(
                writer, proto.file_data_msg(path, content, entry.to_dict())
            )
        except OSError:
            pass
