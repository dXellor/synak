"""
Client-server sync provider.

Server mode: listens for client connections, reconciles and exchanges files
             with each connected client.
Client mode: connects to the server on each sync cycle, pulls what it needs,
             then pushes files the server is missing.

Uses the same vector-clock reconciliation and conflict strategies as the
P2P provider. The only difference is topology: all sync goes through the
designated server node rather than directly between peers.
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


class ClientServerProvider(SyncProvider):
    NAME = "client-server"
    SCHEMA: dict[str, Any] = {
        "type": "object",
        "properties": {
            "mode": {
                "type": "string",
                "enum": ["server", "client"],
                "description": "'server' listens; 'client' connects",
            },
            "host": {
                "type": "string",
                "description": "Bind address (server) or server hostname (client)",
            },
            "port": {"type": "integer"},
            "node_id": {"type": "string"},
            "conflict_strategy": {
                "type": "string",
                "enum": ["last-write-wins", "keep-both"],
            },
            "sync_deletes": {
                "type": "boolean",
                "description": "Whether to propagate remote deletions. Default true.",
            },
        },
        "required": ["mode", "port"],
        "additionalProperties": False,
    }

    def __init__(self) -> None:
        self._context: SyncContext | None = None
        self._state = "stopped"
        self._last_sync: float = 0.0
        self._error: str = ""
        self._task: asyncio.Task | None = None
        self._watch_task: asyncio.Task | None = None
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

        self._index = FileIndex(context.local, self._node_id, extra_excludes=context.exclude)
        await asyncio.to_thread(self._index.load)
        await asyncio.to_thread(self._index.scan)
        await asyncio.to_thread(self._index.save)

        self._state = "idle"
        self._watch_task = asyncio.create_task(
            self._watch_loop(), name=f"cs-watch-{context.pair_id}"
        )
        if cfg["mode"] == "server":
            bind_host = cfg.get("host", "0.0.0.0")
            port = cfg["port"]
            self._server = await asyncio.start_server(
                self._handle_client, bind_host, port
            )
            self._task = asyncio.create_task(
                self._server_loop(), name=f"cs-server-{context.pair_id}"
            )
            logger.info(
                "Client-server node %r (server) listening on %s:%d",
                self._node_id, bind_host, port,
            )
        else:
            self._task = asyncio.create_task(
                self._client_loop(), name=f"cs-client-{context.pair_id}"
            )
            logger.info("Client-server node %r (client) started", self._node_id)

    async def stop(self) -> None:
        self._state = "stopped"
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for task in (self._task, self._watch_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._task = None
        self._watch_task = None

    async def pause(self) -> None:
        self._paused.clear()
        self._state = "paused"

    async def resume(self) -> None:
        self._paused.set()
        self._state = "idle"

    async def trigger(self) -> None:
        cfg = self._context.provider_config if self._context else {}
        if cfg.get("mode") == "client":
            asyncio.create_task(self._run_client_session())

    async def status(self) -> ProviderStatus:
        mode = self._context.provider_config.get("mode", "") if self._context else ""
        return ProviderStatus(
            pair_id=self._context.pair_id if self._context else "",
            state=self._state,
            last_sync=self._last_sync,
            error=self._error,
            extra={"node_id": self._node_id, "mode": mode},
        )

    # --- watch ---

    async def _watch_loop(self) -> None:
        from watchfiles import awatch, Change
        from syncd.sync.file_index import METADATA_DIR
        assert self._context is not None and self._index is not None
        watch_dir = self._context.local
        meta_prefix = os.path.join(watch_dir, METADATA_DIR) + os.sep
        async for changes in awatch(watch_dir):
            if self._index is None:
                break
            dirty = False
            for change_type, path in changes:
                if path.startswith(meta_prefix):
                    continue
                rel = os.path.relpath(path, watch_dir)
                if change_type == Change.deleted:
                    dirty |= self._index.mark_deleted(rel)
                else:
                    dirty |= self._index.scan_one(rel)
            if dirty:
                await asyncio.to_thread(self._index.save)
                logger.debug("Watch: index updated for pair %r", self._context.pair_id)

    # --- server side ---

    async def _server_loop(self) -> None:
        assert self._server is not None
        try:
            async with self._server:
                await self._server.serve_forever()
        except asyncio.CancelledError:
            pass

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        assert self._index is not None and self._context is not None
        peer = writer.get_extra_info("peername")
        logger.info("Client connected: %s", peer)
        try:
            index_dict = {p: e.to_dict() for p, e in self._index.all_entries().items()}
            await proto.send_message(writer, proto.hello_msg(self._node_id, index_dict))

            msg = await proto.read_message(reader)
            if not msg or msg["type"] != "HELLO":
                return
            client_index = {
                p: FileEntry.from_dict(e) for p, e in msg.get("index", {}).items()
            }

            # Phase 1: serve GET_FILE requests from client until SYNC_DONE
            while True:
                req = await proto.read_message(reader)
                if not req:
                    return
                if req["type"] == "SYNC_DONE":
                    break
                if req["type"] == "GET_FILE":
                    await self._serve_file(writer, req["path"])

            # Phase 2: accept FILE_DATA pushes from client until ACK
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
                        logger.info("Server accepted %r from client %s", entry.path, peer)
                    elif action == Action.CONFLICT:
                        resolved = self._resolve_conflict(local_entry, entry, entry.path)
                        if resolved == Action.ACCEPT_REMOTE:
                            await asyncio.to_thread(self._index.apply_remote, entry, content)

            await asyncio.to_thread(self._index.save)
            self._last_sync = time.time()
        except Exception:
            logger.exception("Error handling client %s", peer)
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

    # --- client side ---

    async def _client_loop(self) -> None:
        ctx = self._context
        assert ctx is not None
        interval = ctx.interval if ctx.interval > 0 else 30
        while True:
            await self._paused.wait()
            await self._run_client_session()
            await asyncio.sleep(interval)

    async def _run_client_session(self) -> None:
        ctx = self._context
        assert ctx is not None and self._index is not None
        self._state = "syncing"
        self._error = ""
        try:
            host = ctx.provider_config.get("host", "127.0.0.1")
            port = ctx.provider_config["port"]
            reader, writer = await asyncio.open_connection(host, port)
            try:
                index_dict = {p: e.to_dict() for p, e in self._index.all_entries().items()}
                await proto.send_message(writer, proto.hello_msg(self._node_id, index_dict))

                msg = await proto.read_message(reader)
                if not msg or msg["type"] != "HELLO":
                    return
                server_index = {
                    p: FileEntry.from_dict(e) for p, e in msg.get("index", {}).items()
                }

                # Phase 1a: apply remote deletions (no network round-trip needed)
                if ctx.provider_config.get("sync_deletes", True):
                    for path, remote_entry in server_index.items():
                        if not remote_entry.deleted:
                            continue
                        local_entry = self._index.get(path)
                        if local_entry is None or local_entry.deleted:
                            continue
                        action = reconcile(local_entry, remote_entry, self._node_id)
                        if action == Action.ACCEPT_REMOTE:
                            await asyncio.to_thread(self._index.apply_remote, remote_entry, None)
                            logger.info("Client applied remote deletion of %r", path)

                # Phase 1b: pull files client needs from server
                needed = self._compute_needed(server_index)
                for path in needed:
                    await proto.send_message(writer, proto.get_file_msg(path))
                    resp = await proto.read_message(reader)
                    if resp and resp["type"] == "FILE_DATA":
                        content = proto.decode_content(resp["content"])
                        entry = FileEntry.from_dict(resp["entry"])
                        await asyncio.to_thread(self._index.apply_remote, entry, content)
                        logger.info("Client pulled %r from server", path)

                await proto.send_message(writer, proto.sync_done_msg())

                # Phase 2: push files server is missing or behind on
                for path, local_entry in self._index.all_entries().items():
                    if local_entry.deleted:
                        continue
                    server_entry = server_index.get(path)
                    if server_entry is None or (
                        not server_entry.deleted
                        and reconcile(server_entry, local_entry, self._node_id)
                        == Action.ACCEPT_REMOTE
                    ):
                        abs_path = os.path.join(ctx.local, path)
                        if os.path.exists(abs_path):
                            with open(abs_path, "rb") as f:
                                content = f.read()
                            await proto.send_message(
                                writer,
                                proto.file_data_msg(path, content, local_entry.to_dict()),
                            )
                            logger.info("Client pushed %r to server", path)

                await proto.send_message(writer, proto.ack_msg(self._node_id))
                await asyncio.to_thread(self._index.save)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

            self._last_sync = time.time()
            self._state = "idle"
        except asyncio.CancelledError:
            self._state = "stopped"
            raise
        except ConnectionRefusedError:
            self._error = f"Cannot connect to server at {ctx.provider_config.get('host', '127.0.0.1')}:{ctx.provider_config['port']}"
            self._state = "error"
            logger.warning("Client cannot reach server for pair %r", ctx.pair_id)
        except Exception as e:
            self._error = str(e)
            self._state = "error"
            logger.exception("Client session failed for pair %r", ctx.pair_id)

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
