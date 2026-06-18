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

from syncd.sync.base import SyncContext, ProviderStatus
from syncd.sync.file_index import FileEntry
from syncd.sync.sync_engine import Action, reconcile
from syncd.sync import protocol as proto
from syncd.sync.providers._common import BaseSyncProvider, _close_writer

logger = logging.getLogger(__name__)


class ClientServerProvider(BaseSyncProvider):
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
            "verify_interval": {
                "type": "integer",
                "description": "Seconds between integrity verify passes. 0 or absent = disabled.",
            },
            "verify_sleep": {
                "type": "number",
                "description": "Seconds between per-file hashes during a verify pass. Default 0.1.",
            },
        },
        "required": ["mode", "port"],
        "additionalProperties": False,
    }

    async def start(self, context: SyncContext) -> None:
        self._context = context
        cfg = context.provider_config
        self._node_id = cfg.get("node_id") or str(uuid.uuid4())[:8]
        self._conflict_strategy = cfg.get("conflict_strategy", "last-write-wins")

        await self._init_index(context)

        self._state = "idle"
        self._watch_task = asyncio.create_task(
            self._watch_loop(), name=f"cs-watch-{context.pair_id}"
        )
        self._start_verify_if_configured(context)
        if cfg["mode"] == "server":
            bind_host = cfg.get("host", "0.0.0.0")
            port = cfg["port"]
            self._server = await asyncio.start_server(
                self._handle_client, bind_host, port,
                limit=proto.READER_LIMIT,
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
            await proto.send_message(writer, proto.hello_msg(self._node_id, self._index.all_entries_dict()))

            msg = await proto.read_message(reader)
            if not msg or msg["type"] != "HELLO":
                return
            client_index = {p: FileEntry.from_dict(e) for p, e in msg.get("index", {}).items()}
            await self._apply_remote_deletions(client_index, f"client {peer}")

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
                    content = proto.decode_content(req.get("content", ""))
                    await self._apply_incoming_file(FileEntry.from_dict(req["entry"]), content, f"client {peer}")
                elif req["type"] == "FILE_DATA_STREAM":
                    entry = FileEntry.from_dict(req["entry"])
                    await self._apply_incoming_stream(reader, entry, req["size"], f"client {peer}")
                elif req["type"] == "RENAME_FILE":
                    await self._apply_rename(req["from"], req["to"],
                                             FileEntry.from_dict(req["entry"]), f"client {peer}")

            await asyncio.to_thread(self._index.save)
            self._last_sync = time.time()
        except Exception:
            logger.exception("Error handling client %s", peer)
        finally:
            await _close_writer(writer)

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
            reader, writer = await asyncio.open_connection(host, port, limit=proto.READER_LIMIT)
            try:
                await proto.send_message(writer, proto.hello_msg(self._node_id, self._index.all_entries_dict()))

                msg = await proto.read_message(reader)
                if not msg or msg["type"] != "HELLO":
                    return
                server_index = {
                    p: FileEntry.from_dict(e) for p, e in msg.get("index", {}).items()
                }

                await self._apply_remote_deletions(server_index, "server")

                # Phase 1: pull files client needs from server
                for path in self._compute_needed(server_index):
                    await proto.send_message(writer, proto.get_file_msg(path))
                    resp = await proto.read_message(reader)
                    if resp and resp["type"] in ("FILE_DATA", "FILE_DATA_STREAM"):
                        entry = FileEntry.from_dict(resp["entry"])
                        if resp["type"] == "FILE_DATA_STREAM":
                            abs_path = os.path.join(ctx.local, entry.path)
                            await proto.recv_stream_to_disk(reader, resp["size"], abs_path)
                            await asyncio.to_thread(self._index.apply_remote, entry, None)
                        else:
                            content = proto.decode_content(resp.get("content", ""))
                            await asyncio.to_thread(self._index.apply_remote, entry, content)
                        logger.info("Pulled %r from server", path)

                await proto.send_message(writer, proto.sync_done_msg())

                # Phase 2: push files server is missing or behind on
                server_live_by_checksum = {
                    e.checksum: p
                    for p, e in server_index.items()
                    if not e.deleted and e.checksum
                }
                renames: dict[str, str] = {}
                for path, entry in self._index.all_entries().items():
                    if entry.deleted or not entry.checksum:
                        continue
                    old = server_live_by_checksum.get(entry.checksum)
                    if old and old != path:
                        our_old = self._index.get(old)
                        if our_old and our_old.deleted:
                            renames[path] = old

                for path, local_entry in self._index.all_entries().items():
                    if local_entry.deleted:
                        continue
                    server_entry = server_index.get(path)
                    if (server_entry is None
                            or server_entry.deleted
                            or reconcile(server_entry, local_entry, self._node_id) == Action.ACCEPT_REMOTE):
                        if path in renames:
                            await proto.send_message(writer, proto.rename_msg(renames[path], path, local_entry.to_dict()))
                            logger.info("Sent rename %r → %r to server", renames[path], path)
                        else:
                            abs_path = os.path.join(ctx.local, path)
                            if os.path.exists(abs_path):
                                await proto.send_file_data(writer, path, abs_path, local_entry.to_dict())
                                logger.info("Pushed %r to server", path)

                await proto.send_message(writer, proto.ack_msg(self._node_id))
                await asyncio.to_thread(self._index.save)
            finally:
                await _close_writer(writer)

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
