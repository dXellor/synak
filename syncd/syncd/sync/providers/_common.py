"""Shared base class and helpers for built-in sync providers."""

from __future__ import annotations

import asyncio
import logging
import os

from watchfiles import awatch, Change

from syncd.sync.base import SyncContext, SyncProvider
from syncd.sync.file_index import FileEntry, FileIndex, METADATA_DIR
from syncd.sync.sync_engine import Action, reconcile, resolve_last_write_wins, resolve_keep_both
from syncd.sync import protocol as proto

logger = logging.getLogger(__name__)


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


class BaseSyncProvider(SyncProvider):
    """Common fields and shared logic for all built-in sync providers."""

    def __init__(self) -> None:
        self._context: SyncContext | None = None
        self._state = "stopped"
        self._last_sync: float = 0.0
        self._error: str = ""
        self._task: asyncio.Task | None = None
        self._watch_task: asyncio.Task | None = None
        self._verify_task: asyncio.Task | None = None
        self._server: asyncio.Server | None = None
        self._paused = asyncio.Event()
        self._paused.set()
        self._index: FileIndex | None = None
        self._node_id: str = ""
        self._conflict_strategy: str = "keep-both"
        # Paths seen as remotely deleted once but not yet confirmed — deferred one round
        # to avoid deleting files mid-move when the watcher hasn't indexed the new path yet.
        self._pending_deletes: set[str] = set()

    async def _init_index(self, context: SyncContext) -> None:
        self._index = FileIndex(context.local, self._node_id, extra_excludes=context.exclude)
        await asyncio.to_thread(self._index.load)
        await asyncio.to_thread(self._index.scan)
        await asyncio.to_thread(self._index.save)

    async def stop(self) -> None:
        self._state = "stopped"
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for task in (self._task, self._watch_task, self._verify_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._task = None
        self._watch_task = None
        self._verify_task = None

    async def pause(self) -> None:
        self._paused.clear()
        self._state = "paused"

    async def resume(self) -> None:
        self._paused.set()
        self._state = "idle"

    async def _watch_loop(self) -> None:
        assert self._context is not None and self._index is not None
        watch_dir = self._context.local
        meta_prefix = os.path.join(watch_dir, METADATA_DIR) + os.sep
        async for changes in awatch(watch_dir, watch_filter=lambda _c, p: not p.startswith(meta_prefix)):
            if self._index is None:
                break
            dirty = False
            for change_type, path in changes:
                rel = os.path.relpath(path, watch_dir)
                if change_type == Change.deleted:
                    if not self._index.mark_deleted(rel):
                        dirty |= self._index.mark_dir_deleted(rel)
                    else:
                        dirty = True
                elif os.path.isdir(path):
                    for dirpath, _, filenames in os.walk(path):
                        for fname in filenames:
                            frel = os.path.relpath(os.path.join(dirpath, fname), watch_dir)
                            dirty |= self._index.scan_one(frel)
                else:
                    dirty |= self._index.scan_one(rel)
            if dirty:
                await asyncio.to_thread(self._index.save)
                logger.debug("Watch: index updated for pair %r", self._context.pair_id)

    def _compute_needed(self, remote_index: dict[str, FileEntry]) -> list[str]:
        assert self._index is not None
        local_live_by_checksum = {
            e.checksum: p
            for p, e in self._index.all_entries().items()
            if not e.deleted and e.checksum
        }
        needed = []
        for path, remote_entry in remote_index.items():
            if remote_entry.deleted:
                continue
            local_entry = self._index.get(path)
            if local_entry is None or local_entry.deleted:
                existing = local_live_by_checksum.get(remote_entry.checksum)
                if existing and existing != path:
                    continue  # we have this content under a different name — it's a rename
                if local_entry is None:
                    needed.append(path)
                elif reconcile(local_entry, remote_entry, self._node_id) == Action.ACCEPT_REMOTE:
                    needed.append(path)
                continue
            action = reconcile(local_entry, remote_entry, self._node_id)
            if action == Action.ACCEPT_REMOTE:
                needed.append(path)
            elif action == Action.CONFLICT:
                if self._resolve_conflict(local_entry, remote_entry, path) == Action.ACCEPT_REMOTE:
                    needed.append(path)
            elif action == Action.KEEP_LOCAL:
                if self._index.is_corrupted(path):
                    needed.append(path)
        return needed

    def _resolve_conflict(self, local: FileEntry, remote: FileEntry, path: str) -> Action:
        if local.checksum == remote.checksum:
            return Action.KEEP_LOCAL
        logger.warning(
            "Conflict on %r for pair %r — strategy: %r",
            path, self._context.pair_id if self._context else "?", self._conflict_strategy,
        )
        if self._conflict_strategy == "keep-both":
            assert self._context is not None
            return resolve_keep_both(local, remote, self._node_id, self._context.local)
        return resolve_last_write_wins(local, remote)

    async def _apply_incoming_stream(
        self,
        reader: asyncio.StreamReader,
        entry: FileEntry,
        size: int,
        label: str,
    ) -> None:
        """Receive a FILE_DATA_STREAM, writing to disk only if reconciliation accepts.

        If rejected, the raw bytes are drained from the socket to keep protocol state valid.
        """
        assert self._index is not None and self._context is not None
        local_entry = self._index.get(entry.path)
        action = Action.ACCEPT_REMOTE if local_entry is None else reconcile(local_entry, entry, self._node_id)
        accept = action == Action.ACCEPT_REMOTE
        if action == Action.CONFLICT and local_entry is not None:
            accept = self._resolve_conflict(local_entry, entry, entry.path) == Action.ACCEPT_REMOTE

        abs_path = os.path.join(self._context.local, entry.path)
        if accept:
            await proto.recv_stream_to_disk(reader, size, abs_path)
            await asyncio.to_thread(self._index.apply_remote, entry, None)
            logger.info("Accepted %r from %s", entry.path, label)
        else:
            remaining = size
            while remaining > 0:
                chunk = await reader.readexactly(min(remaining, 1 << 20))
                remaining -= len(chunk)

    async def _apply_incoming_file(
        self, entry: FileEntry, content: bytes, label: str
    ) -> None:
        assert self._index is not None
        local_entry = self._index.get(entry.path)
        if local_entry is None:
            action = Action.ACCEPT_REMOTE
        else:
            action = reconcile(local_entry, entry, self._node_id)
        if action == Action.ACCEPT_REMOTE:
            await asyncio.to_thread(self._index.apply_remote, entry, content)
            logger.info("Accepted %r from %s", entry.path, label)
        elif action == Action.CONFLICT:
            assert local_entry is not None
            if self._resolve_conflict(local_entry, entry, entry.path) == Action.ACCEPT_REMOTE:
                await asyncio.to_thread(self._index.apply_remote, entry, content)

    async def _apply_remote_deletions(
        self, remote_index: dict[str, FileEntry], label: str
    ) -> None:
        assert self._index is not None and self._context is not None
        if not self._context.provider_config.get("sync_deletes", True):
            return
        remote_live_by_checksum = {
            e.checksum: p
            for p, e in remote_index.items()
            if not e.deleted and e.checksum
        }
        for path, remote_entry in remote_index.items():
            if not remote_entry.deleted:
                continue
            local_entry = self._index.get(path)
            if local_entry is None or local_entry.deleted:
                self._pending_deletes.discard(path)
                continue
            new_path = remote_live_by_checksum.get(local_entry.checksum)
            if new_path and new_path != path:
                self._pending_deletes.discard(path)
                continue  # rename incoming — skip deletion
            if reconcile(local_entry, remote_entry, self._node_id) == Action.ACCEPT_REMOTE:
                if path not in self._pending_deletes:
                    # Defer by one round: a move whose create event hasn't landed yet
                    # would look identical to a genuine delete at this moment.
                    self._pending_deletes.add(path)
                    logger.debug("Deferring deletion of %r — confirming next round", path)
                    continue
                self._pending_deletes.discard(path)
                await asyncio.to_thread(self._index.apply_remote, remote_entry, None)
                logger.info("Applied remote deletion of %r from %s", path, label)

    async def _apply_local_renames(self, remote_index: dict[str, FileEntry]) -> None:
        """Detect renames the remote performed and apply them locally without downloading content."""
        assert self._index is not None
        remote_live_by_checksum = {
            e.checksum: p
            for p, e in remote_index.items()
            if not e.deleted and e.checksum
        }
        remote_tombstoned = {p for p, e in remote_index.items() if e.deleted}
        for path, local_entry in list(self._index.all_entries().items()):
            if local_entry.deleted or not local_entry.checksum:
                continue
            if path not in remote_tombstoned:
                continue
            new_path = remote_live_by_checksum.get(local_entry.checksum)
            if not new_path or new_path == path:
                continue
            await self._apply_rename(path, new_path, remote_index[new_path], "remote")

    async def _apply_rename(
        self, from_path: str, to_path: str, new_entry: FileEntry, label: str
    ) -> None:
        assert self._index is not None and self._context is not None
        abs_from = os.path.join(self._context.local, from_path.replace("/", os.sep))
        abs_to   = os.path.join(self._context.local, to_path.replace("/", os.sep))

        from_entry = self._index.get(from_path)
        if os.path.exists(abs_from) and from_entry and from_entry.checksum == new_entry.checksum:
            os.makedirs(os.path.dirname(abs_to), exist_ok=True)
            os.rename(abs_from, abs_to)
            old_clock = from_entry.get_clock(self._node_id)
            old_clock.increment()
            tombstone = FileEntry(
                path=from_path, checksum="", modified_time=0.0,
                vector_clock_data=old_clock.to_dict(), deleted=True,
            )
            await asyncio.to_thread(self._index.apply_remote, tombstone, None)
            await asyncio.to_thread(self._index.apply_remote, new_entry, None)
            parent = os.path.dirname(abs_from)
            while parent != self._context.local and os.path.isdir(parent) and not os.listdir(parent):
                os.rmdir(parent)
                parent = os.path.dirname(parent)
            logger.info("Renamed %r → %r from %s", from_path, to_path, label)
        else:
            logger.warning("Rename %r → %r: source missing, will re-sync next round", from_path, to_path)

    async def _serve_file(self, writer: asyncio.StreamWriter, path: str) -> None:
        assert self._index is not None and self._context is not None
        entry = self._index.get(path)
        if entry is None or entry.deleted:
            return
        abs_path = os.path.join(self._context.local, path)
        try:
            await proto.send_file_data(writer, path, abs_path, entry.to_dict())
        except OSError:
            pass

    def _start_verify_if_configured(self, context: SyncContext) -> None:
        if context.provider_config.get("verify_interval"):
            self._verify_task = asyncio.create_task(
                self._verify_loop(), name=f"verify-{context.pair_id}"
            )

    async def _verify_loop(self) -> None:
        assert self._context is not None and self._index is not None
        cfg = self._context.provider_config
        verify_interval: int = cfg.get("verify_interval", 0)
        verify_sleep: float = cfg.get("verify_sleep", 0.1)
        pair_id = self._context.pair_id

        while True:
            await asyncio.sleep(verify_interval)
            await self._paused.wait()
            for path, entry in list(self._index.all_entries().items()):
                if entry.deleted:
                    continue
                corrupted = await asyncio.to_thread(self._index.verify_one, path)
                if corrupted:
                    self._index.mark_corrupted(path)
                    logger.warning("Corruption detected in %r for pair %r", path, pair_id)
                await asyncio.sleep(verify_sleep)
