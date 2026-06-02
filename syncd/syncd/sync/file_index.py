"""FileIndex — tracks file state and dirty sets following Ramsey & Csirmaz (2001)."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any

from syncd.sync.vector_clock import VectorClock

METADATA_DIR = ".synak"
INDEX_FILE = "index.json"


@dataclass
class FileEntry:
    path: str                       # relative path from watch root
    checksum: str                   # SHA-256 hex digest; "" for tombstones
    modified_time: float            # local mtime at last observed change
    vector_clock_data: dict[str, Any]  # serialised VectorClock (primitives only)
    deleted: bool = False           # Bayou-style tombstone

    def get_clock(self, node_id: str) -> VectorClock:
        return VectorClock.from_dict(self.vector_clock_data, node_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "checksum": self.checksum,
            "modified_time": self.modified_time,
            "vector_clock_data": self.vector_clock_data,
            "deleted": self.deleted,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FileEntry:
        return cls(
            path=data["path"],
            checksum=data["checksum"],
            modified_time=data["modified_time"],
            vector_clock_data=data["vector_clock_data"],
            deleted=data.get("deleted", False),
        )


def _sha256(abs_path: str) -> str:
    h = hashlib.sha256()
    with open(abs_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


class FileIndex:
    def __init__(self, watch_dir: str, node_id: str) -> None:
        self._watch_dir = watch_dir
        self._node_id = node_id
        self._entries: dict[str, FileEntry] = {}
        self._meta_dir = os.path.join(watch_dir, METADATA_DIR)
        self._index_path = os.path.join(self._meta_dir, INDEX_FILE)

    def load(self) -> None:
        os.makedirs(self._meta_dir, exist_ok=True)
        if not os.path.exists(self._index_path):
            return
        with open(self._index_path) as f:
            raw: dict[str, Any] = json.load(f)
        self._entries = {k: FileEntry.from_dict(v) for k, v in raw.items()}

    def save(self) -> None:
        os.makedirs(self._meta_dir, exist_ok=True)
        with open(self._index_path, "w") as f:
            json.dump({k: v.to_dict() for k, v in self._entries.items()}, f, indent=2)

    def scan(self) -> set[str]:
        """Re-scan the directory; return the dirty set (paths that changed)."""
        dirty: set[str] = set()
        seen: set[str] = set()

        for dirpath, dirnames, filenames in os.walk(self._watch_dir):
            dirnames[:] = [d for d in dirnames if d != METADATA_DIR]
            for fname in filenames:
                abs_path = os.path.join(dirpath, fname)
                rel_path = os.path.relpath(abs_path, self._watch_dir)
                seen.add(rel_path)

                mtime = os.path.getmtime(abs_path)
                existing = self._entries.get(rel_path)

                if existing is None or existing.deleted:
                    checksum = _sha256(abs_path)
                    clock = VectorClock(self._node_id)
                    clock.increment()
                    self._entries[rel_path] = FileEntry(
                        path=rel_path,
                        checksum=checksum,
                        modified_time=mtime,
                        vector_clock_data=clock.to_dict(),
                    )
                    dirty.add(rel_path)
                elif abs(mtime - existing.modified_time) > 0.001:
                    checksum = _sha256(abs_path)
                    if checksum != existing.checksum:
                        clock = existing.get_clock(self._node_id)
                        clock.increment()
                        self._entries[rel_path] = FileEntry(
                            path=rel_path,
                            checksum=checksum,
                            modified_time=mtime,
                            vector_clock_data=clock.to_dict(),
                        )
                        dirty.add(rel_path)

        # Detect deletions — mark with tombstone
        for rel_path, entry in list(self._entries.items()):
            if rel_path not in seen and not entry.deleted:
                clock = entry.get_clock(self._node_id)
                clock.increment()
                self._entries[rel_path] = FileEntry(
                    path=rel_path,
                    checksum="",
                    modified_time=0.0,
                    vector_clock_data=clock.to_dict(),
                    deleted=True,
                )
                dirty.add(rel_path)

        return dirty

    def get(self, path: str) -> FileEntry | None:
        return self._entries.get(path)

    def update(self, entry: FileEntry) -> None:
        self._entries[entry.path] = entry

    def all_entries(self) -> dict[str, FileEntry]:
        return dict(self._entries)

    def apply_remote(self, entry: FileEntry, content: bytes | None) -> None:
        """Write a remote file to disk and record it in the index."""
        abs_path = os.path.join(self._watch_dir, entry.path)
        if entry.deleted:
            if os.path.exists(abs_path):
                os.remove(abs_path)
            self._entries[entry.path] = entry
        else:
            parent = os.path.dirname(abs_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            if content is not None:
                with open(abs_path, "wb") as f:
                    f.write(content)
            # Record the actual mtime after writing so future scans don't rehash
            actual_mtime = os.path.getmtime(abs_path) if os.path.exists(abs_path) else entry.modified_time
            self._entries[entry.path] = FileEntry(
                path=entry.path,
                checksum=entry.checksum,
                modified_time=actual_mtime,
                vector_clock_data=entry.vector_clock_data,
                deleted=entry.deleted,
            )
