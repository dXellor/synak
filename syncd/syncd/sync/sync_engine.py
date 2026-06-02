"""Conflict detection and resolution following Ramsey & Csirmaz (2001) and Shapiro et al. (2011)."""

from __future__ import annotations

import os
import shutil
from enum import Enum

from syncd.sync.file_index import FileEntry


class Action(Enum):
    KEEP_LOCAL = "keep_local"
    ACCEPT_REMOTE = "accept_remote"
    CONFLICT = "conflict"


def reconcile(local: FileEntry, remote: FileEntry, node_id: str) -> Action:
    """
    Determine what to do given two versions of the same path.

    Based on vector clock comparison (Parker et al. 1983):
    - remote happened-before local  → KEEP_LOCAL  (we already have the newer version)
    - local happened-before remote  → ACCEPT_REMOTE
    - concurrent                    → CONFLICT
    """
    local_clock = local.get_clock(node_id)
    remote_clock = remote.get_clock(node_id)

    if remote_clock.happens_before(local_clock):
        return Action.KEEP_LOCAL
    if local_clock.happens_before(remote_clock):
        return Action.ACCEPT_REMOTE
    return Action.CONFLICT


def resolve_last_write_wins(local: FileEntry, remote: FileEntry) -> Action:
    """Compare modification timestamps; keep the newer file."""
    return Action.ACCEPT_REMOTE if remote.modified_time > local.modified_time else Action.KEEP_LOCAL


def resolve_keep_both(
    local: FileEntry,
    remote: FileEntry,
    node_id: str,
    watch_dir: str,
) -> Action:
    """
    Keep both versions by renaming the local copy with a .conflict suffix.
    Idempotent: renaming twice produces the same result (CRDT property).
    Returns ACCEPT_REMOTE so the caller writes the remote version to the original path.
    """
    conflict_path = os.path.join(watch_dir, f"{local.path}.conflict.{node_id}")
    src = os.path.join(watch_dir, local.path)
    if os.path.exists(src) and not os.path.exists(conflict_path):
        shutil.copy2(src, conflict_path)
    return Action.ACCEPT_REMOTE
