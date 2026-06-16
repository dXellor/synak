"""
Newline-delimited JSON protocol over TCP (asyncio streams).

Flow for both P2P and client-server sessions:
  1. Initiator → Listener : HELLO  (carries full index)
  2. Listener  → Initiator: HELLO
  3. Initiator → Listener : GET_FILE  (repeated for each file needed)
  4. Listener  → Initiator: FILE_DATA or FILE_DATA_STREAM
  5. Initiator → Listener : SYNC_DONE  (phase boundary: stop requesting)
  6. Initiator → Listener : FILE_DATA or FILE_DATA_STREAM  (push files the listener is missing)
  7. Initiator → Listener : ACK        (end of session)

File transfer encoding:
  - FILE_DATA         — base64 content inline in JSON; used for files < 64 MB (STREAM_THRESHOLD)
  - FILE_DATA_STREAM  — JSON header line followed immediately by raw bytes; used for files ≥ 64 MB.
                        Receiver writes to a .synak.tmp temp file (excluded by the built-in *.tmp 
                        watcher filter) and renames atomically so no partial-write events fire.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
from typing import Any

STREAM_THRESHOLD = 64 * 1024 * 1024  # bytes; files at or above this use binary streaming


async def read_message(reader: asyncio.StreamReader) -> dict[str, Any] | None:
    try:
        line = await reader.readline()
        if not line:
            return None
        return json.loads(line.decode())
    except (json.JSONDecodeError, UnicodeDecodeError, asyncio.IncompleteReadError):
        return None


async def send_message(writer: asyncio.StreamWriter, msg: dict[str, Any]) -> None:
    writer.write(json.dumps(msg).encode() + b"\n")
    await writer.drain()


def encode_content(data: bytes) -> str:
    return base64.b64encode(data).decode()


def decode_content(encoded: str) -> bytes:
    return base64.b64decode(encoded) if encoded else b""


# --- message constructors ---

def hello_msg(node_id: str, index: dict[str, Any]) -> dict[str, Any]:
    return {"type": "HELLO", "node_id": node_id, "index": index}


def get_file_msg(path: str) -> dict[str, Any]:
    return {"type": "GET_FILE", "path": path}


def file_data_msg(path: str, content: bytes, entry: dict[str, Any]) -> dict[str, Any]:
    return {"type": "FILE_DATA", "path": path,
            "content": encode_content(content), "entry": entry}


def delete_msg(path: str, clock: dict[str, Any]) -> dict[str, Any]:
    return {"type": "DELETE_FILE", "path": path, "vector_clock": clock}


def sync_done_msg() -> dict[str, Any]:
    return {"type": "SYNC_DONE"}


def ack_msg(node_id: str) -> dict[str, Any]:
    return {"type": "ACK", "node_id": node_id}


def file_data_stream_header(path: str, size: int, entry: dict[str, Any]) -> dict[str, Any]:
    return {"type": "FILE_DATA_STREAM", "path": path, "size": size, "entry": entry}


async def send_file_data(
    writer: asyncio.StreamWriter, path: str, abs_path: str, entry: dict[str, Any]
) -> None:
    """Send FILE_DATA (base64) for small files, FILE_DATA_STREAM (raw bytes) for large ones."""
    size = os.path.getsize(abs_path)
    if size < STREAM_THRESHOLD:
        with open(abs_path, "rb") as f:
            content = f.read()
        await send_message(writer, file_data_msg(path, content, entry))
    else:
        writer.write((json.dumps(file_data_stream_header(path, size, entry)) + "\n").encode())
        with open(abs_path, "rb") as f:
            while chunk := f.read(1 << 20):
                writer.write(chunk)
        await writer.drain()


async def recv_stream_to_disk(reader: asyncio.StreamReader, size: int, abs_path: str) -> None:
    """Stream FILE_DATA_STREAM body to disk in 1 MB chunks.

    Writes to <abs_path>.synak.tmp first (*.tmp is in the built-in watcher exclusion list,
    so no partial-write events fire). The final os.replace is the only event the watcher sees.
    """
    tmp = abs_path + ".synak.tmp"
    parent = os.path.dirname(abs_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(tmp, "wb") as f:
        remaining = size
        while remaining > 0:
            chunk = await reader.readexactly(min(remaining, 1 << 20))
            f.write(chunk)
            remaining -= len(chunk)
    os.replace(tmp, abs_path)
