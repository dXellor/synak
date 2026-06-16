"""
Newline-delimited JSON protocol over TCP (asyncio streams).

Flow for both P2P and client-server sessions:
  1. Initiator → Listener : HELLO  (carries full index)
  2. Listener  → Initiator: HELLO
  3. Initiator → Listener : GET_FILE  (repeated for each file needed)
  4. Listener  → Initiator: FILE_DATA
  5. Initiator → Listener : SYNC_DONE  (phase boundary: stop requesting)
  6. Initiator → Listener : FILE_DATA  (push files the listener is missing)
  7. Initiator → Listener : ACK        (end of session)
"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any


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
