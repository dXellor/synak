# External Provider Interface

This document defines the contract for implementing a non-Python sync provider that is managed by the `syncd` daemon. It covers the subprocess IPC protocol, the TCP wire protocol between nodes, and the on-disk index format that providers must share to stay interoperable.

---

## Overview

`syncd` manages providers as child processes. When a pair is started, `syncd` spawns the provider binary and communicates with it over **stdin/stdout using newline-delimited JSON**. The binary does all the actual work — file watching, hashing, network sessions — and `syncd` acts as a lifecycle manager and API bridge.

```
syncd (Python daemon)
  └── SubprocessProvider
        │  stdin  → JSON command + \n
        │  stdout ← JSON response + \n
        │  stderr → forwarded to syncd logger at DEBUG
        └── your binary
              ├── file watcher
              ├── index (.synak/index.json)
              └── TCP sync sessions with peers
```

`syncd` sends exactly one command at a time and waits for exactly one response before sending the next. Commands are never pipelined.

---

## IPC Protocol

### Request format

Every request is a single JSON object followed by `\n`:

```json
{"cmd": "<command>", ...}
```

### Response format

Every response is a single JSON object followed by `\n`:

```json
{"ok": true}
{"ok": true, "data": {...}}
{"ok": false, "error": "<human-readable message>"}
```

### Commands

#### `start`

Sent once, before any other command. The binary must initialise itself and begin operation. It must respond **after** the listener is bound and the sync loop is running — `syncd` considers the provider ready when it receives `{"ok": true}`.

```json
{
  "cmd": "start",
  "context": {
    "pair_id":        "my-pair",
    "local":          "/home/user/sync-dir",
    "direction":      "bidirectional",
    "interval":       30,
    "provider_config": { ... },
    "exclude":        []
  }
}
```

`local` may contain a leading `~/` that the binary must expand to the user's home directory.

Response: `{"ok": true}` or `{"ok": false, "error": "..."}`.

#### `stop`

Sent once to request graceful shutdown. The binary must flush any in-progress state (save the index, finish any active session if feasible), respond, and then exit. `syncd` will `SIGKILL` if the process does not exit within a reasonable time.

```json
{"cmd": "stop"}
```

Response: `{"ok": true}`, then the process exits.

#### `pause`

Suspend active work — stop initiating sync rounds. The binary must keep its TCP listener running (so peers can still connect to it) but must not initiate new outgoing sessions while paused.

```json
{"cmd": "pause"}
```

Response: `{"ok": true}`.

#### `resume`

Resume normal operation after a `pause`.

```json
{"cmd": "resume"}
```

Response: `{"ok": true}`.

#### `trigger`

Initiate a sync round immediately, regardless of the configured interval. The binary should start the round in the background and respond immediately — do not block until the round completes.

```json
{"cmd": "trigger"}
```

Response: `{"ok": true}`.

#### `status`

Return the current operational state.

```json
{"cmd": "status"}
```

Response:

```json
{
  "ok": true,
  "data": {
    "pair_id":   "my-pair",
    "state":     "idle",
    "last_sync": 1718462400.123,
    "error":     "",
    "extra":     { "node_id": "n1", "port": 31337 }
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `pair_id` | string | Echo of the pair id from `start` |
| `state` | string | `"idle"`, `"syncing"`, `"paused"`, `"stopped"`, or `"error"` |
| `last_sync` | float | Unix timestamp of last successful sync round, or `0` |
| `error` | string | Last error message, or `""` |
| `extra` | object | Provider-specific metadata (shown in `synctl status`) |

### Stderr

All output written to stderr is forwarded by `syncd` to its own logger at `DEBUG` level. Use it freely for logs. Log lines are not expected to follow any schema.

---

## SyncContext fields

The `context` object passed in `start` has these fields:

| Field | Type | Description |
|-------|------|-------------|
| `pair_id` | string | Stable identifier for this sync pair |
| `local` | string | Absolute path of the local watch directory (after `~/` expansion: do it yourself) |
| `direction` | string | `"push"`, `"pull"`, or `"bidirectional"` |
| `interval` | int | Seconds between sync rounds. `0` means file-watch-triggered only |
| `provider_config` | object | Raw provider config from the TOML file, validated against the provider's SCHEMA |
| `exclude` | []string | Extra glob patterns to exclude from indexing (on top of the built-in defaults) |

`provider_config` contains whatever fields you declare in your Python wrapper's `SCHEMA`. It is passed through verbatim — validate or default your own fields from it.

---

## On-disk index format

Providers that want to be interoperable with built-in Python nodes must read and write the index in the same format.

**Location:** `<local>/.synak/index.json`

**Format:** a JSON object mapping relative POSIX path → `FileEntry`:

```json
{
  "docs/readme.txt": {
    "path":             "docs/readme.txt",
    "checksum":         "a1b2c3...64hex...",
    "modified_time":    1718462400.123,
    "vector_clock_data": {
      "node_id": "n1",
      "clock":   { "n1": 3, "n2": 1 }
    },
    "deleted": false
  },
  "old/file.txt": {
    "path":             "old/file.txt",
    "checksum":         "",
    "modified_time":    0.0,
    "vector_clock_data": { "node_id": "n1", "clock": { "n1": 5 } },
    "deleted": true
  }
}
```

### FileEntry fields

| Field | Type | Notes |
|-------|------|-------|
| `path` | string | Relative path using `/` separators, no leading slash |
| `checksum` | string | SHA-256 hex digest of file content; `""` for tombstones |
| `modified_time` | float | Unix timestamp in seconds (with sub-second precision); `0.0` for tombstones |
| `vector_clock_data` | object | Serialised vector clock (see below) |
| `deleted` | bool | `true` = tombstone; `false` may be omitted |

### Vector clock format

```json
{
  "node_id": "n1",
  "clock":   { "n1": 3, "n2": 1 }
}
```

- `node_id`: the local node's identifier (same string used as key in the clock map)
- `clock`: maps node id → logical timestamp (non-negative integer); missing keys are treated as `0`

**Rules:**
- Increment your own component on every local write or delete
- On receiving a remote entry: take the component-wise maximum of clocks (Bayou merge)
- Tombstones carry a clock: increment before creating the tombstone so peers know the deletion is newer than the last live version

### Built-in excludes

The following filename patterns are excluded from indexing by default. Your implementation should honour them:

```
*.swp  *-swp  *.swpx  *.swn  .DS_Store  Thumbs.db  *.tmp  *.temp  *~
```

The `.synak` directory itself must never be indexed.

---

## TCP wire protocol

Providers that implement direct sync sessions with other nodes (P2P or client-server) must speak this protocol to interoperate with built-in Python nodes.

**Framing:** one JSON object per line, terminated by `\n`, over a raw TCP stream. Use a 1 MB read buffer per connection to avoid short reads on large indexes.

### Message types

#### `HELLO`

Sent by both sides at session open. Carries the sender's full file index.

```json
{
  "type":    "HELLO",
  "node_id": "n1",
  "index":   { "<path>": <FileEntry>, ... }
}
```

#### `GET_FILE`

Sent by the initiator to request a file's content.

```json
{ "type": "GET_FILE", "path": "docs/readme.txt" }
```

#### `FILE_DATA`

Response to `GET_FILE`, or an unsolicited push. Content is base64-encoded.

```json
{
  "type":    "FILE_DATA",
  "path":    "docs/readme.txt",
  "content": "<base64>",
  "entry":   <FileEntry>
}
```

#### `SYNC_DONE`

Sent by the initiator after it has requested all files it needs. Signals the start of the push phase.

```json
{ "type": "SYNC_DONE" }
```

#### `ACK`

Sent by the initiator after it has pushed all files. Signals end of session.

```json
{ "type": "ACK", "node_id": "n1" }
```

### Session flow

```
Initiator → Listener : HELLO (initiator's full index)
Listener  → Initiator : HELLO (listener's full index)

  ┌─ for each file initiator needs:
  │   Initiator → Listener : GET_FILE
  │   Listener  → Initiator : FILE_DATA
  └─

Initiator → Listener : SYNC_DONE

  ┌─ for each file listener is missing or behind on:
  │   Initiator → Listener : FILE_DATA
  └─

Initiator → Listener : ACK
```

Both sides apply each other's deletions after the HELLO exchange (before the pull phase begins). Both sides save the index after ACK/session end.

### Default listen port

If no explicit port is configured, derive a stable port from the pair id:

```
port = 30000 + (sha256_big_integer(pair_id) % 35536)
```

`sha256_big_integer` means: interpret the full 32-byte SHA-256 digest as an unsigned big-endian integer. This matches the Python P2P provider so Go and Python nodes for the same pair automatically agree on a port.

---

## Reconciliation rules

When deciding which side's version of a file to keep:

1. If clocks are **equal**: keep local, do nothing.
2. If remote clock **happens-before** local: keep local (we're already newer).
3. If local clock **happens-before** remote: accept remote.
4. If clocks are **concurrent** (neither happens-before the other): conflict — apply the configured strategy.

`A happens-before B` means every component of A ≤ the corresponding component of B, and at least one component of A < B.

### Conflict strategies

Taken from `provider_config["conflict_strategy"]` (default: `"last-write-wins"`):

- **`last-write-wins`**: compare `modified_time`; keep whichever is newer.
- **`keep-both`**: rename the local copy to `<path>.syncd-conflict.<node_id>`, then accept remote at the original path. No data is lost.

A same-checksum shortcut applies before invoking any strategy: if both sides have the same SHA-256, treat it as KEEP_LOCAL regardless of clocks (avoids spurious conflict noise when clocks diverge on identical content).

---

## Config validation

The validation split is:

- **Native Python providers** (`p2p`, `client-server`, …) — `syncd` validates `provider_config` against the class `SCHEMA` at startup using jsonschema, before the provider is started.
- **External binary providers** — the binary validates its own config when it receives `start`. If the config is invalid it responds `{"ok": false, "error": "..."}` and `syncd` surfaces that error and skips the pair. There is no pre-validation on the Python side.

This means your binary is responsible for rejecting bad config clearly and early, rather than failing silently mid-operation. Validate all required and enum fields at the top of your `start` handler and return a descriptive error immediately if anything is wrong.

---

## Registering a provider in Python

### Without a wrapper (recommended)

No Python file is needed. Any binary with a `binary` key in its config is auto-detected as a subprocess provider:

```toml
[[pairs]]
id        = "work-docs"
mode      = "my-provider"
local     = "~/Documents/Work"
direction = "bidirectional"
interval  = 60

[pairs.provider]
binary = "/usr/local/bin/my-provider"
```

`mode` is just a display name — it does not need to match anything registered in Python.

### With a wrapper (optional, for strict Python-side schema validation)

If you want `syncd` to validate `provider_config` before even spawning the binary, create a wrapper class:

```python
# syncd/syncd/sync/providers/my_provider.py

from syncd.sync.providers.subprocess import SubprocessProvider

class MyProvider(SubprocessProvider):
    NAME = "my-provider"   # must match mode = "my-provider" in config
    SCHEMA = {
        "type": "object",
        "properties": {
            "binary": {"type": "string"},
            # ... your fields ...
        },
        "required": ["binary"],
        "additionalProperties": False,
    }
```

Register it in `syncd/syncd/sync/providers/__init__.py`:

```python
from syncd.sync.providers.my_provider import MyProvider
registry.register(MyProvider)
```

With a wrapper, bad config is caught by jsonschema before the binary is spawned. Without one, bad config is caught by the binary's own `start` handler. Both paths surface the error to the user — the wrapper just catches it earlier.

---

## Implementation checklist

- [ ] Validates `provider_config` at the top of the `start` handler; returns `{"ok": false, "error": "..."}` immediately on bad config
- [ ] Reads commands from stdin, writes responses to stdout, logs to stderr
- [ ] Expands `~/` in `context.local` before using it as a path
- [ ] Responds `{"ok": true}` to `start` only after the listener is bound and sync loop is running
- [ ] Exits the process after responding to `stop`
- [ ] Does not initiate outgoing sessions while paused; keeps listener running
- [ ] Responds to `trigger` immediately (starts round in background)
- [ ] Returns valid `ProviderStatus` JSON in `data` for `status`
- [ ] Uses `/` as path separator in the index and on the wire (convert from OS separator on scan)
- [ ] Stores index at `<local>/.synak/index.json` in the documented format
- [ ] Hides `.synak` on Windows (`SetFileAttributes FILE_ATTRIBUTE_HIDDEN`)
- [ ] Increments the vector clock on every local write and delete
- [ ] Applies the Bayou merge rule (component-wise max) when accepting a remote entry
- [ ] Never increments the clock when detecting corruption — only flags for re-pull
- [ ] Tombstones carry a clock increment so peers know the deletion is intentional
- [ ] Prunes empty parent directories after applying a remote deletion
