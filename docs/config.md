# Configuration Reference

Format: **TOML**. Default location: `~/.config/syncd/config.toml`.

Override with `syncd -c /path/to/config.toml`.

---

## `[daemon]`

```toml
[daemon]
api_socket = "/run/user/1000/syncd.sock"  # default: /run/user/<uid>/syncd.sock
log_level  = "info"                        # default: "info"
```

| Field | Values | Description |
|-------|--------|-------------|
| `api_socket` | path string | Unix socket the daemon and synctl communicate over |
| `log_level` | `debug` `info` `warning` `error` `critical` | Logging verbosity |

---

## `[[pairs]]`

Repeat this block for each directory you want to sync. Each pair runs its own provider instance.

```toml
[[pairs]]
id        = "work-docs"       # required, unique
mode      = "client-server"   # required: "client-server" or "p2p"
local     = "~/Documents/Work" # required, ~ is expanded
direction = "bidirectional"   # required: "push", "pull", or "bidirectional"
interval  = 300               # seconds between syncs; 0 = watch-based (default: 0)

[pairs.provider]
# provider-specific fields — see below
```

| Field | Required | Description |
|-------|----------|-------------|
| `id` | yes | Unique name used in `synctl sync trigger <id>` |
| `mode` | yes | Which provider to use |
| `local` | yes | Local directory to sync |
| `direction` | yes | `push` — send only; `pull` — receive only; `bidirectional` — both |
| `interval` | no | Seconds between sync cycles. `0` uses filesystem watching (client-server only) |

---

## `[pairs.provider]` — client-server

```toml
[pairs.provider]
mode              = "server"         # required: "server" or "client"
host              = "0.0.0.0"        # bind address (server) or server hostname (client)
port              = 5000             # required
node_id           = "my-node"        # optional, auto-generated UUID if omitted
conflict_strategy = "last-write-wins" # optional, default: "last-write-wins"
sync_deletes      = true             # optional, default: true
```

| Field | Required | Description |
|-------|----------|-------------|
| `mode` | yes | `"server"` listens for connections; `"client"` connects to the server |
| `host` | no | Server: interface to bind. Client: server hostname. Default `0.0.0.0` / `127.0.0.1` |
| `port` | yes | TCP port |
| `node_id` | no | Human-readable node name. Persisted in `.synak/` — only set this once |
| `conflict_strategy` | no | `"last-write-wins"` keeps the newer file; `"keep-both"` renames the local copy to `file.conflict.<node_id>` |
| `sync_deletes` | no | If `false`, remote deletions are ignored — your local files are never deleted by a remote |

---

## `[pairs.provider]` — p2p

```toml
[pairs.provider]
peers             = ["192.168.1.10:5000", "192.168.1.11:5000"]  # required
port              = 5000              # required, this node's listen port
node_id           = "node-a"         # optional
conflict_strategy = "keep-both"      # optional, default: "last-write-wins"
sync_deletes      = true             # optional, default: true
```

| Field | Required | Description |
|-------|----------|-------------|
| `peers` | yes | List of other nodes as `"host:port"` |
| `port` | yes | TCP port this node listens on for incoming peer connections |
| `node_id` | no | Human-readable node name |
| `conflict_strategy` | no | Same as client-server |
| `sync_deletes` | no | Same as client-server |

Each node must list the others in its `peers`. For two nodes A and B: A lists B, B lists A.

---

## `[peers]`

Global peer discovery settings. Currently only `"static"` discovery is implemented.

```toml
[peers]
discovery = "static"   # default: "static"

[[peers.static]]
id      = "peer-abc123"
address = "192.168.1.42:5000"
```

---

## Full example

```toml
[daemon]
api_socket = "/run/user/1000/syncd.sock"
log_level  = "info"

# Client-server pair — this node is the client
[[pairs]]
id        = "work-docs"
mode      = "client-server"
local     = "~/Documents/Work"
direction = "bidirectional"
interval  = 60

[pairs.provider]
mode              = "client"
host              = "myserver.example.com"
port              = 5000
conflict_strategy = "keep-both"
sync_deletes      = false

# P2P pair — syncs photos with two other machines
[[pairs]]
id        = "photos"
mode      = "p2p"
local     = "~/Pictures"
direction = "bidirectional"
interval  = 30

[pairs.provider]
peers    = ["192.168.1.10:5001", "192.168.1.11:5001"]
port     = 5001
node_id  = "laptop"
```

---

## Reload without restart

```bash
synctl config reload
# or
kill -SIGHUP <syncd-pid>
```
