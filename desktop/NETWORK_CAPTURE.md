# Private native network capture

Opt in with `--network-capture-directory PATH`. PATH must be a **new,
nonexistent** directory whose parent already exists; symlinks in the path are
rejected. `--network-capture-max-bytes N` is positive and defaults to 268435456.
Without the directory option there is no recorder thread or capture filesystem
activity. Capture currently requires Unix; other platforms reject only the
capture option, not normal playback.

This is sensitive local evidence, not a packet capture. Directories/files are
created with Unix modes 0700/0600. The capture directory contains a `.gitignore`
with `*`. Do not publish payloads: they can contain credentials, account IDs,
login/session data and game state. There is no AS trace collection. Existing
player logging and screenshot facilities are independent of this recorder.

## Format v1

`network-index.jsonl` has one JSON object per event:

- `time_ms`: Unix epoch milliseconds at submission (not a monotonic clock).
- `id`: session-wide HTTP request/socket attempt number; zero is session scope.
- `event`: event name below.
- `file`: relative numbered raw file path for bytes, otherwise null.
- `offset`: byte offset in that directional file (zero for non-byte events).
- `value`: byte count for `bytes` and `http_request_body_submitted`, HTTP status
  for `http_final_response`, otherwise zero.

HTTP files: `http/<id>.request.bin`, `http/<id>.response.bin`.
Socket files: `connections/<id>.client.bin`, `connections/<id>.server.bin`.
Empty streams have no raw file. Index order is writer queue order; within each
file, append order and offsets preserve byte order. Socket read boundaries and
HTTP chunks are not protocol message boundaries. The existing byte-level game
protocol oracle can decode the directional socket streams, including RTMP
handshake/AMF bytes, separately.

No URLs (including paths, userinfo or query strings), hostnames, headers, or error
messages are indexed. HTTP requests identify GET/POST and final response status,
not endpoint. This intentionally trades endpoint correlation for privacy.

## Coverage and completeness

- `http_get_submitted` / `http_post_submitted` and
  `http_request_body_submitted` describe the application's submitted request
  body, **not proof of bytes transmitted**. The transport may fail or redirect.
- `http_final_response` records the final reqwest response status. Redirect
  exchanges, HTTP framing, headers, TLS packets and unconsumed bytes are absent.
- Successful response bytes are recorded only when the player consumes `body()`
  or `next_chunk()`; no eager background draining is added.
- `http_response_eof` establishes consumed response EOF. `http_body_error`,
  `http_request_error`, `http_network_unavailable`,
  `http_error_body_unconsumed` and `http_cancelled_or_unconsumed` do **not**.
  Non-success HTTP response bodies are not consumed by the existing backend and
  therefore are not captured. File/bundle loads and website navigation are not
  HTTP capture events. An invalid URL rejected before fetch is not captured.
- `socket_attempt`, `socket_connected`, `socket_denied`,
  `socket_connect_timeout` and `socket_connect_error` identify connection state.
  Socket bytes are exactly the successful read/write slices, including partial
  writes, before the existing buffers are forwarded/drained. No retry or ordering
  changes are made. `socket_server_eof`, `socket_read_error`,
  `socket_write_error`, `socket_client_queue_closed` and
  `socket_closed_or_cancelled` identify termination observations; cancellation
  does not establish a remote EOF or a fully transmitted application queue.
- `session_start_v1` and `session_shutdown` delimit the application's recorder
  lifetime when those events fit the budget. Movie reloads share the same owner.

An empty `capture.incomplete` marker exists from initialization. On normal
writer shutdown it is renamed to `capture.closed`, or to
`capture.partial-quota`, `capture.partial-queue`, or `capture.partial-storage`
on loss. A crash or failed marker rename leaves `capture.incomplete`.
**`capture.closed` means the recorder drained, not that every request/socket
reached EOF.** Check individual events. Any partial/incomplete marker makes all
later completeness claims invalid. Storage failure can leave a truncated last
JSON line or raw bytes without an index entry; only indexed spans are evidence.

## Resource and lifecycle bounds

One owned in-process writer accepts a nonblocking 64-entry queue. Each entry has
at most 64 KiB of payload (~4 MiB queued plus bounded per-producer/in-flight
chunks). No disk operation occurs on the player/update path. The total budget
counts logical contents of raw files, JSONL and the one-byte `.gitignore`;
filesystem allocation blocks/inodes are not byte-budgeted. Empty markers need
no remaining payload budget, so even N=1 produces an honest partial capture.

Queue overflow, quota exhaustion and storage errors stop recording globally,
log a generic warning and leave partial evidence. Gameplay is neither failed
nor retried. Accepted queued events are drained up to the first writer failure;
no subsequent traffic is represented as complete. Shutdown cancels player and
runtime producers, stops acceptance, drains, flushes and joins the writer.
Producer handles cannot keep the writer alive. Flush is not fsync/power-loss
durability, and normal exit can wait for the filesystem. The private directory
must not be concurrently manipulated by another process with the same UID.
