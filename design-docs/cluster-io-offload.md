# Cluster Bus I/O Offload

## Overview

Cluster bus read, write, and TLS accept work can run on the shared I/O thread
pool, while all cluster state mutation stays on the main thread.

Jobs flow through:
- `io_shared_inbox` (SPMC): main thread -> I/O workers
- `io_shared_outbox` (MPSC): I/O workers -> main thread

If the pool is inactive or enqueue fails, the caller falls back to synchronous
I/O on the main thread and increments `stat_cluster_io_sync_fallbacks`.

## Architecture

```text
+---------------------------+       +-------------------------------+       +---------------------------+
|       Main Thread         |       |        Shared Queues          |       |     I/O Thread Pool       |
|                           |       |                               |       |                           |
| clusterReadOffloadHandler |------>| io_shared_inbox (SPMC)        |------>| clusterReadJob            |
| clusterWriteHandler       |       |                               |       | clusterWriteJob           |
| clusterAcceptHandler      |       |                               |       | clusterAcceptJob          |
| clusterSendMessage        |       +-------------------------------+       +---------------------------+
|                           |
| processIOThreadsResponses |<------ io_shared_outbox (MPSC) <------ sendToMainThread(...)
| clusterHandle*Completion  |
| clusterProcessPacket      |
| freeClusterLink           |
+---------------------------+
```

## Key Design Decisions

1. Reuse the existing shared SPMC/MPSC queues; no cluster-specific threading primitives.
2. I/O threads do transport work only: `connRead`, `connWrite`, `connAccept`, framing, and result publication.
3. Main thread alone applies cluster packets and mutates cluster state.
4. Write offload uses a single canonical `send_msg_queue` plus a snapshot boundary:
   `io_last_send_block`, `io_head_offset`, and `io_nodes_sent`.
5. Read offload uses `rcvbuf` snapshot boundaries: `io_rcvbuf_snapshot_len` and `io_rcvbuf_snapshot_packets`.
6. Read completion drains the queued `rcvbuf` snapshot fully; there is no bounded packet/time budget.
7. Link teardown is deferred with `io_refs` and `async_close`.
8. Accept offload is serialized per connection with `CONN_FLAG_ACCEPT_OFFLOAD_PENDING`.
9. `ConnOwnerKind` lets generic connection/TLS code distinguish client-owned and cluster-owned connections safely.

## Data Flow: Read Path

```text
[MAIN dispatch] clusterReadOffloadHandler
    |
    | drain queued snapshot
    | trySendClusterReadToIOThreads()
    v
[QUEUE] io_shared_inbox
    | JOB_REQ_CLUSTER_READ
    v
[IO worker] clusterReadJob
    | connRead() into rcvbuf
    | grow rcvbuf if needed
    | snapshot complete packet prefix
    | set io_rcvbuf_snapshot_len / packets
    | set io_result
    v
[QUEUE] io_shared_outbox
    | JOB_RES_CLUSTER_READ
    v
[MAIN completion] clusterHandleReadCompletion
    | set io_read_state = IDLE
    | io_refs--
    | reconcile memory
    | drain queued rcvbuf snapshot fully
    | shrink/free as needed
```

If dispatch returns `C_ERR`, the main thread falls back to `clusterReadHandler(conn)`.

## Data Flow: Write Path

```text
[MAIN dispatch] clusterSendMessage / clusterWriteHandler
    |
    | append to send_msg_queue
    | snapshot io_last_send_block / io_head_offset
    | trySendClusterWriteToIOThreads()
    v
[QUEUE] io_shared_inbox
    | JOB_REQ_CLUSTER_WRITE
    v
[IO worker] clusterWriteJob
    | write from queue head
    | stop at io_last_send_block
    | set io_nodes_sent
    | set io_head_offset
    | set io_result
    v
[QUEUE] io_shared_outbox
    | JOB_RES_CLUSTER_WRITE
    v
[MAIN completion] clusterHandleWriteCompletion
    | set io_write_state = IDLE
    | io_refs--
    | pop sent nodes
    | apply head_msg_send_offset
    | reinstall handler or free on error
```

Important note:
- New messages appended while a write job is in flight stay on `send_msg_queue`,
  but the worker stops at `io_last_send_block`, so those new nodes are picked up
  by a later dispatch.

If dispatch returns `C_ERR`, `clusterWriteHandler(conn)` falls back to the synchronous `connWrite()` loop.

## Data Flow: Accept Path (TLS)

```text
[MAIN dispatch] clusterAcceptHandler
    |
    | create accepted conn
    | set owner kind
    | allow accept offload
    | trySendClusterAcceptToIOThreads()
    v
[QUEUE] io_shared_inbox
    | JOB_REQ_CLUSTER_ACCEPT
    v
[IO worker] clusterAcceptJob
    | connAccept(conn, NULL)
    v
[QUEUE] io_shared_outbox
    | JOB_RES_CLUSTER_ACCEPT
    v
[MAIN completion] clusterHandleAcceptCompletion
    | clear ACCEPT_OFFLOAD_PENDING
    | finalize conn state / refs
    | if ACCEPTING: return
    | else clusterConnAcceptHandler()
    | create clusterLink on success
```

Important note:
- TLS retries can re-enter the generic accept offload path. Cluster-owned
  connections are routed back to `trySendClusterAcceptToIOThreads`, and the
  pending flag ensures only one accept job is in flight for that connection.

If dispatch returns `C_ERR`, the main thread falls back to `connAccept(conn, clusterConnAcceptHandler)`.

## clusterLink I/O State

```text
IDLE --dispatch read-->  READ_PENDING  --read completion-->  IDLE
IDLE --dispatch write--> WRITE_PENDING --write completion--> IDLE

If freeClusterLink() sees io_refs > 0:
  detach link from node
  remove handlers
  set async_close = 1
  defer final free until the last completion drops io_refs to 0
```

Read and write jobs are mutually exclusive per link.

## Invariants

### Link State

- `io_refs >= 0` always.
- `io_refs == 0` implies both `io_read_state` and `io_write_state` are IDLE.
- Read and write jobs are mutually exclusive.
- `send_msg_queue_mem` tracks the canonical `send_msg_queue`.
- Link buffer-limit accounting currently uses `send_msg_queue_mem + rcvbuf_len`.
- `stat_cluster_links_memory` is updated on the main thread.

### I/O Thread MAY

- Call `connRead()` / `connWrite()` / `connAccept()` on the offloaded connection.
- Grow `rcvbuf` and update `rcvbuf_len` / `rcvbuf_alloc`.
- Scan `rcvbuf` and publish `io_rcvbuf_snapshot_len` / `io_rcvbuf_snapshot_packets`.
- Read `send_msg_queue` up to the snapped `io_last_send_block`.
- Write `io_result`, `io_nodes_sent`, `io_head_offset`, and `last_io_read_time`.
- Publish completions with `sendToMainThread()`.

### I/O Thread MUST NOT

- Touch `clusterNode`, `clusterState`, slot ownership, epoch/failover state, or any other cluster-global state.
- Call `clusterProcessPacket()` or any packet-application logic.
- Modify `io_read_state`, `io_write_state`, `io_refs`, or `async_close`.
- Pop queue nodes or update `send_msg_queue_mem`.
- Update `server.stat_cluster_links_memory`.
- Free `clusterLink`, `clusterNode`, or connection objects.

### Main Thread MUST NOT While a Job Is In Flight

- For read: touch `rcvbuf`, `rcvbuf_len`, `rcvbuf_alloc`, `io_rcvbuf_snapshot_len`, or `io_rcvbuf_snapshot_packets`.
- For write: pop already-visible queue nodes or rewrite the tracked head send offset.
- Dispatch a second read/write job for the same link.

## Dispatch Contract

All three dispatch helpers (`trySendClusterReadToIOThreads`,
`trySendClusterWriteToIOThreads`, `trySendClusterAcceptToIOThreads`) follow the
same pattern:

1. `connSetPostponeUpdateState(conn, 1)` and `connIncrRefs(conn)` before enqueue.
2. Roll back state and refs on enqueue failure.
3. Finalize postponed connection state on completion.

Return values:
- `C_OK`: work was offloaded, or an equivalent job is already pending.
- `C_ERR`: pool inactive or enqueue failed, so caller may sync-fallback.

## Deferred Teardown

If `freeClusterLink(link)` sees `io_refs > 0`:

1. Detach the link from node fields.
2. Remove read/write handlers so no new work is scheduled.
3. Set `async_close = 1`.
4. Return without closing/freeing immediately.

The actual `connClose()` and final free happen later, when the last completion
handler drops `io_refs` to `0`.

## Failure Detection

`last_io_read_time` is updated by the read worker on successful reads and is
included in the cluster node delay calculation.

```text
data_delay = now - max(node->data_received,
                       node->link->last_io_read_time,
                       node->inbound_link->last_io_read_time)
```

The I/O thread stores `last_io_read_time` with release ordering; the main thread
loads it with acquire ordering.

## Error Handling

| Result / Condition | Handling |
|--------------------|----------|
| `CLUSTER_IO_BAD_HEADER` / `CLUSTER_IO_BAD_LENGTH` | Log warning, free link immediately |
| `CLUSTER_IO_READ_ERROR` / `CLUSTER_IO_EOF` | Log debug, drain queued `rcvbuf` snapshot, then free link |
| `CLUSTER_IO_WRITE_ERROR` | Log debug, free link |
| `CONN_STATE_ACCEPTING` after accept completion | Leave connection open; TLS event flow will retry |
| Pool inactive / queue full | Sync fallback + increment `stat_cluster_io_sync_fallbacks` |

## Follow-Up

- Read completion currently drains the queued `rcvbuf` snapshot in one pass on the
  main thread and compacts the buffer after each packet.
- Follow-up work: either reintroduce bounded completion with a correct continuation
  mechanism, or keep full drain semantics but switch to a front-offset model so we
  avoid repeated per-packet `memmove()` while a large burst is being applied.
- Current behavior on `CLUSTER_IO_BAD_HEADER` / `CLUSTER_IO_BAD_LENGTH` is to close
  the link immediately, even if the same offloaded read also contained a valid
  packet prefix in the queued snapshot. This is accepted for now as an invalid-peer
  path, but if we ever need sync/offload parity here, read completion should drain
  the valid prefix before tearing the link down.
