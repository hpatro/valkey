# Cluster Bus I/O Offload

## Overview

Offloads cluster bus read, write, and TLS accept operations from the main
thread onto the I/O-thread pool (PR #3324 queue-based model), while keeping
all cluster state mutation on the main thread.

Jobs flow through the shared SPMC work queue (`io_shared_inbox`) and
completions return via the shared MPSC response queue (`io_shared_outbox`).
When the pool is inactive or the queue is full, all paths fall back to
synchronous I/O on the main thread.

## Architecture

```
+---------------------------+       +-------------------------------+       +---------------------------+
|       Main Thread         |       |        Shared Queues          |       |     I/O Thread Pool       |
|                           |       |                               |       |                           |
| Dispatch Logic            |       | spmcQueue io_shared_inbox     |       | clusterReadJob            |
|  (beforeSleep/clusterCron)|------>|  (SPMC: main -> I/O threads)  |------>|  (read + frame)           |
|                           |       |                               |       |                           |
| Response Drainer          |       | mpscQueue io_shared_outbox    |       | clusterWriteJob           |
|  (beforeSleep/blocked)    |<------|  (MPSC: I/O threads -> main)  |<------|  (write from inflight)    |
|                           |       |                               |       |                           |
| Packet Application        |       +-------------------------------+       | clusterAcceptJob          |
|  (clusterProcessPacket)   |                                               |  (TLS handshake)          |
|                           |                                               +---------------------------+
| Write Completion Handler  |
| Accept Completion Handler |
| freeClusterLink           |
|  (deferred if io_refs > 0)|
+---------------------------+
```

## Key Design Decisions

1. Reuse PR #3324 shared SPMC/MPSC queues — no cluster-specific threading primitives.
2. I/O threads frame packets (signature/length validation only); main thread applies them.
3. Write path swaps `send_msg_queue` ↔ `send_msg_queue_inflight` in O(1); I/O thread writes from inflight only.
4. Deferred teardown via `io_refs` counter and `async_close` flag. TLS close defers SSL teardown when `connHasRefs()`.
5. Dispatch holds a connection ref (`connIncrRefs`) and postpones state updates (`connSetPostponeUpdateState`).
6. Accept offload operates on bare `connection *` — `clusterLink` is created only after successful handshake.
7. `ConnOwnerKind` discriminator guards client-specific casts in `socket.c` safety assertions.

## Data Flow: Read Path

```
  Main Thread                    Work Queue       I/O Thread              Response Queue
  |                                  |                |                        |
  | clusterLink readable fires       |                |                        |
  | Check io_read_state == IDLE      |                |                        |
  | Set io_read_state = PENDING      |                |                        |
  | io_refs++                        |                |                        |
  |--- push clusterReadJob(link) --->|                |                        |
  |                                  |--- dequeue --->|                        |
  |                                  |                | connRead() into rcvbuf |
  |                                  |                | Frame packets          |
  |                                  |                |  (<=16 pkts or <=64KB) |
  |                                  |                | Update last_io_read_time
  |                                  |                |--- post completion --->|
  |<-------------- drain completions in beforeSleep --|------------------------|
  | io_refs--, io_read_state = IDLE  |                |                        |
  | Apply <=64 packets or <=250us    |                |                        |
  |                                  |                |                        |
  | [If async_close && io_refs==0: final free]        |                        |
```

## Data Flow: Write Path

```
  Main Thread                    Work Queue       I/O Thread              Response Queue
  |                                  |                |                        |
  | clusterSendMessage appends       |                |                        |
  |  to send_msg_queue (pending)     |                |                        |
  | Check io_write_state == IDLE     |                |                        |
  |  && io_read_state == IDLE        |                |                        |
  |  && pending non-empty            |                |                        |
  | Swap pending <-> inflight (O(1)) |                |                        |
  | Set io_write_state = PENDING     |                |                        |
  | io_refs++                        |                |                        |
  |--- push clusterWriteJob(link) -->|                |                        |
  |                                  |--- dequeue --->|                        |
  |                                  |                | connWrite() from       |
  |                                  |                |  inflight at offset    |
  |                                  |                |--- post completion --->|
  |<-------------- drain completions ------------------|------------------------|
  | io_refs--, advance inflight      |                |                        |
  | Pop fully-sent nodes             |                |                        |
  | Update offset for partial        |                |                        |
  |                                  |                |                        |
  | [If more data: reschedule write job]              |                        |
```

## Data Flow: Accept Path (TLS)

```
  Main Thread                    Work Queue       I/O Thread              Response Queue
  |                                  |                |                        |
  | clusterAcceptHandler:            |                |                        |
  |  connCreateAccepted(conn)        |                |                        |
  | Check CONN_FLAG_ALLOW_ACCEPT_    |                |                        |
  |  OFFLOAD                         |                |                        |
  |--- trySendClusterAcceptTo ------>|                |                        |
  |    IOThreads(conn)               |                |                        |
  |                                  |--- dequeue --->|                        |
  |                                  |                | connAccept(conn)       |
  |                                  |                |  (TLS handshake)       |
  |                                  |                |--- post completion --->|
  |<-- drain in processIOThreadsResponses ------------|------------------------|
  |                                  |                |                        |
  | [Accept succeeded]:              |                |                        |
  |  Create clusterLink              |                |                        |
  |  Set conn->private_data          |                |                        |
  |  Install clusterReadHandler      |                |                        |
  |                                  |                |                        |
  | [Accept failed]:                 |                |                        |
  |  connClose(conn)                 |                |                        |
```

## clusterLink I/O State

```
  IDLE --[dispatch]--> READ_PENDING or WRITE_PENDING --[completion]--> IDLE
                                    |
                          [freeClusterLink with io_refs > 0]
                                    |
                              ASYNC_CLOSE --[last completion, io_refs==0]--> FREED
```

Read and write PENDING are mutually exclusive — at most one I/O job per link.

## Invariants

### Link State
- `io_refs >= 0` always; `io_refs == 0` implies both states IDLE.
- Read PENDING and write PENDING are mutually exclusive.
- `send_msg_queue_mem` = combined memory of `send_msg_queue` + `send_msg_queue_inflight`.
- `stat_cluster_links_memory` updated on main thread only.
- `framed_packets_mem` bounded to one read job's output (≤16 packets or ≤64 KB).

### I/O Thread MAY
- `connRead()`/`connWrite()`/`connAccept()` on the offloaded connection
- Grow `link->rcvbuf`/`rcvbuf_len`/`rcvbuf_alloc`
- Append to `link->framed_packets`, update `framed_packets_mem`
- Read `send_msg_queue_inflight`; write `inflight_nodes_sent`, `inflight_send_offset`
- Write `io_result`, `last_io_read_time` (atomic)
- Call `sendToMainThread()`, `mstime()`

### I/O Thread MUST NOT
- Touch `clusterNode`, `clusterState`, slot/epoch/failover state, `todo_before_sleep`
- Call `clusterProcessPacket()` or any state-mutating function
- Modify `io_read_state`, `io_write_state`, `io_refs`, `async_close`
- Append to or pop from `send_msg_queue` (pending queue)
- Modify `send_msg_queue_mem` or `stat_cluster_links_memory`
- Free `clusterLink`, `clusterNode`, or connection objects

### Main Thread MUST NOT (while job in flight)
- Touch `rcvbuf`/`rcvbuf_len`/`rcvbuf_alloc`, `framed_packets`/`framed_packets_mem`
- Touch `send_msg_queue_inflight`/`inflight_send_offset`
- Dispatch a second job for the same link

## Dispatch Contract

All three dispatch functions (`trySendCluster{Read,Write,Accept}ToIOThreads`):
1. `connSetPostponeUpdateState(conn, 1)` + `connIncrRefs(conn)` before enqueue
2. On failure: rollback refs and state postponement
3. On completion: `connSetPostponeUpdateState(conn, 0)` + `connUpdateState(conn)` + `connDecrRefs(conn)`

Return `C_OK` if offloaded or if a job is already pending (prevents sync fallback on busy link).
Return `C_ERR` if pool inactive or queue full (caller falls back to sync).

## Deferred Teardown

`freeClusterLink` with `io_refs > 0`:
1. Set `async_close = 1`, detach from node, close connection (causes in-flight I/O to fail)
2. Return 0 (deferred)
3. Completion handler decrements `io_refs`; if 0 and `async_close`: final free

TLS: `connTLSClose` defers `SSL_shutdown`/`SSL_free` when `connHasRefs()` is true.

## Failure Detection

`data_delay = now - max(node->data_received, link->last_io_read_time, inbound_link->last_io_read_time)`

All timestamps use `mstime()`. I/O thread writes `last_io_read_time` with `memory_order_release`;
main thread reads with `memory_order_acquire`.

## Error Handling

| Error | Handling |
|-------|----------|
| `CLUSTER_IO_BAD_HEADER/BAD_LENGTH` | Log warning, `freeClusterLink` |
| `CLUSTER_IO_READ_ERROR/EOF` | Log debug, `freeClusterLink` |
| `CLUSTER_IO_WRITE_ERROR` | Log debug, `freeClusterLink` |
| `CLUSTER_IO_ACCEPT_ERROR` | Log verbose, `connClose(conn)` |
| Queue full / pool inactive | Sync fallback, increment `stat_cluster_io_sync_fallbacks` |
