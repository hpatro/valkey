# Cluster Bus I/O Threading Notes

This tree now includes the queue-based I/O thread substrate from `#3324`.
Cluster bus offload work should build on that substrate rather than reintroducing
main-thread polling lists.

Current baseline assumptions:

- Main to I/O dispatch uses the shared `spmcQueue` in `queues.h` plus the per-thread `spscQueue` inboxes in `io_threads.c`.
- I/O to main completions use the shared `mpscQueue` outbox drained by `processIOThreadsResponses()` before `clusterBeforeSleep()`.
- Job routing is tag-based (`tagJob()` / `untagJob()`), so cluster jobs and responses should add new tags instead of adding cluster-specific pending-I/O lists.
- Cluster state ownership stays on the main thread. Worker-side cluster work is limited to socket/TLS I/O, bounded framing, bounded writes from the frozen inflight queue, and completion reporting.
- Cluster TLS accept offload reuses the existing accept request tag with owner-tagged `connection *` payloads and a cluster-specific completion tag, keeping the tagged-pointer request space bounded.
- `clusterLink` lifetime must use explicit in-flight reference tracking and `async_close`; worker threads must never free a link directly.
- Cluster write offload uses `pending` and `inflight` queue rotation instead of copied immutable write snapshots.
