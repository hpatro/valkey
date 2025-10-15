# Valkey Cluster Backend Modularization Specification

## 1. Purpose

This document proposes an abstraction layer that allows the Valkey cluster
subsystem to support multiple clustering backends. Today the logic in
`src/cluster.c` implements core/shared facilities, while
`src/cluster_legacy.c` contains the cluster bus specific behavior. The
immediate goal is to define a clear backend API so that the legacy backend can
co-exist with future implementations (e.g. out-of-process coordinators or
alternate transport protocols) without invasive changes to the shared cluster
code.

## 2. Scope

The specification only covers the backend-facing API between the shared
cluster core (currently `cluster.c`) and a pluggable backend (currently the
logic in `cluster_legacy.c`). Topics specifically **in scope** include:

* Initialization and lifecycle hooks (`clusterInit`, `clusterCron`, teardown).
* Management of node membership (add/remove/update operations).
* Dissemination of node state changes (fail markers, slot ownership, role).
* Surfaces needed by the cluster core to ask the backend to perform network
  I/O or to notify the core of backend-driven events.

Topics **out of scope** for this iteration include: redesigning the gossip or
transport protocol, changing the cluster configuration on disk, reworking
client-facing commands, or changing the shared data structures that are
already used by the cluster core (`clusterNode`, `clusterShard`, etc.).

## 3. Terminology

* **Cluster core** – the logic in `cluster.c` that holds shared structures,
  performs hash slot accounting, manages failover state machines, etc.
* **Cluster backend** – a module providing the transport and coordination
  mechanics that were previously hardcoded in `cluster_legacy.c`.
* **Backend handle** – opaque pointer owned by the backend that the core uses
  to address backend-specific state (connections, timers, etc.).

## 4. High-level architecture

1. Introduce a `clusterBackend` vtable structure that `cluster.c` depends on.
   The structure is populated by the active backend during server startup.
2. `cluster.c` owns the lifecycle of cluster state and forwards events to the
   backend via the vtable. It also exposes callbacks that allow the backend to
   notify the core about asynchronous events (gossip messages, timeouts).
3. Backends live in dedicated compilation units (e.g. `cluster_legacy.c`)
   and register themselves through a `clusterRegisterBackend()` helper.
4. Future work may allow runtime selection, but initially the legacy backend
   registers itself during `server.c` startup.

```
+------------------+        +---------------------+
| cluster.c        | <----> | struct clusterBackend |
| (core services)  |        | (vtable + context)    |
+------------------+        +---------------------+
                               ^
                               |
                     +---------------------+
                     | cluster_legacy.c    |
                     +---------------------+
```

## 5. Core-to-backend API surface

The shared core holds a global pointer `clusterBackend *server.cluster_backend`.
The proposed vtable is outlined below. Return values use `C_OK`/`C_ERR` and
populate `server.cluster_backend->last_error` for richer diagnostics when
needed.

```c
typedef struct clusterBackendAPI {
    const char *name;                  /* Human-friendly identifier */
    uint32_t capabilities;             /* Feature flags */

    /* Lifecycle ----------------------------------------------------------- */
    int (*init)(struct clusterContext *ctx);              /* Called from clusterInit */
    void (*deinit)(struct clusterContext *ctx);           /* Called during shutdown */
    void (*cron)(struct clusterContext *ctx, mstime_t now); /* Invoked from clusterCron */

    /* Membership management ----------------------------------------------- */
    int (*add_node)(struct clusterContext *ctx,
                    const clusterNodeDescriptor *desc,
                    clusterNodeHandle **out_handle);
    int (*remove_node)(struct clusterContext *ctx,
                       clusterNodeHandle *handle,
                       clusterNodeRemovalReason reason);

    /* State mutation ------------------------------------------------------ */
    int (*update_role)(struct clusterContext *ctx,
                       clusterNodeHandle *handle,
                       clusterNodeRole new_role,
                       clusterNodeRoleChangeFlags flags);
    int (*update_slots)(struct clusterContext *ctx,
                        clusterNodeHandle *handle,
                        const clusterSlotDelta *delta);
    int (*mark_fail)(struct clusterContext *ctx,
                     clusterNodeHandle *handle,
                     clusterFailFlags flags);

    /* Messaging ----------------------------------------------------------- */
    int (*broadcast_update)(struct clusterContext *ctx,
                            const clusterTopologyDigest *digest);
    int (*request_failover_auth)(struct clusterContext *ctx,
                                 clusterNodeHandle *requester);

    /* Debug & observability ----------------------------------------------- */
    sds (*describe)(struct clusterContext *ctx); /* Optional human-readable state */
} clusterBackendAPI;
```

Supporting types that need to be introduced in `cluster.h`:

* `clusterContext` – lightweight wrapper with pointers to the server
  configuration, node dictionaries, and shared helpers (e.g. scheduling IO).
* `clusterNodeDescriptor` – input structure containing immutable properties
  required to create a node (ID, addresses, flags, initial role).
* `clusterNodeHandle` – opaque backend-owned reference returned from
  `add_node` and passed back on updates/removal.
* `clusterSlotDelta` – structure describing slot ownership/intents to migrate
  (e.g. primary slots, importing, migrating bitmaps).
* `clusterNodeRoleChangeFlags` – indicates whether role change implies slot
  closure, forced replica promotion, etc.
* `clusterFailFlags` – reason for fail marking (manual, timeout, data loss).
* `clusterTopologyDigest` – summary used for periodic gossip (can initially
  wrap existing `clusterMsg` structures).
* `clusterNodeRemovalReason` – enumerates voluntary leave, handshake timeout,
  etc., letting the backend tailor messaging.

## 6. Backend-to-core callbacks

To keep the core as the source of truth, the backend must not mutate shared
cluster structures directly. Instead, it notifies the core via a callback
vector supplied in the `clusterContext`. The context exposes function pointers
similar to:

```c
typedef struct clusterBackendCallbacks {
    void (*on_node_event)(clusterNodeHandle *handle, clusterNodeEvent event,
                          const clusterNodeState *state);
    void (*on_slot_update)(clusterNodeHandle *handle,
                           const clusterSlotState *state);
    void (*on_message)(clusterNodeHandle *sender,
                       const clusterBackendMessage *message);
    void (*schedule_cron)(mstime_t delay); /* ask core to re-run cron sooner */
} clusterBackendCallbacks;
```

The backend receives a pointer to `clusterBackendCallbacks` inside
`clusterContext`. The existing logic that processes gossip, failover,
config epoch, and slot migration remains in `cluster.c`, using these callbacks
as the sole entry point for backend-driven changes.

## 7. Node management flows

### 7.1 Add node

1. Core parses user input or handshake packet into a `clusterNodeDescriptor`.
2. Core allocates/initializes a `clusterNode` and inserts it into dictionaries.
3. Core invokes `backend->add_node(ctx, &desc, &handle)` to allow the backend
   to set up connections, handshake timers, etc.
4. On success, the handle is stored in the `clusterNode` structure for future
   calls. On failure, the node is removed and the error propagated.

### 7.2 Remove node

1. Core decides to remove a node (manual command, handshake timeout, etc.).
2. Core removes the node from dictionaries and updates slot ownership.
3. Core invokes `backend->remove_node(ctx, handle, reason)` to close links,
   cancel timers, and broadcast the departure.

### 7.3 Update node state

* `mark_fail` – triggered when the core concludes a node is failing. Backend
  is responsible for broadcasting FAIL messages or equivalent transport
  semantics.
* `update_slots` – used whenever slot ownership/import/migrate state changes.
  The delta structure lets the backend decide whether to send incremental or
  full updates.
* `update_role` – called after promotion/demotion decisions. Flags allow the
  backend to express requirements like forcing slot closure when a replica is
  promoted.

All update operations return `C_OK`/`C_ERR`. Failures are logged and surfaced
as cluster events to aid troubleshooting.

## 8. Lifecycle hooks

* `init` replaces the ad-hoc initialization in `clusterInit`. The backend may
  register file events, spawn background tasks, or load persisted metadata.
* `cron` is invoked from `clusterCron` with the current time. Backends should
  keep `cron` light and offload heavy work to asynchronous routines or request
  rescheduling via `schedule_cron` callback.
* `deinit` allows clean shutdown during `clusterShutdown` / `serverCron`
  teardown.

## 9. Capability flags

To ease incremental adoption, the API exposes `capabilities` bit flags. The
core can use this to enable/disable features such as:

* `CLUSTER_CAP_DYNAMIC_MIGRATION` – backend supports concurrent slot
  resharding notifications.
* `CLUSTER_CAP_EXTERNAL_COORDINATOR` – backend defers failover decisions to an
  external system, so the core should skip certain heuristics.
* `CLUSTER_CAP_MULTI_TRANSPORT` – backend can serve TLS/non-TLS simultaneously.

The legacy backend initially advertises zero capabilities to maintain current
behavior.

## 10. Error handling & observability

* All backend calls must be non-blocking and re-entrant with respect to the
  event loop.
* Errors returned from backend functions should be logged with the backend
  name and a human-readable detail string retrievable through `describe()`.
* The backend may expose counters (messages sent, failed links) through the
  `server.cluster_stats` struct; wiring this is outside the current scope but
  should be kept in mind while designing data structures.

## 11. Migration plan

1. Introduce the new header definitions (`cluster_backend.h`) with stubs.
2. Update `cluster.c` to depend on the vtable instead of calling legacy
   helpers directly (initially the vtable forwards to the same legacy
   functions).
3. Gradually refactor `cluster_legacy.c` to implement the new backend API.
4. Once parity is achieved, other backends can be implemented by registering
   their own `clusterBackendAPI` instance.

## 12. Open questions

* Should node handles map 1:1 with `clusterNode` or allow backends to coalesce
  nodes (e.g. per-shard handles)? Initially 1:1 seems simplest.
* Do we need explicit async result handling for operations like `update_slots`
  that may require network round trips? The first version can be synchronous,
  but future iterations might need promises or callbacks.
* How should persistence of backend-specific metadata be handled? This spec
  assumes the backend owns it and hooks into RDB/AOF via existing module
  interfaces.

---

This specification focuses on defining a narrow, testable contract between the
cluster core and its backend implementation. Additional polishing—such as
final naming, header layout, and telemetry integration—can be addressed when
porting the legacy backend onto this API.
