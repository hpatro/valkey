# Valkey Active-Active Replication

## Overview

**What problem are we solving?**
Valkey today has a single writable primary per shard. Serving a globally distributed application therefore forces one of two painful choices: route every write across to one primary region while paying tens to hundreds of milliseconds of cross-region latency on the write path and losing all write availability when that region is unreachable or stand up an external replication pipeline (a message broker plus a custom sync service) that mirrors data between regions. The external pipeline adds operational surface, extra network hops, its own failure modes, and a lag/consistency model that lives outside Valkey and outside the commands users already know. Neither option lets an application in any region read and write its local Valkey at low latency while staying converged with its peers.

**Who is the user?**
Operators running Valkey behind globally distributed applications for use cases like multi-region session stores, user profiles, shopping carts, feature flags, counters, rate limiters, and edge caches. The users who need local read/write latency in every region and continued availability during a regional outage or cross-region partition. These customers are willing to accept eventual (rather than strong) consistency in exchange for local latency and always-writable regions, and they do not want to build or operate a separate cross-region sync tier to get it.

**What is the proposed solution?**
We will add native active-active replication to Valkey geographically separated deployments that each accept client writes for the same keyspace and exchange updates asynchronously over Valkey's own replication transport. Rather than shipping raw commands, each region derives a deterministic effect from every write and ships that, tagged with causal metadata; peers merge effects using per-type Conflict-Free Replicated Data Types (CRDTs) so any delivery order converges to the same state. There is no external broker or sync service on the write path i.e. cross-region replication becomes a first-class Valkey feature reusing replication layer, the keyspace store, cluster slots, and RDB/AOF.

## Goal

The overall goal of active-active replication is to let every region serve local reads and writes at low latency while all regions converge to the same state, without an external replication pipeline and without changing the commands users already know.

Our specific design goals include:

* **Native, first-class cross-region replication.** Replication between regions is built into Valkey and rides its existing replication transport — no external dependency on the write path.
* **Unified read/write interface via Valkey.** Applications keep using the standard Valkey command surface. Every write command is supported natively. There is no separate API, SDK, or query path for the multi-region case.
* **One model for standalone and cluster.** The same active-active pattern applies to a single primary/replica pair and to a sharded cluster, with the cluster case running the pattern once per shard.
* **Local writes in every region.** Each region accepts client writes locally and acknowledges them locally; a lagging or partitioned peer never blocks a local write.
* **Eventual consistency with no silent data loss.** Concurrent, conflicting writes converge deterministically via CRDTs. Where a CRDT rule can preserve an acknowledged write it always does; where convergence forces a choice (e.g. last-writer-wins), the loss is surfaced via a conflict counter, never hidden.

## Requirements

The following captures the current V1 direction based on discussions so far. This is intended to define the starting scope, not close all design details. The five headline requirements below (R1–R5) are the acceptance bar for active-active; the remaining requirements refine them.

### V1 Requirements

#### Native Replication
* **R1. Native cross-DC/region replication.** Replication between regions is a first-class Valkey feature rather than an external pipeline — eliminating the intermediate service and message-broker hops on the write path, with no external dependency. Peer links reuse the existing replication transport layer.

#### Interface Compatibility
* **R2. Unified interface for reads/writes via Valkey.** All commands and features are driven through the standard Valkey interface; there is no proxy or separate API for multi-region use. Write commands are supported natively via effect derivation and merge.
* **R2a. No client changes.** Existing clients read and write against their local region unchanged; local reads never block on remote regions.

#### Topology
* **R3. Standalone and cluster mode.** The same model applies whether the deployment is a single primary/replica pair or a sharded cluster. In cluster mode the pattern runs once per shard; the cluster bus stays region-local and slot ownership must match across regions per shard.

#### Write Availability
* **R4. Writes across regions.** Every region accepts client writes locally and acknowledges them locally (local-durable). A lagging or partitioned peer must never block a local write; both sides of a partition remain writable and converge on heal.

#### Consistency
* **R5. Eventual consistency across regions.** Data converges to a single deterministic state across all servers via CRDTs. Merge is commutative, associative, and idempotent so any delivery order converges (strong eventual consistency).
* **R5a. Deterministic, non-lossy-where-possible conflict resolution.** Concurrent conflicting writes resolve by fixed per-type rules (add-wins sets, PN-counters, LWW registers with HLC + instance-ID tiebreak, RGA lists). No acknowledged write is silently dropped where a CRDT rule can preserve it; forced losses are counted and observable.

#### Persistence & Recovery
* **R6. Persistence.** RDB serializes additional metadata to restore data or to support full sync on a bootstrapped node. AOF records additional metadata so replay reconstructs identical state. Reconnect after a partition delta-resumes from the per-peer backlog or performs a merge-on-load full sync.

#### Observability
* **R7. Runtime metrics.** Expose per-peer link state and lag, sent/acked offsets, pending effects, conflict counters (LWW / type / resurrection).