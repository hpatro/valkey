/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "server.h"
#include "raft_state.h"

raftState *raft_state = NULL;

/* Free a batch operation structure */
static void freeEntry(void *ptr) {
    raftEntry *op = ptr;
    if (op->argv) {
        for (int i = 0; i < op->argc; i++) {
            decrRefCount(op->argv[i]);
        }
        zfree(op->argv);
    }
    if (op->argv_len) zfree(op->argv_len);
    zfree(op);
}

/* Create an entry */
raftEntry *createEntry(int dictid, robj **argv, int argc, struct serverCommand *cmd) {
    raftEntry *op = zmalloc(sizeof(raftEntry));
    op->dictid = dictid;
    op->argc = argc;
    op->cmd = cmd;
    op->argv = zmalloc(sizeof(robj *) * argc);

    /* Copy arguments */
    for (int i = 0; i < argc; i++) {
        op->argv[i] = argv[i];
        incrRefCount(argv[i]);
    }

    return op;
}

/* ================================ Initialization ============================== */

void raftStateInit(void) {
    if (raft_state) {
        serverLog(LL_WARNING, "Raft state already initialized");
        return;
    }

    raft_state = zcalloc(sizeof(raftState));

    /* Initialize persistent state */
    raft_state->current_term = 0;
    raft_state->voted_for = -1; /* -1 means no vote cast */

    /* Initialize volatile state */
    raft_state->commit_index = 0;
    raft_state->last_applied = 0;

    /* Initialize log state */
    raft_state->last_log_index = 0;
    raft_state->last_log_term = 0;

    /* Initialize node state */
    raft_state->myself->role = RAFT_ROLE_FOLLOWER;

    /* Initialize configuration */
    raft_state->election_timeout_ms = 1000;  /* Default 1 second */
    raft_state->heartbeat_interval_ms = 100; /* Default 100ms */

    /* Initialize timeouts */
    raftStateResetElectionTimeout();
    raftStateResetHeartbeatTimeout();

    /* Leader state initialized to NULL */
    raft_state->operation_log = listCreate();
    listSetFreeMethod(raft_state->operation_log, freeEntry);

    server.raft = raft_state;
    serverLog(LL_NOTICE, "Raft state initialized");
}

void raftStateAddLog(raftEntry *entry) {
    listAddNodeHead(raft_state->operation_log, entry);
    entry->index = raftStateIncrementLogIndex();
    entry->term = raftStateGetCurrentTerm();
    return;
}

void raftStateCleanup(void) {
    if (!raft_state) return;

    raftStateFreeLeaderState();
    zfree(raft_state);
    raft_state = NULL;
    serverLog(LL_NOTICE, "Raft state cleaned up");
}

void raftStateReset(void) {
    if (!raft_state) return;

    raft_state->current_term = 0;
    raft_state->voted_for = -1;
    raft_state->commit_index = 0;
    raft_state->last_applied = 0;
    raft_state->last_log_index = 0;
    raft_state->last_log_term = 0;
    raft_state->myself->role = RAFT_ROLE_FOLLOWER;

    raftStateFreeLeaderState();
    raftStateResetElectionTimeout();
    raftStateResetHeartbeatTimeout();
}

/* ================================ Term Management ============================== */

long long raftStateGetCurrentTerm(void) {
    return raft_state ? raft_state->current_term : 0;
}

void raftStateSetCurrentTerm(long long term) {
    if (term > raft_state->current_term) {
        raft_state->current_term = term;
        raft_state->voted_for = -1; /* Reset vote when term changes */
        serverLog(LL_DEBUG, "Raft: Updated term to %lld", term);
    }
}

int raftStateIncrementTerm(void) {
    raftState *rs = raft_state;
    if (!rs) return C_ERR;

    raft_state->current_term++;
    raft_state->voted_for = -1;
    serverLog(LL_DEBUG, "Raft: Incremented term to %lld", raft_state->current_term);
    return C_OK;
}

/* ================================ Commit Index Management ============================== */

long long raftStateGetCommitIndex(void) {
    return raft_state ? raft_state->commit_index : 0;
}

void raftStateSetCommitIndex(long long index) {
    if (index > raft_state->commit_index) {
        serverLog(LL_DEBUG, "Raft: Advancing commit index from %lld to %lld",
                  raft_state->commit_index, index);
        raft_state->commit_index = index;
    }
}

int raftStateCanCommit(long long index) {
    return index <= raft_state->commit_index;
}

/* ================================ Log Index Management ============================== */

long long raftStateGetLastLogIndex(void) {
    return raft_state ? raft_state->last_log_index : 0;
}

long long raftStateGetLastLogTerm(void) {
    return raft_state ? raft_state->last_log_term : 0;
}

long long raftStateGetLastApplied(void) {
    return raft_state ? raft_state->last_applied : 0;
}

void raftStateUpdateLastLog(long long index, long long term) {
    raft_state->last_log_index = index;
    raft_state->last_log_term = term;
}

long long raftStateIncrementLogIndex(void) {
    raft_state->last_log_index++;
    return raft_state->last_log_index;
}

long long raftStateIncrementLastApplied(void) {
    raft_state->last_applied++;
    return raft_state->last_applied;
}


/* ================================ Role Management ============================== */

raftRole raftStateGetRole(void) {
    return raft_state ? raft_state->myself->role : RAFT_ROLE_FOLLOWER;
}

void raftStateSetRole(raftRole role) {
    if (raft_state->myself->role != role) {
        serverLog(LL_NOTICE, "Raft: Role changed from %s to %s",
                  raftStateRoleString(raft_state->myself->role),
                  raftStateRoleString(role));
        raft_state->myself->role = role;

        /* Reset timeouts based on new role */
        if (role == RAFT_ROLE_LEADER) {
            raftStateResetHeartbeatTimeout();
        } else {
            raftStateResetElectionTimeout();
        }
    }
}

const char *raftStateRoleString(raftRole role) {
    switch (role) {
    case RAFT_ROLE_FOLLOWER: return "FOLLOWER";
    case RAFT_ROLE_CANDIDATE: return "CANDIDATE";
    case RAFT_ROLE_LEADER: return "LEADER";
    default: return "UNKNOWN";
    }
}

/* ================================ Voting ============================== */

long long raftStateGetVotedFor(void) {
    return raft_state ? raft_state->voted_for : -1;
}

void raftStateSetVotedFor(long long candidate_id) {
    raft_state->voted_for = candidate_id;
    serverLog(LL_DEBUG, "Raft: Voted for candidate %lld in term %lld",
              candidate_id, raft_state->current_term);
}

int raftStateCanVoteFor(long long candidate_id, long long term) {
    /* Can vote if:
     * 1. Haven't voted in this term, OR
     * 2. Already voted for this candidate in this term
     */
    if (term == raft_state->current_term) {
        return (raft_state->voted_for == -1 || raft_state->voted_for == candidate_id);
    }

    /* If term is newer, can vote */
    return term > raft_state->current_term;
}

/* ================================ Leader State Management ============================== */

void raftStateInitLeaderState(int num_followers) {
    /* Free existing state if any */
    raftStateFreeLeaderState();
    /* TODO: Initialize replica nodes */
    serverLog(LL_DEBUG, "Raft: Initialized leader state for %d followers", num_followers);
}

void raftStateFreeLeaderState(void) {
    return;
}

long long raftStateGetNextIndex(int follower_idx) {
    UNUSED(follower_idx);
    return 0;
}

void raftStateSetNextIndex(int follower_idx, long long index) {
    UNUSED(follower_idx);
    UNUSED(index);
}

long long raftStateGetMatchIndex(int follower_idx) {
    UNUSED(follower_idx);
    return 0;
}

void raftStateSetMatchIndex(int follower_idx, long long index) {
    UNUSED(follower_idx);
    UNUSED(index);
}

/* ================================ Timeout Management ============================== */

void raftStateResetElectionTimeout(void) {
    /* Add randomization to prevent split votes */
    int jitter = rand() % (raft_state->election_timeout_ms / 2);
    raft_state->election_timeout = mstime() + raft_state->election_timeout_ms + jitter;
}

void raftStateResetHeartbeatTimeout(void) {
    raft_state->heartbeat_timeout = mstime() + raft_state->heartbeat_interval_ms;
}

int raftStateIsElectionTimeout(void) {
    return mstime() >= raft_state->election_timeout;
}

int raftStateIsHeartbeatTimeout(void) {
    return mstime() >= raft_state->heartbeat_timeout;
}

/* ================================ Quorum Calculation ============================== */

int raftStateCalculateQuorum(int num_nodes) {
    return (num_nodes / 2) + 1;
}

int raftStateHasQuorum(long long index, int num_nodes) {
    UNUSED(index);
    UNUSED(num_nodes);
    return 0;
}

/* Comparator for descending sort of log indices */
static int compareIndicesDesc(const void *a, const void *b) {
    long long ia = *(const long long *)a;
    long long ib = *(const long long *)b;
    if (ia > ib) return -1;
    if (ia < ib) return 1;
    return 0;
}

/**
 * Calculate the quorum index based on current match indices.
 *
 * The quorum index is the highest log index that has been replicated
 * to a quorum of nodes (including the primary). This is calculated by:
 * 1. Collecting all match indices from replicas
 * 2. Including the primary's last_log_index
 * 3. Sorting in descending order
 * 4. Selecting the Nth highest where N equals the quorum size
 *
 * @return The quorum index, or 0 if Raft state is not initialized
 */
long long raftStateGetQuorumIndex(void) {
    /* Determine number of replicas from server.replicas list */
    int num_replicas = listLength(server.replicas);
    int total_nodes = num_replicas + 1;

    /* Handle edge case: no replicas, return primary's index */
    if (num_replicas == 0) {
        serverLog(LL_DEBUG, "Raft: No replicas, quorum index equals last_log_index %lld", raft_state->last_log_index);
        return raft_state->last_log_index;
    }

    /* Allocate array for all indices (replicas + primary) */
    long long *indices = zmalloc(sizeof(long long) * total_nodes);

    /* Initialize array to avoid invalid sort operation */
    memset(indices, 0, sizeof(long long) * total_nodes);

    /* Collect match indices from replicas */
    for (int i = 0; i < num_replicas; i++) {
        indices[i] = raft_state->replicas[i]->match_index;
    }

    /* Add primary's index */
    indices[num_replicas] = raft_state->last_log_index;

    /* Sort in descending order */
    qsort(indices, total_nodes, sizeof(long long), compareIndicesDesc);

    /* Get quorum index (Nth highest where N = quorum_size) */
    int quorum_size = raftStateCalculateQuorum(total_nodes);
    long long quorum_index = indices[quorum_size - 1];

    serverLog(LL_DEBUG, "Raft: Calculated quorum index %lld (nodes: %d, quorum: %d, last_log: %lld)",
              quorum_index, total_nodes, quorum_size, raft_state->last_log_index);

    /* Validate: quorum index should not exceed primary's index */
    if (quorum_index > raft_state->last_log_index) {
        serverLog(LL_WARNING, "Raft: Quorum index %lld exceeds last_log_index %lld, capping to last_log_index",
                  quorum_index, raft_state->last_log_index);
        quorum_index = raft_state->last_log_index;
    }

    zfree(indices);

    return quorum_index;
}

/**
 * Update the quorum index and commit index based on current match indices.
 *
 * This function should be called after any match index update to determine
 * if the commit index can advance. The commit index will only advance if
 * the new quorum index is higher than the current commit index.
 *
 * @return 1 if commit index advanced, 0 otherwise
 */
__attribute__((unused))
static int raftStateUpdateQuorumIndex(void) {
    long long old_commit = raft_state->commit_index;
    long long new_quorum = raftStateGetQuorumIndex();

    if (new_quorum > old_commit) {
        raftStateSetCommitIndex(new_quorum);
        serverLog(LL_DEBUG, "Raft: Quorum index advanced from %lld to %lld",
                  old_commit, new_quorum);
        return 1;
    }

    return 0;
}

/**
 * Check if a specific log index has achieved quorum.
 *
 * This helper function determines whether a given log index has been
 * replicated to a quorum of nodes by comparing it against the current
 * quorum index.
 *
 * @param index The log index to check
 * @return 1 if index has achieved quorum, 0 otherwise
 */
__attribute__((unused))
static int raftStateHasQuorumForIndex(long long index) {
    long long quorum_index = raftStateGetQuorumIndex();
    return index <= quorum_index;
}

/* ================================ State Validation ============================== */

int raftStateValidate(void) {
    /* Validate term is non-negative */
    if (raft_state->current_term < 0) {
        serverLog(LL_WARNING, "Raft: Invalid current_term: %lld", raft_state->current_term);
        return C_ERR;
    }

    /* Validate commit_index <= last_log_index */
    if (raft_state->commit_index > raft_state->last_log_index) {
        serverLog(LL_WARNING, "Raft: commit_index (%lld) > last_log_index (%lld)",
                  raft_state->commit_index, raft_state->last_log_index);
        return C_ERR;
    }

    /* Validate last_applied <= commit_index */
    if (raft_state->last_applied > raft_state->commit_index) {
        serverLog(LL_WARNING, "Raft: last_applied (%lld) > commit_index (%lld)",
                  raft_state->last_applied, raft_state->commit_index);
        return C_ERR;
    }

    return C_OK;
}

/* ================================ Debug and Logging ============================== */
sds raftStateToString(void) {
    raftState *rs = raft_state;
    if (!rs) return sdsnew("NULL");

    return sdscatprintf(sdsempty(),
                        "role=%s term=%lld voted_for=%lld commit=%lld applied=%lld "
                        "last_log_idx=%lld last_log_term=%lld",
                        raftStateRoleString(raft_state->myself->role),
                        raft_state->current_term,
                        raft_state->voted_for,
                        raft_state->commit_index,
                        raft_state->last_applied,
                        raft_state->last_log_index,
                        raft_state->last_log_term);
}
