/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "server.h"
#include "raft_state.h"

/* ================================ Initialization ============================== */

void raftStateInit(void) {
    if (server.raft) {
        serverLog(LL_WARNING, "Raft state already initialized");
        return;
    }

    raftState *rs = zcalloc(sizeof(raftState));

    /* Initialize persistent state */
    rs->current_term = 0;
    rs->voted_for = -1; /* -1 means no vote cast */

    /* Initialize volatile state */
    rs->commit_index = 0;
    rs->last_applied = 0;

    /* Initialize log state */
    rs->last_log_index = 0;
    rs->last_log_term = 0;

    /* Initialize node state */
    rs->role = RAFT_ROLE_FOLLOWER;

    /* Initialize configuration */
    rs->election_timeout_ms = 1000;  /* Default 1 second */
    rs->heartbeat_interval_ms = 100; /* Default 100ms */

    /* Initialize timeouts */
    raftStateResetElectionTimeout();
    raftStateResetHeartbeatTimeout();

    /* Leader state initialized to NULL */
    rs->next_index = NULL;
    rs->match_index = NULL;

    server.raft = rs;
    serverLog(LL_NOTICE, "Raft state initialized");
}

void raftStateCleanup(void) {
    if (!server.raft) return;

    raftStateFreeLeaderState();
    zfree(server.raft);
    server.raft = NULL;
    serverLog(LL_NOTICE, "Raft state cleaned up");
}

void raftStateReset(void) {
    raftState *rs = server.raft;
    if (!rs) return;

    rs->current_term = 0;
    rs->voted_for = -1;
    rs->commit_index = 0;
    rs->last_applied = 0;
    rs->last_log_index = 0;
    rs->last_log_term = 0;
    rs->role = RAFT_ROLE_FOLLOWER;

    raftStateFreeLeaderState();
    raftStateResetElectionTimeout();
    raftStateResetHeartbeatTimeout();
}

/* ================================ Term Management ============================== */

long long raftStateGetCurrentTerm(void) {
    return server.raft ? server.raft->current_term : 0;
}

void raftStateSetCurrentTerm(long long term) {
    raftState *rs = server.raft;
    if (!rs) return;

    if (term > rs->current_term) {
        rs->current_term = term;
        rs->voted_for = -1; /* Reset vote when term changes */
        serverLog(LL_DEBUG, "Raft: Updated term to %lld", term);
    }
}

int raftStateIncrementTerm(void) {
    raftState *rs = server.raft;
    if (!rs) return C_ERR;

    rs->current_term++;
    rs->voted_for = -1;
    serverLog(LL_DEBUG, "Raft: Incremented term to %lld", rs->current_term);
    return C_OK;
}

/* ================================ Commit Index Management ============================== */

long long raftStateGetCommitIndex(void) {
    return server.raft ? server.raft->commit_index : 0;
}

void raftStateSetCommitIndex(long long index) {
    raftState *rs = server.raft;
    if (!rs) return;

    if (index > rs->commit_index) {
        serverLog(LL_DEBUG, "Raft: Advancing commit index from %lld to %lld",
                  rs->commit_index, index);
        rs->commit_index = index;
    }
}

int raftStateCanCommit(long long index) {
    raftState *rs = server.raft;
    if (!rs) return 0;
    return index <= rs->commit_index;
}

/* ================================ Log Index Management ============================== */

long long raftStateGetLastLogIndex(void) {
    return server.raft ? server.raft->last_log_index : 0;
}

long long raftStateGetLastLogTerm(void) {
    return server.raft ? server.raft->last_log_term : 0;
}

void raftStateUpdateLastLog(long long index, long long term) {
    raftState *rs = server.raft;
    if (!rs) return;

    rs->last_log_index = index;
    rs->last_log_term = term;
}

long long raftStateIncrementLogIndex(void) {
    raftState *rs = server.raft;
    if (!rs) return 0;

    rs->last_log_index++;
    return rs->last_log_index;
}

/* ================================ Role Management ============================== */

raftRole raftStateGetRole(void) {
    return server.raft ? server.raft->role : RAFT_ROLE_FOLLOWER;
}

void raftStateSetRole(raftRole role) {
    raftState *rs = server.raft;
    if (!rs) return;

    if (rs->role != role) {
        serverLog(LL_NOTICE, "Raft: Role changed from %s to %s",
                  raftStateRoleString(rs->role),
                  raftStateRoleString(role));
        rs->role = role;

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
    return server.raft ? server.raft->voted_for : -1;
}

void raftStateSetVotedFor(long long candidate_id) {
    raftState *rs = server.raft;
    if (!rs) return;

    rs->voted_for = candidate_id;
    serverLog(LL_DEBUG, "Raft: Voted for candidate %lld in term %lld",
              candidate_id, rs->current_term);
}

int raftStateCanVoteFor(long long candidate_id, long long term) {
    raftState *rs = server.raft;
    if (!rs) return 0;

    /* Can vote if:
     * 1. Haven't voted in this term, OR
     * 2. Already voted for this candidate in this term
     */
    if (term == rs->current_term) {
        return (rs->voted_for == -1 || rs->voted_for == candidate_id);
    }

    /* If term is newer, can vote */
    return term > rs->current_term;
}

/* ================================ Leader State Management ============================== */

void raftStateInitLeaderState(int num_followers) {
    raftState *rs = server.raft;
    if (!rs) return;

    /* Free existing state if any */
    raftStateFreeLeaderState();

    if (num_followers > 0) {
        rs->next_index = zcalloc(sizeof(long long) * num_followers);
        rs->match_index = zcalloc(sizeof(long long) * num_followers);

        /* Initialize next_index to last_log_index + 1 */
        /* Initialize match_index to 0 */
        for (int i = 0; i < num_followers; i++) {
            rs->next_index[i] = rs->last_log_index + 1;
            rs->match_index[i] = 0;
        }

        serverLog(LL_DEBUG, "Raft: Initialized leader state for %d followers", num_followers);
    }
}

void raftStateFreeLeaderState(void) {
    raftState *rs = server.raft;
    if (!rs) return;

    if (rs->next_index) {
        zfree(rs->next_index);
        rs->next_index = NULL;
    }

    if (rs->match_index) {
        zfree(rs->match_index);
        rs->match_index = NULL;
    }
}

long long raftStateGetNextIndex(int follower_idx) {
    raftState *rs = server.raft;
    if (!rs || !rs->next_index) return 0;
    return rs->next_index[follower_idx];
}

void raftStateSetNextIndex(int follower_idx, long long index) {
    raftState *rs = server.raft;
    if (!rs || !rs->next_index) return;
    rs->next_index[follower_idx] = index;
}

long long raftStateGetMatchIndex(int follower_idx) {
    raftState *rs = server.raft;
    if (!rs || !rs->match_index) return 0;
    return rs->match_index[follower_idx];
}

void raftStateSetMatchIndex(int follower_idx, long long index) {
    raftState *rs = server.raft;
    if (!rs || !rs->match_index) return;
    rs->match_index[follower_idx] = index;
}

/* ================================ Timeout Management ============================== */

void raftStateResetElectionTimeout(void) {
    raftState *rs = server.raft;
    if (!rs) return;

    /* Add randomization to prevent split votes */
    int jitter = rand() % (rs->election_timeout_ms / 2);
    rs->election_timeout = mstime() + rs->election_timeout_ms + jitter;
}

void raftStateResetHeartbeatTimeout(void) {
    raftState *rs = server.raft;
    if (!rs) return;

    rs->heartbeat_timeout = mstime() + rs->heartbeat_interval_ms;
}

int raftStateIsElectionTimeout(void) {
    raftState *rs = server.raft;
    if (!rs) return 0;
    return mstime() >= rs->election_timeout;
}

int raftStateIsHeartbeatTimeout(void) {
    raftState *rs = server.raft;
    if (!rs) return 0;
    return mstime() >= rs->heartbeat_timeout;
}

/* ================================ Quorum Calculation ============================== */

int raftStateCalculateQuorum(int num_nodes) {
    return (num_nodes / 2) + 1;
}

int raftStateHasQuorum(long long index, int num_nodes) {
    raftState *rs = server.raft;
    if (!rs || !rs->match_index) return 0;

    int count = 1; /* Count self */

    /* Count how many followers have replicated this index */
    for (int i = 0; i < num_nodes - 1; i++) {
        if (rs->match_index[i] >= index) {
            count++;
        }
    }

    int quorum = raftStateCalculateQuorum(num_nodes);
    return count >= quorum;
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
    raftState *rs = server.raft;
    if (!rs) return 0;

    /* Determine number of replicas from server.replicas list */
    int num_replicas = listLength(server.replicas);
    int total_nodes = num_replicas + 1;

    /* Handle edge case: no replicas, return primary's index */
    if (num_replicas == 0) {
        return rs->last_log_index;
    }

    /* Allocate array for all indices (replicas + primary) */
    long long *indices = zmalloc(sizeof(long long) * total_nodes);

    /* Initialize array to avoid invalid sort operation */
    memset(indices, 0, sizeof(long long) * total_nodes);

    /* Collect match indices from replicas */
    if (rs->match_index) {
        for (int i = 0; i < num_replicas; i++) {
            indices[i] = rs->match_index[i];
        }
    }

    /* Add primary's index */
    indices[num_replicas] = rs->last_log_index;

    /* Sort in descending order */
    qsort(indices, total_nodes, sizeof(long long), compareIndicesDesc);

    /* Get quorum index (Nth highest where N = quorum_size) */
    int quorum_size = raftStateCalculateQuorum(total_nodes);
    long long quorum_index = indices[quorum_size - 1];

    /* Validate: quorum index should not exceed primary's index */
    if (quorum_index > rs->last_log_index) {
        serverLog(LL_WARNING, "Raft: Quorum index %lld exceeds last_log_index %lld",
                  quorum_index, rs->last_log_index);
        quorum_index = rs->last_log_index;
    }

    zfree(indices);

    return quorum_index;
}

/* ================================ State Validation ============================== */

int raftStateValidate(void) {
    raftState *rs = server.raft;
    if (!rs) return C_ERR;

    /* Validate term is non-negative */
    if (rs->current_term < 0) {
        serverLog(LL_WARNING, "Raft: Invalid current_term: %lld", rs->current_term);
        return C_ERR;
    }

    /* Validate commit_index <= last_log_index */
    if (rs->commit_index > rs->last_log_index) {
        serverLog(LL_WARNING, "Raft: commit_index (%lld) > last_log_index (%lld)",
                  rs->commit_index, rs->last_log_index);
        return C_ERR;
    }

    /* Validate last_applied <= commit_index */
    if (rs->last_applied > rs->commit_index) {
        serverLog(LL_WARNING, "Raft: last_applied (%lld) > commit_index (%lld)",
                  rs->last_applied, rs->commit_index);
        return C_ERR;
    }

    return C_OK;
}

/* ================================ Debug and Logging ============================== */

void raftStatePrint(void) {
    raftState *rs = server.raft;
    if (!rs) {
        serverLog(LL_NOTICE, "Raft state: NULL");
        return;
    }

    serverLog(LL_NOTICE, "Raft State:");
    serverLog(LL_NOTICE, "  Role: %s", raftStateRoleString(rs->role));
    serverLog(LL_NOTICE, "  Term: %lld", rs->current_term);
    serverLog(LL_NOTICE, "  Voted for: %lld", rs->voted_for);
    serverLog(LL_NOTICE, "  Commit index: %lld", rs->commit_index);
    serverLog(LL_NOTICE, "  Last applied: %lld", rs->last_applied);
    serverLog(LL_NOTICE, "  Last log index: %lld", rs->last_log_index);
    serverLog(LL_NOTICE, "  Last log term: %lld", rs->last_log_term);
}

sds raftStateToString(void) {
    raftState *rs = server.raft;
    if (!rs) return sdsnew("NULL");

    return sdscatprintf(sdsempty(),
                        "role=%s term=%lld voted_for=%lld commit=%lld applied=%lld "
                        "last_log_idx=%lld last_log_term=%lld",
                        raftStateRoleString(rs->role),
                        rs->current_term,
                        rs->voted_for,
                        rs->commit_index,
                        rs->last_applied,
                        rs->last_log_index,
                        rs->last_log_term);
}
