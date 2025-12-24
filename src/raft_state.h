/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef __RAFT_STATE_H
#define __RAFT_STATE_H

#include "server.h"

/* Raft node role */
typedef enum {
    RAFT_ROLE_FOLLOWER,
    RAFT_ROLE_CANDIDATE,
    RAFT_ROLE_LEADER
} raftRole;

/* Raft state structure - encapsulates all Raft consensus state */
typedef struct raftState {
    list *operation_log;
    /* Persistent state (should be persisted to stable storage) */
    long long current_term;        /* Latest term server has seen */
    long long voted_for;           /* CandidateId that received vote in current term */
    
    /* Volatile state on all servers */
    long long commit_index;        /* Index of highest log entry known to be committed */
    long long last_applied;        /* Index of highest log entry applied to state machine */
    
    /* Volatile state on leaders (reinitialized after election) */
    long long *next_index;         /* For each server, index of next log entry to send */
    long long *match_index;        /* For each server, index of highest log entry known to be replicated */
    
    /* Log state */
    long long last_log_index;      /* Index of last entry in log */
    long long last_log_term;       /* Term of last entry in log */
    
    /* Node state */
    raftRole role;                 /* Current role of this node */
    mstime_t election_timeout;     /* When to start election (follower/candidate) */
    mstime_t heartbeat_timeout;    /* When to send next heartbeat (leader) */
    
    /* Configuration */
    int election_timeout_ms;       /* Election timeout in milliseconds */
    int heartbeat_interval_ms;     /* Heartbeat interval in milliseconds */
} raftState;

/* ================================ API Functions ============================== */

/* Initialization and cleanup */
void raftStateInit(void);
void raftStateCleanup(void);
void raftStateReset(void);

/* Term management */
long long raftStateGetCurrentTerm(void);
void raftStateSetCurrentTerm(long long term);
int raftStateIncrementTerm(void);

/* Commit index management */
long long raftStateGetCommitIndex(void);
void raftStateSetCommitIndex(long long index);
int raftStateCanCommit(long long index);

/* Log storage */
void raftStateAddLog(raftEntry *entry);

/* Log index management */
long long raftStateGetLastLogIndex(void);
long long raftStateGetLastLogTerm(void);
long long raftStateGetLastApplied(void);
void raftStateUpdateLastLog(long long index, long long term);
long long raftStateIncrementLogIndex(void);
long long raftStateIncrementLastApplied(void);

/* Role management */
raftRole raftStateGetRole(void);
void raftStateSetRole(raftRole role);
const char *raftStateRoleString(raftRole role);

/* Voting */
long long raftStateGetVotedFor(void);
void raftStateSetVotedFor(long long candidate_id);
int raftStateCanVoteFor(long long candidate_id, long long term);

/* Leader state management */
void raftStateInitLeaderState(int num_followers);
void raftStateFreeLeaderState(void);
long long raftStateGetNextIndex(int follower_idx);
void raftStateSetNextIndex(int follower_idx, long long index);
long long raftStateGetMatchIndex(int follower_idx);
void raftStateSetMatchIndex(int follower_idx, long long index);

/* Timeout management */
void raftStateResetElectionTimeout(void);
void raftStateResetHeartbeatTimeout(void);
int raftStateIsElectionTimeout(void);
int raftStateIsHeartbeatTimeout(void);

/* Quorum calculation */
int raftStateCalculateQuorum(int num_nodes);
int raftStateHasQuorum(long long index, int num_nodes);

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
long long raftStateGetQuorumIndex(void);

/* State validation */
int raftStateValidate(void);

/* Debug and logging */
void raftStatePrint(void);
sds raftStateToString(void);

/* Entry management */
raftEntry *createEntry(int dictid, robj **argv, int argc, struct serverCommand *cmd);
#endif /* __RAFT_STATE_H */
