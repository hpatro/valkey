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

/* Log index management */
long long raftStateGetLastLogIndex(void);
long long raftStateGetLastLogTerm(void);
void raftStateUpdateLastLog(long long index, long long term);
long long raftStateIncrementLogIndex(void);

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

/* State validation */
int raftStateValidate(void);

/* Debug and logging */
void raftStatePrint(void);
sds raftStateToString(void);

#endif /* __RAFT_STATE_H */
