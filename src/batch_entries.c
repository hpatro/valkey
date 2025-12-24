/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "server.h"
#include "raft_state.h"

void batchEntriesProcessDeferred(client *c);

/* Free a batch operation structure */
static void freeBatchOperation(void *ptr) {
    batchEntry *op = ptr;
    if (op->argv) {
        for (int i = 0; i < op->argc; i++) {
            decrRefCount(op->argv[i]);
        }
        zfree(op->argv);
    }
    if (op->argv_len) zfree(op->argv_len);
    zfree(op);
}

/* Create a new batch entries */
batchEntries *batchEntriesCreate(void) {
    batchEntries *bc = zcalloc(sizeof(batchEntries));
    bc->operations = listCreate();
    listSetFreeMethod(bc->operations, freeBatchOperation);
    return bc;
}

/* Free a batch entries and all its resources */
void batchEntriesFree(batchEntries *bc) {
    if (!bc) return;
    if (bc->operations) {
        listRelease(bc->operations);
    }
    zfree(bc);
}

/* Reset a batch entries for reuse */
void batchEntriesReset(batchEntries *bc) {
    if (!bc) return;
    
    listEmpty(bc->operations);
    bc->count = 0;
}

/* Add an entry to the batch entries */
int batchEntriesAddEntry(batchEntries *be, int dictid, robj **argv, int argc, struct serverCommand *cmd) {
    if (!be) return C_ERR;

    batchEntry *op = zmalloc(sizeof(batchEntry));
    op->dictid = dictid;
    op->argc = argc;
    op->cmd = cmd;
    op->argv = zmalloc(sizeof(robj *) * argc);
    op->log_index = raftStateIncrementLogIndex();
    op->term = raftStateGetCurrentTerm();

    /* Copy arguments */
    for (int i = 0; i < argc; i++) {
        op->argv[i] = argv[i];
        incrRefCount(argv[i]);
    }

    listAddNodeTail(be->operations, op);

    /* Update batch count */
    be->count++;

    return C_OK;
}

/* Calculate memory overhead of batch entries */
size_t batchEntriesMemOverhead(batchEntries *bc) {
    if (!bc) return 0;
    
    size_t mem = sizeof(batchEntries);
    
    listIter li;
    listNode *ln;
    listRewind(bc->operations, &li);
    while ((ln = listNext(&li))) {
        batchEntry *op = listNodeValue(ln);
        mem += sizeof(batchEntry);
        mem += sizeof(robj *) * op->argc;
        if (op->argv_len) mem += sizeof(size_t) * op->argc;
        for (int i = 0; i < op->argc; i++) {
            mem += sdsAllocSize(op->argv[i]->ptr);
        }
    }
    
    return mem;
}

/* ================================ REPLICA-SIDE OPERATIONS ============================== */

/* Send BATCH-ACK to primary with both log index and commit index */
static void sendBatchAck(long long log_index, long long commit_index, long long raft_term, int success, char *error_msg) {
    UNUSED(success);
    UNUSED(error_msg);
    client *primary = server.primary;
    /* Force reply to be sent even though this is a primary connection */
    primary->flag.primary_force_reply = 1;
    
    addReplyArrayLen(primary, 5);
    addReplyBulkCString(primary, "REPLCONF");
    addReplyBulkCString(primary, "BATCH-ACK");
    addReplyBulkLongLong(primary, log_index);
    addReplyBulkLongLong(primary, commit_index);
    addReplyBulkLongLong(primary, raft_term);
    
    primary->flag.primary_force_reply = 0;
    
    /* Reset accumulation as done in replicationSendAck() */
    primary->net_output_bytes_curr_cmd = 0;
    
    serverLog(LL_DEBUG, "Sent BATCH-ACK: log_index=%lld, commit_index=%lld, term=%lld, status=%s",
              log_index, commit_index, raft_term, success ? "OK" : "ERROR");
}

    
void queueAeCommand(client *c) {
    /* Always queue commands on replica side */
    if (!c->batch_entries) c->batch_entries = batchEntriesCreate();
    
    /* Increment log index on successful queuing */
    long long new_log_index = raftStateIncrementLogIndex();
    serverLog(LL_DEBUG, "Queued command %s, incremented log index to %lld", 
              c->cmd->fullname, new_log_index);
    
    batchEntriesAddEntry(c->batch_entries, c->db->id, c->argv, c->argc, c->cmd);
    
    /* Send BATCH-ACK to primary on successful queuing */
    if (c->flag.primary) {
        sendBatchAck(raftStateGetLastLogIndex(), raftStateGetCommitIndex(), raftStateGetLastLogTerm(), 1, NULL);
    }    
}

/* ================================ DEFERRED EXECUTION ============================== */

/* Process deferred batches that can now be committed */
void batchEntriesProcessDeferred(client *c) {
    if (!c->batch_entries) {
        return;
    }

    if (c->batch_entries->count == 0) {
        return;
    }
    long long commit_index = raftStateGetCommitIndex();
    serverLog(LL_DEBUG, "Processing deferred batches: queue_size=%lu, commit_index=%lld",
              c->batch_entries->count, commit_index);

    listIter li;
    listNode *ln;
    listRewind(c->batch_entries->operations, &li);

    /* Save original client state */
    struct ClientFlags old_flags = c->flag;
    robj **orig_argv = c->argv;
    int orig_argc = c->argc;
    int orig_argv_len = c->argv_len;
    struct serverCommand *orig_cmd = c->cmd;
    int old_reply_off = c->flag.reply_off;

    while ((ln = listNext(&li))) {
        batchEntry *op = listNodeValue(ln);
        if (op->log_index > raftStateGetCommitIndex()) {
            break;
        }
        
        if (!op->cmd) {
            serverPanic("Batch operation has NULL command");
        }
        
        serverLog(LL_DEBUG, "Executing batch operation %s", op->cmd->fullname);
    
        /* Configure execution environment: no blocking, no replies */
        c->flag.deny_blocking = 1;
        c->flag.reply_off = 1;

        c->argc = op->argc;
        c->argv = op->argv;
        c->argv_len = op->argc;
        c->cmd = c->realcmd = op->cmd;
        
        /* Check ACL permissions */
        int acl_errpos;
        int acl_retval = ACLCheckAllPerm(c, &acl_errpos);
        if (acl_retval != ACL_OK) {
            serverLog(LL_WARNING, "Batch operation: ACL check failed: %s", op->cmd->fullname);
            addACLLogEntry(c, acl_retval, ACL_LOG_CTX_MULTI, acl_errpos, NULL, NULL);
        } else {
            call(c, CMD_CALL_NONE);
            /* Increment commit index on successful execution */
            raftStateIncrementLastApplied();
        }

        freeClientOriginalArgv(c);
        listDelNode(c->batch_entries->operations, ln);
        c->batch_entries->count--;
    }    
    /* Restore client state */
    c->flag.reply_off = old_reply_off;
    if (!old_flags.deny_blocking) c->flag.deny_blocking = 0;
    c->argv = orig_argv;
    c->argv_len = orig_argv_len;
    c->argc = orig_argc;
    c->cmd = c->realcmd = orig_cmd;
}
