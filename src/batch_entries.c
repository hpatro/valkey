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
    bc->raft_term = 0;
    bc->raft_index = 0;
    bc->prev_log_index = 0;
    bc->prev_log_term = 0;
    bc->expected_count = 0;
}

/* Add an entry to the batch entries */
int batchEntriesAddEntry(batchEntries *bc, int dictid, robj **argv, int argc, 
                         size_t *argv_len, struct serverCommand *cmd, int slot, client *c) {
    if (!bc) return C_ERR;

    batchEntry *op = zmalloc(sizeof(batchEntry));
    op->dictid = dictid;
    op->argc = argc;
    op->cmd = cmd;
    op->slot = slot;
    op->client = c;
    op->argv = zmalloc(sizeof(robj *) * argc);
    op->argv_len = argv_len ? zmalloc(sizeof(size_t) * argc) : NULL;

    /* Copy arguments */
    for (int i = 0; i < argc; i++) {
        op->argv[i] = argv[i];
        incrRefCount(argv[i]);
        if (argv_len) op->argv_len[i] = argv_len[i];
    }

    listAddNodeTail(bc->operations, op);

    /* Update batch count */
    bc->count++;

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

/* Send BATCH-ACK to primary */
static void sendBatchAck(client *c, long long raft_index, long long raft_term, int success, char *error_msg) {
    if (!c || !c->flag.replica) return;

    sds ack_cmd = success 
        ? sdscatprintf(sdsempty(), "*5\r\n$8\r\nREPLCONF\r\n$9\r\nBATCH-ACK\r\n$%d\r\n%lld\r\n$%d\r\n%lld\r\n$2\r\nOK\r\n",
                       (int)sdigits10(raft_index), raft_index,
                       (int)sdigits10(raft_term), raft_term)
        : sdscatprintf(sdsempty(), "*6\r\n$8\r\nREPLCONF\r\n$9\r\nBATCH-ACK\r\n$%d\r\n%lld\r\n$%d\r\n%lld\r\n$5\r\nERROR\r\n$%zu\r\n%s\r\n",
                       (int)sdigits10(raft_index), raft_index,
                       (int)sdigits10(raft_term), raft_term,
                       strlen(error_msg), error_msg);

    if (connWrite(c->conn, ack_cmd, sdslen(ack_cmd)) == -1) {
        serverLog(LL_WARNING, "Failed to send BATCH-ACK to primary");
    }
    sdsfree(ack_cmd);
}

/* Execute all operations in the batch entries (replica-side) */
// TODO Remove client, we should be able to execute without it.
int batchEntriesExecute(batchEntries *bc, client *c) {
    if (!bc || !c) {
        serverLog(LL_WARNING, "batchEntriesExecute: NULL batch entries or client");
        return C_ERR;
    }
    
    /* Validate operation count */
    if (bc->expected_count > 0 && bc->count != (size_t)bc->expected_count) {
        serverLog(LL_WARNING, "Batch operation count mismatch: expected %d, got %zu",
                  bc->expected_count, bc->count);
        if (c->flag.replica) {
            sendBatchAck(c, raftStateGetLastLogIndex(), raftStateGetLastLogTerm(), 0, "Operation count mismatch");
        }
        return C_ERR;
    }
    
    serverLog(LL_DEBUG, "Executing batch with %zu operations", bc->count);
    
    /* Save original client state */
    struct ClientFlags old_flags = c->flag;
    robj **orig_argv = c->argv;
    int orig_argc = c->argc;
    int orig_argv_len = c->argv_len;
    struct serverCommand *orig_cmd = c->cmd;
    
    /* Configure execution environment: no blocking, no replies */
    c->flag.deny_blocking = 1;
    int old_reply_off = c->flag.reply_off;
    c->flag.reply_off = 1;
    
    /* Execute all operations */
    int execution_success = 1;
    listIter li;
    listNode *ln;
    listRewind(bc->operations, &li);
    
    int op_num = 0;
    while ((ln = listNext(&li))) {
        batchEntry *op = listNodeValue(ln);
        op_num++;
        
        if (!op->cmd) {
            serverLog(LL_WARNING, "Batch operation %d has NULL command", op_num);
            execution_success = 0;
            continue;
        }
        
        serverLog(LL_DEBUG, "Executing batch operation %d: %s (argc=%d)", 
                  op_num, op->cmd->fullname, op->argc);
        
        c->argc = op->argc;
        c->argv = op->argv;
        c->argv_len = op->argc;  /* Set to argc since we don't track individual lengths */
        c->cmd = c->realcmd = op->cmd;
        
        /* Check ACL permissions */
        int acl_errpos;
        int acl_retval = ACLCheckAllPerm(c, &acl_errpos);
        if (acl_retval != ACL_OK) {
            serverLog(LL_WARNING, "Batch operation %d ACL check failed: %s", op_num, op->cmd->fullname);
            addACLLogEntry(c, acl_retval, ACL_LOG_CTX_MULTI, acl_errpos, NULL, NULL);
            execution_success = 0;
        } else {
            call(c, CMD_CALL_NONE);
            serverAssert(c->flag.blocked == 0);
        }
        
        freeClientOriginalArgv(c);
    }
    
    serverLog(LL_DEBUG, "Batch execution completed: %s", execution_success ? "SUCCESS" : "FAILED");
    
    /* Restore client state */
    c->flag.reply_off = old_reply_off;
    if (!old_flags.deny_blocking) c->flag.deny_blocking = 0;
    c->argv = orig_argv;
    c->argv_len = orig_argv_len;
    c->argc = orig_argc;
    c->cmd = c->realcmd = orig_cmd;
    
    /* Send ACK if this is a replica */
    if (c->flag.replica) {
        if (execution_success) {
            sendBatchAck(c, raftStateGetLastLogIndex(), raftStateGetLastLogTerm(), 1, NULL);
        } else {
            sendBatchAck(c, raftStateGetLastLogIndex(), raftStateGetLastLogTerm(), 0, "Execution failed");
        }
    }
    
    return execution_success ? C_OK : C_ERR;
}

void queueAeCommand(client *c) {
    /* Always queue commands on replica side, regardless of AE_START marker */
    if (!c->batch_entries) c->batch_entries = batchEntriesCreate();
    
    batchEntriesAddEntry(c->batch_entries, c->db->id, c->argv, c->argc, NULL, c->cmd, c->slot, c);
    
    /* Reset the client's args since we copied them into the batch entries */
    c->argv = NULL;
    c->argc = 0;
    c->argv_len_sum = 0;
    c->argv_len = 0;
}

void discardAeTransaction(client *c) {
    if (c->batch_entries) {
        batchEntriesFree(c->batch_entries);
        c->batch_entries = NULL;
    }
    c->flag.ae = 0;
}

/* ================================ DEFERRED EXECUTION ============================== */

/* Process deferred batches that can now be committed */
void batchEntriesProcessDeferred(client *c) {
    if (!server.deferred_batches || listLength(server.deferred_batches) == 0) {
        return;
    }

    long long commit_index = raftStateGetCommitIndex();
    serverLog(LL_DEBUG, "Processing deferred batches: queue_size=%lu, commit_index=%lld",
              listLength(server.deferred_batches), commit_index);

    listIter li;
    listNode *ln, *next;
    listRewind(server.deferred_batches, &li);

    while ((ln = listNext(&li))) {
        batchEntries *bc = listNodeValue(ln);

        /* Check if this batch can be committed */
        if (raftStateCanCommit(bc->raft_index)) {
            serverLog(LL_DEBUG, "Executing deferred batch: index=%lld, term=%lld, operations=%zu",
                      bc->raft_index, bc->raft_term, bc->count);

            /* Execute the batch */
            int result = batchEntriesExecute(bc, c);

            if (result == C_OK) {
                serverLog(LL_DEBUG, "Successfully executed batch index=%lld",
                          bc->raft_index);
            } else {
                serverLog(LL_WARNING, "Failed to execute deferred batch index=%lld", bc->raft_index);
            }

            /* Remove from queue (will be freed by list free method) */
            next = ln->next;
            listDelNode(server.deferred_batches, ln);
            ln = next ? next->prev : NULL; /* Adjust iterator */
        } else {
            serverLog(LL_DEBUG, "Batch index=%lld not yet committed (commit_index=%lld), keeping in queue",
                      bc->raft_index, commit_index);
        }
    }
}
