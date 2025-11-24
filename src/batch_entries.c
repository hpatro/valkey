/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "server.h"

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
batchEntries *batchEntriesCreate(client *c) {
    batchEntries *bc = zcalloc(sizeof(batchEntries));
    bc->operations = listCreate();
    listSetFreeMethod(bc->operations, freeBatchOperation);
    bc->client = c;
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
    bc->payload_size = 0;
    bc->batch_start_time = 0;
    bc->raft_term = 0;
    bc->raft_index = 0;
    bc->prev_log_index = 0;
    bc->prev_log_term = 0;
    bc->commit_index = 0;
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

    /* Copy arguments and calculate payload size */
    size_t op_payload_size = 0;
    for (int i = 0; i < argc; i++) {
        op->argv[i] = argv[i];
        incrRefCount(argv[i]);
        if (argv_len) op->argv_len[i] = argv_len[i];
        op_payload_size += sdslen(argv[i]->ptr);
    }

    listAddNodeTail(bc->operations, op);

    /* Update batch statistics */
    if (bc->count++ == 0 && !bc->client) {
        /* Primary-side: start timing */
        bc->batch_start_time = mstime();
    }
    bc->payload_size += op_payload_size;

    return C_OK;
}

/* Check if batch should be flushed (primary-side) */
int batchEntriesShouldFlush(batchEntries *bc) {
    if (!bc || bc->count == 0) return 0;
    
    /* Check batch size limit */
    if (server.batch_max_operations > 0 && bc->count >= (size_t)server.batch_max_operations) {
        return 1;
    }
    
    /* Check payload size limit */
    if (server.batch_max_payload_size > 0 && bc->payload_size >= (size_t)server.batch_max_payload_size) {
        return 1;
    }
    
    /* Check timeout limit */
    if (server.batch_timeout_ms > 0 && bc->batch_start_time > 0) {
        mstime_t elapsed = mstime() - bc->batch_start_time;
        if (elapsed >= server.batch_timeout_ms) {
            return 1;
        }
    }
    
    return 0;
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

/* ================================ RAFT ENTRY CONVERSION ============================== */

/* Format a batch entries into a Raft entry */
raftEntry *batchEntriesToRaftEntry(batchEntries *bc) {
    if (!bc || bc->count == 0) return NULL;
    
    raftEntry *entry = zmalloc(sizeof(raftEntry));
    if (!entry) return NULL;
    
    entry->index = bc->raft_index;
    entry->term = bc->raft_term;
    entry->operation_count = bc->count;
    entry->payload = sdsempty();
    
    /* Track current database to minimize SELECT commands */
    int current_dictid = -1;
    
    /* Serialize each operation in the batch */
    listIter li;
    listNode *ln;
    listRewind(bc->operations, &li);
    
    while ((ln = listNext(&li))) {
        batchEntry *op = listNodeValue(ln);
        
        /* Add database selection only when database changes */
        if (op->dictid != -1 && op->dictid != current_dictid) {
            sds dictid_str = sdsfromlonglong(op->dictid);
            entry->payload = sdscatprintf(entry->payload, "*2\r\n$6\r\nSELECT\r\n$%zu\r\n%s\r\n", 
                                        sdslen(dictid_str), dictid_str);
            sdsfree(dictid_str);
            current_dictid = op->dictid;
        }
        
        /* Add the command in RESP format */
        entry->payload = sdscatprintf(entry->payload, "*%d\r\n", op->argc);
        for (int i = 0; i < op->argc; i++) {
            sds arg = op->argv[i]->ptr;
            entry->payload = sdscatprintf(entry->payload, "$%zu\r\n%s\r\n", sdslen(arg), arg);
        }
    }
    return entry;
}

/* Serialize a Raft entry into the AE START/END format using RESP */
sds serializeRaftEntry(raftEntry *entry) {
    if (!entry) return NULL;

    /* Format: *6\r\n$8\r\nAE_START\r\n$<len>\r\n<term>\r\n$<len>\r\n<prev_log_index>\r\n$<len>\r\n<prev_log_term>\r\n$<len>\r\n<commit_index>\r\n$<len>\r\n<count>\r\n<payload>*1\r\n$6\r\nAE_END\r\n */
    sds term_str = sdsfromlonglong(entry->term);
    sds prev_index_str = sdsfromlonglong(entry->index - 1);  /* prev_log_index is index - 1 */
    sds prev_term_str = sdsfromlonglong(entry->term);  /* Simplified: use same term */
    sds commit_str = sdsfromlonglong(entry->index);  /* commit_index is current index */
    sds count_str = sdsfromlonglong(entry->operation_count);

    sds serialized = sdscatprintf(sdsempty(),
                                  "*6\r\n$8\r\nAE_START\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n",
                                  sdslen(term_str), term_str,
                                  sdslen(prev_index_str), prev_index_str,
                                  sdslen(prev_term_str), prev_term_str,
                                  sdslen(commit_str), commit_str,
                                  sdslen(count_str), count_str);

    sdsfree(term_str);
    sdsfree(prev_index_str);
    sdsfree(prev_term_str);
    sdsfree(commit_str);
    sdsfree(count_str);

    serialized = sdscatsds(serialized, entry->payload);
    serialized = sdscat(serialized, "*1\r\n$6\r\nAE_END\r\n");

    return serialized;
}

/* Free a Raft entry and its resources */
void freeRaftEntry(raftEntry *entry) {
    if (!entry) return;
    if (entry->payload) {
        sdsfree(entry->payload);
    }
    zfree(entry);
}

/* Calculate CRC32 checksum for Raft entry payload */
uint32_t calculateRaftEntryChecksum(const char *data, size_t len) {
    /* Use existing CRC64 function and truncate to 32 bits for simplicity */
    uint64_t crc = crc64(0, (unsigned char *)data, len);
    return (uint32_t)(crc & 0xFFFFFFFF);
}

/* ================================ PRIMARY-SIDE OPERATIONS ============================== */

/* Flush the batch entries (primary-side: serialize and send to replicas) */
int batchEntriesFlush(batchEntries *bc) {
    if (!bc || bc->count == 0) return C_ERR;
    
    /* Increment Raft index for this batch */
    server.raft_current_index++;
    bc->raft_index = server.raft_current_index;
    bc->raft_term = server.raft_current_term;
    
    /* Format the batch as a Raft entry */
    raftEntry *entry = batchEntriesToRaftEntry(bc);
    if (!entry) {
        serverLog(LL_WARNING, "Failed to format Raft entry for batch");
        batchEntriesReset(bc);
        return C_ERR;
    }
    
    /* Serialize the Raft entry */
    sds serialized = serializeRaftEntry(entry);
    if (!serialized) {
        serverLog(LL_WARNING, "Failed to serialize Raft entry for batch");
        freeRaftEntry(entry);
        batchEntriesReset(bc);
        return C_ERR;
    }
    
    /* Must install write handler for all replicas first before feeding replication stream */
    prepareReplicasToWrite();
    
    /* Send the serialized Raft entry to the replication buffer */
    feedReplicationBuffer(serialized, sdslen(serialized));
    
    /* Log the batch flush for debugging */
    serverLog(LL_DEBUG, "Flushed batch with %zu operations, Raft index %lld, term %lld",
              bc->count, entry->index, entry->term);
    
    /* Clean up */
    sdsfree(serialized);
    freeRaftEntry(entry);
    
    /* Reset the batch entries for the next batch */
    batchEntriesReset(bc);
    
    return C_OK;
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
            sendBatchAck(c, bc->prev_log_index + 1, bc->raft_term, 0, "Operation count mismatch");
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
    if (c->flag.replica && execution_success) {
        sendBatchAck(c, bc->prev_log_index + 1, bc->raft_term, 1, NULL);
    }
    
    return execution_success ? C_OK : C_ERR;
}

/* ================================ AE COMMAND HANDLERS ============================== */

void aeStartCommand(client *c) {
    long long raft_term, prev_log_index, prev_log_term, commit_index, op_count;

    /* Parse arguments: AE_START <raft_term> <prev_log_index> <prev_log_term> <commit_index> <op_count> */
    if (c->argc != 6) {
        addReplyErrorArity(c);
        return;
    }

    if (getLongLongFromObjectOrReply(c, c->argv[1], &raft_term, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[2], &prev_log_index, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &prev_log_term, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[4], &commit_index, NULL) != C_OK) return;
    if (getLongLongFromObjectOrReply(c, c->argv[5], &op_count, NULL) != C_OK) return;

    if (!c->batch_entries) c->batch_entries = batchEntriesCreate(c);
    c->flag.ae = 1;
    c->batch_entries->raft_term = raft_term;
    c->batch_entries->prev_log_index = prev_log_index;
    c->batch_entries->prev_log_term = prev_log_term;
    c->batch_entries->commit_index = commit_index;
    c->batch_entries->expected_count = (int)op_count;

    addReply(c, shared.ok);
}

void aeEndCommand(client *c) {
    if (!c->flag.ae || !c->batch_entries) {
        addReplyError(c, "AE_END without AE_START");
        return;
    }

    int result = batchEntriesExecute(c->batch_entries, c);
    
    batchEntriesFree(c->batch_entries);
    c->batch_entries = NULL;
    c->flag.ae = 0;

    if (result == C_OK) {
        addReply(c, shared.ok);
    } else {
        addReplyError(c, "Batch execution failed");
    }
}

void queueAeCommand(client *c) {
    if (!c->batch_entries) c->batch_entries = batchEntriesCreate(c);
    
    batchEntriesAddEntry(c->batch_entries, -1, c->argv, c->argc, NULL, c->cmd, c->slot, c);
    
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
