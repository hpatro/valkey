#include "generated_wrappers.hpp"

#include <cstring>

extern "C" {
#include <arpa/inet.h>

#include "server.h"
#include "cluster.h"
#include "cluster_legacy.h"

typedef struct clusterMsgSendBlock {
    size_t totlen;
    int refcount;
    union {
        clusterMsg msg;
        clusterMsgLight msg_light;
    } data[1];
} clusterMsgSendBlock;

typedef struct clusterIOPacket {
    char *buf;
    size_t len;
} clusterIOPacket;

clusterLink *createClusterLink(clusterNode *node);
int freeClusterLink(clusterLink *link);
clusterMsgSendBlock *createClusterMsgSendBlock(int type, uint32_t msglen);
clusterIOResult testOnlyClusterFrameInboundPackets(clusterLink *link,
                                                   int packet_limit,
                                                   size_t byte_limit,
                                                   int *framed_packets,
                                                   size_t *framed_bytes);
void testOnlyClusterEnqueueMessage(clusterLink *link, clusterMsgSendBlock *msgblock);
void testOnlyClusterRotateSendMessageQueues(clusterLink *link);
int testOnlyClusterAdvanceInflightSendQueue(clusterLink *link, size_t advance);
clusterIOResult testOnlyClusterWriteInflightMessages(clusterLink *link, size_t max_bytes, size_t *total_written, int *blocked);
void testOnlyClusterMsgSendBlockRelease(clusterMsgSendBlock *msgblock);
}

namespace {
constexpr uint32_t kLightMsgLen = 32;
constexpr int kLightPublishType = CLUSTERMSG_TYPE_PUBLISH | CLUSTERMSG_LIGHT;
constexpr int kLightPublishShardType = CLUSTERMSG_TYPE_PUBLISHSHARD | CLUSTERMSG_LIGHT;
constexpr int kLightModuleType = CLUSTERMSG_TYPE_MODULE | CLUSTERMSG_LIGHT;
}

typedef struct fakeClusterConnection {
    connection conn;
    char *buffer;
    size_t capacity;
    size_t written;
    size_t max_write;
    int postpone_calls;
    int last_postpone;
    int update_calls;
} fakeClusterConnection;

static ConnectionType CT_FakeCluster;

static int fakeClusterWrite(connection *conn, const void *data, size_t size) {
    fakeClusterConnection *fake = (fakeClusterConnection *)conn;
    size_t to_write = size < fake->max_write ? size : fake->max_write;
    if (fake->written + to_write > fake->capacity) {
        to_write = fake->capacity - fake->written;
    }
    if (to_write == 0) return -1;
    memcpy(fake->buffer + fake->written, data, to_write);
    fake->written += to_write;
    return (int)to_write;
}

static void fakeClusterPostponeUpdateState(connection *conn, int on) {
    fakeClusterConnection *fake = (fakeClusterConnection *)conn;
    fake->postpone_calls++;
    fake->last_postpone = on;
}

static void fakeClusterUpdateState(connection *conn) {
    fakeClusterConnection *fake = (fakeClusterConnection *)conn;
    fake->update_calls++;
}

static fakeClusterConnection *createFakeClusterConnection(size_t capacity) {
    fakeClusterConnection *fake = (fakeClusterConnection *)zcalloc(sizeof(*fake));
    fake->conn.type = &CT_FakeCluster;
    fake->conn.fd = -1;
    fake->conn.iovcnt = IOV_MAX;
    fake->conn.state = CONN_STATE_CONNECTED;
    fake->buffer = (char *)zmalloc(capacity);
    fake->capacity = capacity;
    fake->max_write = (size_t)-1;
    return fake;
}

static void freeFakeClusterConnection(fakeClusterConnection *fake) {
    zfree(fake->buffer);
    zfree(fake);
}

static void releaseSendBlock(clusterMsgSendBlock *msgblock) {
    testOnlyClusterMsgSendBlockRelease(msgblock);
}

class ClusterLegacyIOTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        memset(&CT_FakeCluster, 0, sizeof(CT_FakeCluster));
        CT_FakeCluster.write = fakeClusterWrite;
        CT_FakeCluster.postpone_update_state = fakeClusterPostponeUpdateState;
        CT_FakeCluster.update_state = fakeClusterUpdateState;
    }

    void SetUp() override {
        server.cluster = NULL;
        server.stat_cluster_links_memory = 0;
        server.stat_cluster_io_inbound_packets_queued = 0;
        server.stat_cluster_io_async_closed_links = 0;
    }
};

TEST_F(ClusterLegacyIOTest, FramesPacketAfterPartialHeaderAndPayloadArrival) {
    clusterLink *link = createClusterLink(NULL);
    clusterMsgSendBlock *msgblock = createClusterMsgSendBlock(kLightPublishType, kLightMsgLen);
    clusterMsgLight *msg = &msgblock->data[0].msg_light;
    size_t packet_len = ntohl(msg->totlen);

    memcpy(link->rcvbuf, msg, 16);
    link->rcvbuf_len = 16;

    int framed_packets = 0;
    size_t framed_bytes = 0;
    EXPECT_EQ(testOnlyClusterFrameInboundPackets(link, 0, 0, &framed_packets, &framed_bytes), CLUSTER_IO_OK);
    EXPECT_EQ(framed_packets, 0);
    EXPECT_EQ(framed_bytes, 0u);
    EXPECT_EQ(link->recv_packet_queue, nullptr);

    memcpy(link->rcvbuf + 16, ((char *)msg) + 16, packet_len - 16);
    link->rcvbuf_len = packet_len;

    EXPECT_EQ(testOnlyClusterFrameInboundPackets(link, 0, 0, &framed_packets, &framed_bytes), CLUSTER_IO_OK);
    ASSERT_NE(link->recv_packet_queue, nullptr);
    ASSERT_EQ(listLength(link->recv_packet_queue), 1u);
    EXPECT_EQ(link->rcvbuf_len, 0u);
    EXPECT_EQ(framed_packets, 1);
    EXPECT_EQ(framed_bytes, packet_len);

    clusterIOPacket *packet = (clusterIOPacket *)listNodeValue(listFirst(link->recv_packet_queue));
    ASSERT_NE(packet, nullptr);
    EXPECT_EQ(packet->len, packet_len);
    EXPECT_EQ(memcmp(packet->buf, msg, packet_len), 0);

    freeClusterLink(link);
    releaseSendBlock(msgblock);
}

TEST_F(ClusterLegacyIOTest, FramesMultiplePacketsFromSingleBuffer) {
    clusterLink *link = createClusterLink(NULL);
    clusterMsgSendBlock *first = createClusterMsgSendBlock(kLightPublishType, kLightMsgLen);
    clusterMsgSendBlock *second = createClusterMsgSendBlock(kLightPublishShardType, kLightMsgLen);
    clusterMsgLight *first_msg = &first->data[0].msg_light;
    clusterMsgLight *second_msg = &second->data[0].msg_light;
    size_t first_len = ntohl(first_msg->totlen);
    size_t second_len = ntohl(second_msg->totlen);

    memcpy(link->rcvbuf, first_msg, first_len);
    memcpy(link->rcvbuf + first_len, second_msg, second_len);
    link->rcvbuf_len = first_len + second_len;

    int framed_packets = 0;
    size_t framed_bytes = 0;
    EXPECT_EQ(testOnlyClusterFrameInboundPackets(link, 0, 0, &framed_packets, &framed_bytes), CLUSTER_IO_OK);
    ASSERT_NE(link->recv_packet_queue, nullptr);
    EXPECT_EQ(listLength(link->recv_packet_queue), 2u);
    EXPECT_EQ(framed_packets, 2);
    EXPECT_EQ(framed_bytes, first_len + second_len);

    freeClusterLink(link);
    releaseSendBlock(first);
    releaseSendBlock(second);
}

TEST_F(ClusterLegacyIOTest, FramingRejectsBadHeaderAndLength) {
    clusterLink *header_link = createClusterLink(NULL);
    clusterMsgSendBlock *msgblock = createClusterMsgSendBlock(kLightPublishType, kLightMsgLen);
    clusterMsgLight *msg = &msgblock->data[0].msg_light;
    size_t packet_len = ntohl(msg->totlen);

    memcpy(header_link->rcvbuf, msg, packet_len);
    memcpy(header_link->rcvbuf, "FAIL", 4);
    header_link->rcvbuf_len = packet_len;
    EXPECT_EQ(testOnlyClusterFrameInboundPackets(header_link, 0, 0, NULL, NULL), CLUSTER_IO_BAD_HEADER);

    clusterLink *length_link = createClusterLink(NULL);
    memcpy(length_link->rcvbuf, msg, packet_len);
    ((clusterMsgHeader *)length_link->rcvbuf)->totlen = htonl(CLUSTERMSG_LIGHT_MIN_LEN - 1);
    length_link->rcvbuf_len = packet_len;
    EXPECT_EQ(testOnlyClusterFrameInboundPackets(length_link, 0, 0, NULL, NULL), CLUSTER_IO_BAD_LENGTH);

    freeClusterLink(header_link);
    freeClusterLink(length_link);
    releaseSendBlock(msgblock);
}

TEST_F(ClusterLegacyIOTest, RotatesPendingAndInflightQueuesWithoutMixingNewMessages) {
    clusterLink *link = createClusterLink(NULL);
    clusterMsgSendBlock *first = createClusterMsgSendBlock(kLightPublishType, kLightMsgLen);
    clusterMsgSendBlock *second = createClusterMsgSendBlock(kLightPublishShardType, kLightMsgLen);
    clusterMsgSendBlock *third = createClusterMsgSendBlock(kLightModuleType, kLightMsgLen);

    testOnlyClusterEnqueueMessage(link, first);
    testOnlyClusterEnqueueMessage(link, second);
    EXPECT_EQ(listLength(link->send_msg_queue_pending), 2u);
    EXPECT_EQ(listLength(link->send_msg_queue_inflight), 0u);

    testOnlyClusterRotateSendMessageQueues(link);
    EXPECT_EQ(listLength(link->send_msg_queue_pending), 0u);
    EXPECT_EQ(listLength(link->send_msg_queue_inflight), 2u);
    EXPECT_EQ(listNodeValue(listFirst(link->send_msg_queue_inflight)), first);
    EXPECT_EQ(listNodeValue(listLast(link->send_msg_queue_inflight)), second);

    testOnlyClusterEnqueueMessage(link, third);
    EXPECT_EQ(listLength(link->send_msg_queue_pending), 1u);
    EXPECT_EQ(listLength(link->send_msg_queue_inflight), 2u);
    EXPECT_EQ(listNodeValue(listFirst(link->send_msg_queue_pending)), third);

    freeClusterLink(link);
    releaseSendBlock(first);
    releaseSendBlock(second);
    releaseSendBlock(third);
}

TEST_F(ClusterLegacyIOTest, PartialWriteAdvancesInflightOffsetAndPreservesOrder) {
    clusterLink *link = createClusterLink(NULL);
    fakeClusterConnection *fake = createFakeClusterConnection(4096);
    clusterMsgSendBlock *first = createClusterMsgSendBlock(kLightPublishType, kLightMsgLen);
    clusterMsgSendBlock *second = createClusterMsgSendBlock(kLightPublishShardType, kLightMsgLen);
    clusterMsgLight *first_msg = &first->data[0].msg_light;
    clusterMsgLight *second_msg = &second->data[0].msg_light;
    size_t first_len = ntohl(first_msg->totlen);
    size_t second_len = ntohl(second_msg->totlen);

    link->conn = (connection *)fake;
    testOnlyClusterEnqueueMessage(link, first);
    testOnlyClusterEnqueueMessage(link, second);
    testOnlyClusterRotateSendMessageQueues(link);

    fake->max_write = first_len - 5;
    size_t written = 0;
    int blocked = 0;
    EXPECT_EQ(testOnlyClusterWriteInflightMessages(link, first_len + second_len, &written, &blocked), CLUSTER_IO_OK);
    EXPECT_EQ(written, first_len - 5);
    EXPECT_EQ(blocked, 1);
    EXPECT_EQ(testOnlyClusterAdvanceInflightSendQueue(link, written), C_OK);
    EXPECT_EQ(link->inflight_head_msg_send_offset, first_len - 5);
    EXPECT_EQ(listLength(link->send_msg_queue_inflight), 2u);

    fake->max_write = (size_t)-1;
    EXPECT_EQ(testOnlyClusterWriteInflightMessages(link, first_len + second_len, &written, &blocked), CLUSTER_IO_OK);
    EXPECT_EQ(written, second_len + 5);
    EXPECT_EQ(blocked, 0);
    EXPECT_EQ(testOnlyClusterAdvanceInflightSendQueue(link, written), C_OK);
    EXPECT_EQ(link->inflight_head_msg_send_offset, 0u);
    EXPECT_EQ(listLength(link->send_msg_queue_inflight), 0u);
    EXPECT_EQ(fake->written, first_len + second_len);

    link->conn = NULL;
    freeFakeClusterConnection(fake);
    freeClusterLink(link);
    releaseSendBlock(first);
    releaseSendBlock(second);
}

TEST_F(ClusterLegacyIOTest, FreeClusterLinkDefersFinalFreeWhileIOIsInFlight) {
    clusterLink *link = createClusterLink(NULL);
    link->io_refs = 1;

    EXPECT_EQ(freeClusterLink(link), 0);
    EXPECT_EQ(link->async_close, 1);
    EXPECT_EQ(server.stat_cluster_io_async_closed_links, 1);

    link->io_refs = 0;
    EXPECT_EQ(freeClusterLink(link), 1);
}

TEST_F(ClusterLegacyIOTest, AcceptCompletionClearsPendingFlagAndUpdatesConnectionState) {
    fakeClusterConnection *fake = createFakeClusterConnection(1);
    fake->conn.state = CONN_STATE_ACCEPTING;
    fake->conn.flags |= CONN_FLAG_ACCEPT_PENDING_IO;

    processClusterIOAcceptDone((connection *)fake);

    EXPECT_EQ(fake->conn.flags & CONN_FLAG_ACCEPT_PENDING_IO, 0);
    EXPECT_EQ(fake->postpone_calls, 1);
    EXPECT_EQ(fake->last_postpone, 0);
    EXPECT_EQ(fake->update_calls, 1);

    freeFakeClusterConnection(fake);
}
