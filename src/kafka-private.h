/*
 * Copyright (c) 2013, David Reynolds <david@alwaysmovefast.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _LIBKAFKA_PRIVATE_H_
#define _LIBKAFKA_PRIVATE_H_

#include <stdint.h>
#include <zookeeper/zookeeper.h>

#include "vector.h"
#include "jansson/jansson.h"
#include "jansson/hashtable.h"

#define KAFKA_EXPORT __attribute__((visibility("default")))

typedef enum {
	PRODUCE=0,
	FETCH=1,
	OFFSET=2,
	METADATA=3,
	LEADER_AND_ISR=4,
	STOP_REPLICA=5,
	OFFSET_COMMIT=6,
	OFFSET_FETCH=7
} kafka_request_types;

/* buffer.c */
typedef struct {
	size_t alloced;
	size_t len;
	uint8_t *data;
	uint8_t *cur;
} KafkaBuffer;

KafkaBuffer *KafkaBufferNew(size_t size);
void KafkaBufferFree(KafkaBuffer *buffer);
size_t KafkaBufferReserve(KafkaBuffer *buffer, size_t size);
size_t KafkaBufferResize(KafkaBuffer *buffer);

typedef struct {
	int32_t len;
	uint8_t *data;
} bytestring_t;

typedef struct {
	int32_t id;
	char *hostname;
	int32_t port;
	int fd;
} broker_t;

typedef struct {
	int16_t error;
	int32_t partition_id;
	broker_t *leader;
	hashtable_t *replicas;
	hashtable_t *isr;
} partition_metadata_t;

typedef struct {
	char *topic;
	int32_t num_partitions;
	hashtable_t *partitions;
	int16_t error;
} topic_metadata_t;

typedef struct {
	hashtable_t *brokers;
	hashtable_t *topicsMetadata;
} topic_metadata_response_t;

struct kafka_producer {
	unsigned magic;
#define KAFKA_PRODUCER_MAGIC 0xb5be14d0
	zhandle_t *zh;
	clientid_t cid;
	hashtable_t *brokers;
	hashtable_t *metadata;
	int res;
};

typedef struct {
	int64_t offset;
	int32_t size;
	int32_t crc;
	int8_t magic;
	int8_t attrs;
} __attribute__((packed)) message_header_t;

typedef struct {
	int32_t size;
	int16_t apikey;
	int16_t apiversion;
	int32_t correlation_id;
} __attribute__((packed)) request_header_t;

typedef struct {
	int32_t partition;
	struct vector *messages;
} partition_messages_t;

typedef struct {
	char *topic;
	hashtable_t *partitions;
} topic_partitions_t;

typedef struct {
	int16_t acks;
	int32_t ttl;
	hashtable_t *topics_partitions;
} produce_request_t;

struct kafka_message {
	char *topic;
	bytestring_t *key;
	bytestring_t *value;
};

/* metadata/partition_metadata.c */
partition_metadata_t *partition_metadata_new(int32_t partition_id, broker_t *broker,
					hashtable_t *replicas, hashtable_t *isr, int16_t error);
partition_metadata_t *partition_metadata_from_buffer(KafkaBuffer *buffer,
						hashtable_t *brokers);

/* broker.c */
int broker_connect(broker_t *broker);

/* utils.c */
size_t jenkins(const void *key);
int keycmp(const void *a, const void *b);

void free_String_vector(struct String_vector *v);
char *string_builder(const char *fmt, ...);
void print_bytes(uint8_t *buf, size_t len);

json_t *get_json_from_znode(zhandle_t *zh, const char *znode);
json_t *wget_json_from_znode(zhandle_t *zh, const char *znode,
			watcher_fn watcher, void *ctx);

/* message.c */
int32_t kafka_message_packed_size(struct kafka_message *m);

/* crc32.c */
uint32_t crc32(uint32_t crc, const void *buf, size_t size);

/* producer/watchers.c */
void producer_init_watcher(zhandle_t *zp, int type, int state,
			const char *path, void *ctx);

/* producer/produce_request.c */

produce_request_t *produce_request_new(int16_t sync);
void produce_request_free(produce_request_t *r);
int produce_request_append(struct kafka_producer *p, produce_request_t *req,
			struct kafka_message *msg);

/*
 * {
 *     "foo_topic": {
 *         "partition_0": [
 *             { msg 1 },
 *             { msg 2},
 *         ],
 *         "partition_1": [
 *             { msg 3 }
 *         ]
 *     },
 *     "bar_topic": {
 *         "partition_1": [
 *             { msg 4 }
 *         ]
 *     }
 * }
 */

/* metadata/metadata_request.c */

int TopicMetadataRequest(broker_t *broker, const char **topics,
			hashtable_t **brokers, hashtable_t **metadata);

/**
 * OBJ stuff taken from miniobj.h in Varnish. Written by PHK.
 */

#define ALLOC_OBJ(ptr, type_magic)               \
    do {                                         \
        (ptr) = calloc(1, sizeof *(ptr));        \
        if ((ptr) != NULL)                       \
            (ptr)->magic = (type_magic);         \
    } while (0)

#define FREE_OBJ(ptr)                           \
    do {                                        \
        (ptr)->magic = (0);                     \
        free(ptr);                              \
    } while (0)

#define CHECK_OBJ(ptr, type_magic)              \
    do {                                        \
        assert((ptr)->magic == type_magic);     \
    } while (0)

#define CHECK_OBJ_NOTNULL(ptr, type_magic)      \
    do {                                        \
        assert((ptr) != NULL);                  \
        assert((ptr)->magic == type_magic);     \
    } while (0)

#define CAST_OBJ(to, from, type_magic)         \
    do {                                       \
        (to) = (from);                         \
        if ((to) != NULL)                      \
            CHECK_OBJ((to), (type_magic));     \
    } while (0)

#define CAST_OBJ_NOTNULL(to, from, type_magic)  \
    do {                                        \
        (to) = (from);                          \
        CHECK_OBJ_NOTNULL((to), (type_magic));  \
    } while (0)

#endif
