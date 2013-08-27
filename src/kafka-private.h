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

typedef struct {
	int32_t len;
	uint8_t *data;
} bytestring_t;

struct kafka_producer {
	unsigned magic;
#define KAFKA_PRODUCER_MAGIC 0xb5be14d0
	zhandle_t *zh;
	clientid_t cid;
	json_t *topics;
        json_t *brokers;
	json_t *topicsPartitions;
	pthread_mutex_t mtx;
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
} __attribute__((packed)) request_message_header_t;

typedef struct {
	int32_t partition;
	struct vector *buffers;
} partition_messages_t;

typedef struct {
	char *topic;
	struct vector *partitions;
} topic_partitions_t;

typedef struct {
	int16_t acks;
	int32_t ttl;
	struct vector *topics_partitions;
} produce_request_t;

struct kafka_message {
	char *topic;
	bytestring_t *key;
	bytestring_t *value;
};

/* broker.c */
json_t *broker_map_new(zhandle_t *zh, struct String_vector *v);
json_t *topic_map_new(zhandle_t *zh, struct String_vector *v);
json_t *topic_partitions_map_new(struct kafka_producer *p, const char *topic,
				struct String_vector *v);
json_t *get_json_from_znode(zhandle_t *zh, const char *znode);
json_t *wget_json_from_znode(zhandle_t *zh, const char *znode,
			watcher_fn watcher, void *ctx);

/* utils.c */
size_t jenkins(const void *key);
int keycmp(const void *a, const void *b);

void free_String_vector(struct String_vector *v);
char *string_builder(const char *fmt, ...);
void print_bytes(uint8_t *buf, size_t len);
char *peel_topic(const char *path);
char *peel_partition(const char *path);

/* crc32.c */
uint32_t crc32(uint32_t crc, const void *buf, size_t size);

/* producer/watchers.c */
void producer_init_watcher(zhandle_t *zp, int type, int state,
			const char *path, void *ctx);
void producer_watch_broker_ids(zhandle_t *zp, int type, int state,
			const char *path, void *ctx);
void producer_watch_broker_topics(zhandle_t *zp, int type, int state,
				const char *path, void *ctx);
void watch_topic_partition_state(zhandle_t *zp, int type, int state,
				const char *path, void *ctx);

/* producer/request.c */
enum {PRODUCE=0, FETCH=1, MULTIFETCH=2, MULTIPRODUCE=3, OFFSETS=4};

/*
 * [(topic, (partition, messages), (topic, (partition, messages)))]
 */

/*
 * {
 *     "metadata": { "topics": 2 },
 *     "foo_topic": {
 *         "metadata": { "partitions": 2 },
 *         "partition_0": [
 *             { msg 1 },
 *             { msg 2},
 *         ],
 *         "partition_1": [
 *             { msg 3 }
 *         ]
 *     },
 *     "bar_topic": {
 *         "metadata": { "partitions": 1 },
 *         "partition_1": [
 *             { msg 4 }
 *         ]
 *     }
 * }
 */

produce_request_t *produce_request_new(void);
void produce_request_free(produce_request_t *r);
int produce_request_append(struct kafka_producer *p, produce_request_t *req,
			struct kafka_message *msg);
//int produce_request_append_message(produce_request_t *req, kafka_message_t *msg);
uint8_t *produce_request_serialize(produce_request_t *req, uint32_t *outlen);

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
