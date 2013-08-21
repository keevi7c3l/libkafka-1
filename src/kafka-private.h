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
#include "jansson/jansson.h"

#define KAFKA_EXPORT __attribute__((visibility("default")))

/* broker.c */
json_t *broker_map_new(zhandle_t *zh, struct String_vector *v);
json_t *topic_map_new(zhandle_t *zh, struct String_vector *v);

/* utils.c */
void free_String_vector(struct String_vector *v);
char *string_builder(const char *fmt, ...);
void print_bytes(uint8_t *buf, size_t len);

/* crc32.c */
uint32_t crc32(uint32_t crc, const void *buf, size_t size);

/* producer/request.c */

enum {PRODUCE=0, FETCH=1, MULTIFETCH=2, MULTIPRODUCE=3, OFFSETS=4};

typedef struct {
	int32_t len;
	uint8_t *data;
} bytestring_t;

typedef struct {
	int32_t size;
	int16_t apikey;
	int16_t apiversion;
	int32_t correlation_id;
	char *client_id;
} request_message_header_t;

typedef struct {
	int32_t checksum;
	int8_t magic;
	int8_t attrs;
} __attribute__((packed)) kafka_message_header_t;

typedef struct {
	int64_t offset;
	int32_t size;
	kafka_message_header_t header;
	bytestring_t *key;
	bytestring_t *value;
} kafka_message_t;

typedef struct {
	int16_t acks;
	int32_t ttl;
	char *topic;
	int32_t partition;
	int32_t message_set_size;
} produce_request_header_t;

typedef struct {
	request_message_header_t header;
	produce_request_header_t pr_header;
	kafka_message_t **messages;
	unsigned _length, _next;
} produce_request_t;

produce_request_t *produce_request_new(const char *topic, int partition);
void produce_request_free(produce_request_t *r);
kafka_message_t *kafka_message_new(const char *str);
int produce_request_append_message(produce_request_t *req, kafka_message_t *msg);
uint8_t *produce_request_serialize(produce_request_t *req, uint32_t *outlen);

#if 0
typedef struct {
    int32_t length;
    int16_t type;
    int16_t topic_length;
    char *topic; /* not null-terminated, only topic_length bytes */
    int32_t partition;
} request_header_t;

typedef struct {
    int32_t length;
    int8_t magic;
    int8_t compression;
    uint32_t checksum;
    uint8_t *payload;
} kafka_message_t;

typedef struct {
    request_header_t *header;
    int32_t messages_length;
    kafka_message_t **messages;
    unsigned _length, _next; /* for managing messages pointers */
} produce_request_t;

produce_request_t *produce_request_new(const char *topic, int partition);
kafka_message_t *kafka_message_new(uint8_t *payload, int32_t length);
void kafka_message_free(kafka_message_t *msg);
int produce_request_append_message(produce_request_t *req, kafka_message_t *msg);
uint8_t *produce_request_serialize(produce_request_t *req, uint32_t *outlen);
#endif

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
