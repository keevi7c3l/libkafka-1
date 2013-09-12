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

#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <kafka.h>
#include "../kafka-private.h"
#include "../serialize.h"

static topic_metadata_t *topic_metadata_new(char *topic, int32_t num_partitions,
					hashtable_t *partitions, int16_t error);

static size_t topic_metadata_from_buffer(uint8_t *ptr,
					hashtable_t *brokers,
					topic_metadata_t **out);

KAFKA_EXPORT struct metadata_response *
metadata_response_read(int fd)
{
	int rc;
	uint8_t *buffer;
	int32_t size = 0;
	struct metadata_response *resp;

	do {
		rc = read(fd, &size, sizeof(int32_t));
	} while (rc == -1 && errno == EINTR);

	if (rc == -1)
		return NULL;

	size = ntohl(size);
	buffer = calloc(size, 1);
	do {
		rc = read(fd, buffer, size);
	} while (rc == -1 && errno == EINTR);

	if (rc == -1) {
		free(buffer);
		return NULL;
	}

	assert(rc == size);
	resp = metadata_response_from_buffer(buffer, size);
	free(buffer);
	return resp;
}

KAFKA_EXPORT struct metadata_response *
metadata_response_from_buffer(uint8_t *buffer, size_t size)
{
	int i;
	uint8_t *ptr;
	struct metadata_response *resp;
	ptr = buffer;
	resp = calloc(1, sizeof *resp);
	resp->brokers = hashtable_create(int32_hash, int32_cmp, NULL, NULL);
	resp->metadata = hashtable_create(jenkins, keycmp, NULL, NULL);
	ptr += uint32_unpack(ptr, &resp->correlation_id);
	ptr += uint32_unpack(ptr, &resp->numBrokers);

	for (i = 0; i < resp->numBrokers; i++) {
		/* TODO: refactor this */
		broker_t *b;
		b = calloc(1, sizeof *b);
		ptr += uint32_unpack(ptr, &b->id);
		ptr += string_unpack(ptr, &b->hostname);
		ptr += uint32_unpack(ptr, &b->port);
		broker_connect(b);
		hashtable_set(resp->brokers, &b->id, b);
	}

	ptr += uint32_unpack(ptr, &resp->numTopics);
	for (i = 0; i < resp->numTopics; i++) {
		topic_metadata_t *topic;
		ptr += topic_metadata_from_buffer(ptr, resp->brokers, &topic);
		hashtable_set(resp->metadata, topic->topic, topic);
	}

	assert(ptr - buffer == size);
	return resp;
}

static topic_metadata_t *
topic_metadata_new(char *topic, int32_t num_partitions, hashtable_t *partitions,
		int16_t error)
{
	topic_metadata_t *t;
	t = calloc(1, sizeof *t);
	t->topic = topic;
	t->num_partitions = num_partitions;
	t->partitions = partitions;
	t->error = error;
	return t;
}

static size_t
topic_metadata_from_buffer(uint8_t *ptr, hashtable_t *brokers, topic_metadata_t **out)
{
	int32_t i, numPartitions;
	int16_t errCode;
	char *topic;
	hashtable_t *partitions;
	size_t u = 0;
	u += uint16_unpack(ptr, &errCode);
        u += string_unpack(ptr+u, &topic);
	u += uint32_unpack(ptr+u, &numPartitions);
	partitions = hashtable_create(int32_hash, int32_cmp, NULL, NULL);
	for (i = 0; i < numPartitions; i++) {
		partition_metadata_t *part;
		u += partition_metadata_from_buffer(ptr+u, brokers, &part);
		hashtable_set(partitions, &part->partition_id, part);
	}
	*out = topic_metadata_new(topic, numPartitions, partitions, errCode);
	return u;
}
