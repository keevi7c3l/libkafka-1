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

#include <assert.h>
#include <string.h>
#include <errno.h>
#include "../kafka-private.h"

static void print_broker(broker_t *broker);
static topic_metadata_t *topic_metadata_new(char *topic, int32_t num_partitions,
					hashtable_t *partitions, int16_t error);
static topic_metadata_t *topic_metadata_from_buffer(KafkaBuffer *buffer,
						hashtable_t *brokers);
static int parse_topic_metadata_response(KafkaBuffer *buffer, hashtable_t **brokersOut,
					hashtable_t **metadataOut);

int
TopicMetadataRequest(broker_t *broker, const char **topics,
		hashtable_t **brokers, hashtable_t **metadata)
{
	/**
	 * @param topics NULL-terminated list of strings
	 *
	 * Returns brokers and topic metadata
	 */
	int32_t i, numTopics = 0;
	size_t len;
	uint8_t *ptr;
	KafkaBuffer *buffer;
	request_header_t header;
	const char *client = "libkafka";

	memset(&header, 0, sizeof header);
	header.apikey = METADATA;
	header.correlation_id = 1;

	len = sizeof header;
	len += 2 + strlen(client);

	len += 4; /* number of topics (size in bytes) */
	if (topics) {
		while (topics[numTopics]) {
			len += 2 + strlen(topics[numTopics]);
			numTopics++;
		}
	}

	buffer = KafkaBufferNew(len);
	ptr = buffer->data;
	ptr += request_header_pack(&header, client, ptr);
	ptr += uint32_pack(numTopics, ptr);

	for (i = 0; i < numTopics; i++) {
		ptr += string_pack(topics[i], ptr);
	}

	buffer->len = ptr - buffer->data;
	/* write header size */
	uint32_pack(buffer->len - 4, &buffer->data[0]);

	/* send metadata request */
	int rc;
	do {
		rc = write(broker->fd, buffer->data, buffer->len);
	} while (rc == -1 && errno == EINTR);
	if (rc == -1)
		return rc;
	assert(rc == buffer->len);
	KafkaBufferFree(buffer);

	/* read metadata response */
	int32_t size = 0;
	do {
		rc = read(broker->fd, &size, sizeof(int32_t));
	} while (rc == -1 && errno == EINTR);
	if (rc == -1)
		return rc;
	size = ntohl(size);

	topic_metadata_response_t *resp;
	buffer = KafkaBufferNew(size);
	assert(read(broker->fd, buffer->data, buffer->alloced) == size);
	buffer->len = buffer->alloced;
	buffer->cur = buffer->data;
	rc = parse_topic_metadata_response(buffer, brokers, metadata);
	KafkaBufferFree(buffer);
	return rc;
}

static void
print_broker(broker_t *broker)
{
	printf("%d %s:%d\n", broker->id, broker->hostname, broker->port);
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

static topic_metadata_t *
topic_metadata_from_buffer(KafkaBuffer *buffer, hashtable_t *brokers)
{
	int32_t i, numPartitions;
	int16_t errCode;
	char *topic;
	hashtable_t *partitions;
	buffer->cur += uint16_unpack(buffer->cur, &errCode);
	buffer->cur += string_unpack(buffer->cur, &topic);
	buffer->cur += uint32_unpack(buffer->cur, &numPartitions);
	partitions = hashtable_create(jenkins, keycmp, free, NULL);
	for (i = 0; i < numPartitions; i++) {
		char *id;
		partition_metadata_t *part;
		part = partition_metadata_from_buffer(buffer, brokers);
		id = string_builder("%d", part->partition_id);
		hashtable_set(partitions, id, part);
	}
	return topic_metadata_new(topic, numPartitions, partitions, errCode);
}

static int
parse_topic_metadata_response(KafkaBuffer *buffer, hashtable_t **brokersOut, hashtable_t **metadataOut)
{
	uint8_t *ptr;
	int32_t correlation_id;
	int32_t i, j, numBrokers, numTopics;
	hashtable_t *brokers, *metadata;

	brokers = hashtable_create(jenkins, keycmp, free, NULL);
	metadata = hashtable_create(jenkins, keycmp, NULL, NULL);

	buffer->cur += uint32_unpack(buffer->cur, &correlation_id);
	buffer->cur += uint32_unpack(buffer->cur, &numBrokers);

	/* load brokers */
	for (i = 0; i < numBrokers; i++) {
		char *bstr;
		broker_t *b;
		b = calloc(1, sizeof *b);
		buffer->cur += uint32_unpack(buffer->cur, &b->id);
		buffer->cur += string_unpack(buffer->cur, &b->hostname);
		buffer->cur += uint32_unpack(buffer->cur, &b->port);
		broker_connect(b);
		bstr = string_builder("%d", b->id);
		hashtable_set(brokers, bstr, b);
	}

	/* Read Topic Metadata */
	buffer->cur += uint32_unpack(buffer->cur, &numTopics);
	for (i = 0; i < numTopics; i++) {
		topic_metadata_t *topic;
		topic = topic_metadata_from_buffer(buffer, brokers);
		hashtable_set(metadata, topic->topic, topic);
	}
	assert(buffer->cur - buffer->data == buffer->len);
	*brokersOut = brokers;
	*metadataOut = metadata;
	return 0;
}
