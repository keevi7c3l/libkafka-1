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
#include "../kafka-private.h"

static void
print_broker(broker_t *broker)
{
	printf("%d %s:%d\n", broker->id, broker->hostname, broker->port);
}

static topic_metadata_t *
topic_metadata_new(char *topic, partition_metadata_t **partitionsMetadata,
		int16_t error)
{
	topic_metadata_t *t;
	t = calloc(1, sizeof *t);
	t->topic = topic;
	t->partitions_metadata = partitionsMetadata;
	t->error = error;
	return t;
}

static topic_metadata_t *
topic_metadata_from_buffer(KafkaBuffer *buffer, hashtable_t *brokers)
{
	int32_t i, numPartitions;
	int16_t errCode;
	char *topic;
	partition_metadata_t **partitions;
	buffer->cur += uint16_unpack(buffer->cur, &errCode);
	buffer->cur += string_unpack(buffer->cur, &topic);
	buffer->cur += uint32_unpack(buffer->cur, &numPartitions);
	partitions = calloc(numPartitions, sizeof *partitions);
	for (i = 0; i < numPartitions; i++) {
		partitions[i] = partition_metadata_from_buffer(buffer, brokers);
	}
	return topic_metadata_new(topic, partitions, errCode);
}

static topic_metadata_response_t *
parse_topic_metadata_response(KafkaBuffer *buffer)
{
	/**
	 * @todo: return brokers and topics metadata.
	 */
	uint8_t *ptr;
	int32_t correlation_id;
	int32_t i, j, numBrokers, numTopics;
	topic_metadata_response_t *resp;

	resp = calloc(1, sizeof *resp);
	resp->brokers = hashtable_create(jenkins, keycmp, free, NULL);
	resp->topicsMetadata = hashtable_create(jenkins, keycmp, NULL, NULL);

	buffer->cur += uint32_unpack(buffer->cur, &correlation_id);
	buffer->cur += uint32_unpack(buffer->cur, &numBrokers);

	/* load brokers */
	for (i = 0; i < numBrokers; i++) {
		char bstr[33];
		broker_t *b;
		b = calloc(1, sizeof *b);
		buffer->cur += uint32_unpack(buffer->cur, &b->id);
		buffer->cur += string_unpack(buffer->cur, &b->hostname);
		buffer->cur += uint32_unpack(buffer->cur, &b->port);
		broker_connect(b);
		memset(bstr, 0, sizeof bstr);
		snprintf(bstr, sizeof bstr, "%d", b->id);
		hashtable_set(resp->brokers, strdup(bstr), b);
	}

	/* Read Topic Metadata */
	buffer->cur += uint32_unpack(buffer->cur, &numTopics);
	for (i = 0; i < numTopics; i++) {
		topic_metadata_t *topic;
		topic = topic_metadata_from_buffer(buffer, resp->brokers);
		hashtable_set(resp->topicsMetadata, topic->topic, topic);
	}
	assert(buffer->cur - buffer->data == buffer->len);
	return resp;
}

topic_metadata_response_t *
topic_metadata_request(broker_t *broker, const char **topics)
{
	/**
	 * @param topics NULL-terminated list of strings
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
	assert(write(broker->fd, buffer->data, buffer->len) == buffer->len);
	KafkaBufferFree(buffer);

	/* read metadata response */
	int32_t size = 0;
	read(broker->fd, &size, sizeof(int32_t));
	size = ntohl(size);

	topic_metadata_response_t *resp;
	buffer = KafkaBufferNew(size);
	assert(read(broker->fd, buffer->data, buffer->alloced) == size);
	buffer->len = buffer->alloced;
	buffer->cur = buffer->data;
	resp = parse_topic_metadata_response(buffer);
	KafkaBufferFree(buffer);
	return resp;
}
