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
#include "serialize.h"

inline size_t
uint8_unpack(uint8_t *ptr, uint8_t *value)
{
	memcpy(value, ptr, 1);
	return 1;
}

inline size_t
uint16_unpack(uint8_t *ptr, uint16_t *value)
{
	uint16_t v;
	memcpy(&v, ptr, 2);
	*value = ntohs(v);
	return 2;
}

inline size_t
uint32_unpack(uint8_t *ptr, uint32_t *value)
{
	uint32_t v;
	memcpy(&v, ptr, 4);
	*value = ntohl(v);
	return 4;
}

inline size_t
uint64_unpack(uint8_t *ptr, uint64_t *value)
{
	uint32_t v;
	uint32_unpack(ptr, &v);
	*value = (uint64_t)v << 32;
	uint32_unpack(ptr+4, &v);
	*value |= v;
	return 8;
}

inline size_t
string_unpack(uint8_t *ptr, char **value)
{
	/**
	 * creates a new string that must be freed.
	 */
	char *v;
	int16_t len;
	uint16_unpack(ptr, &len);
	v = calloc(len+1, 1);
	memcpy(v, ptr+2, len);
	*value = v;
	return 2 + len;
}

inline size_t
uint8_pack(uint8_t value, uint8_t *ptr)
{
	memcpy(ptr, &value, 1);
	return 1;
}

inline size_t
uint16_pack(uint16_t value, uint8_t *ptr)
{
	value = htons(value);
	memcpy(ptr, &value, 2);
	return 2;
}

inline size_t
uint32_pack(uint32_t value, uint8_t *ptr)
{
	value = htonl(value);
	memcpy(ptr, &value, 4);
	return 4;
}

inline size_t
uint64_pack(uint64_t value, uint8_t *ptr)
{
	uint32_t v;
	v = htonl((uint32_t)value);
	uint32_pack(v, ptr);
	v = htonl((uint32_t)(value >> 32));
	uint32_pack(v, ptr+4);
	return 8;
}

inline size_t
string_pack(const char *str, uint8_t *ptr)
{
	/**
	 * strings are prefixed with int16_t length
	 */
	uint16_t len = strlen(str);
	size_t offset = uint16_pack(len, ptr);
	memcpy(ptr + offset, str, len);
	return offset + len;
}

inline size_t
bytestring_pack(bytestring_t *str, uint8_t *ptr)
{
	/**
	 * bytestrings are prefixed by int32_t length
	 */
	size_t offset = uint32_pack(str->len, ptr);
	if (str->len > 0) {
		memcpy(ptr + offset, str->data, str->len);
		offset += str->len;
	}
	return offset;
}

static inline size_t
kafka_message_serialize0(struct kafka_message *m, uint8_t *ptr)
{
	int32_t crc;
	uint8_t *p = ptr;
	size_t crc_offset = 12;
	size_t crc_payload = 16;
	p += uint64_pack(0, p);
	p += uint32_pack(kafka_message_packed_size(m), p);
	p += sizeof crc; /* skip crc */
	p += uint8_pack(0, p); /* magic */
	p += uint8_pack(0, p); /* attrs */
	p += bytestring_pack(m->key, p);
	p += bytestring_pack(m->value, p);
	crc = crc32(0, ptr+crc_payload, p - (ptr+crc_payload));
	uint32_pack(crc, ptr+crc_offset);
	return p - ptr;
}

static int32_t
count_keys(hashtable_t *p)
{
	int32_t k = 0;
	void *iter = hashtable_iter(p);
	for (; iter; iter = hashtable_iter_next(p, iter)) {
		k++;
	}
	return k;
}

size_t
serialize_topic_partitions(hashtable_t *partitions, KafkaBuffer *buffer)
{
	void *iter;
	unsigned u;
	size_t offset = buffer->cur - buffer->data;
	iter = hashtable_iter(partitions);
	for (; iter; iter = hashtable_iter_next(partitions, iter)) {
		int32_t msgSetSize = 0;
		size_t msgSetSizeOffset;
		int32_t partId = *(int32_t *)hashtable_iter_key(iter);
		struct vector *messages = hashtable_iter_value(iter);

		KafkaBufferReserve(buffer, sizeof(int32_t) * 2);
		buffer->cur += uint32_pack(partId, buffer->cur);
		msgSetSizeOffset = buffer->cur - buffer->data;
		buffer->cur += sizeof(int32_t); /* msgSetSize is written later */

		for (u = 0; u < vector_size(messages); u++) {
			struct kafka_message *msg = vector_at(messages, u);
			size_t msgSize = sizeof(int64_t) + sizeof(int32_t);
			msgSize += kafka_message_packed_size(msg);
			KafkaBufferReserve(buffer, msgSize);
			buffer->cur += kafka_message_serialize0(msg, buffer->cur);
			msgSetSize += msgSize;
		}
		uint32_pack(msgSetSize, &buffer->data[msgSetSizeOffset]);
	}
	return (buffer->cur - buffer->data) - offset;
}

size_t
serialize_topics_and_partitions(hashtable_t *topicsAndPartitions, KafkaBuffer *buffer)
{
	size_t len;
	void *u, *v;

	buffer->cur += uint32_pack(count_keys(topicsAndPartitions), buffer->cur);

	u = hashtable_iter(topicsAndPartitions);
	for (; u; u = hashtable_iter_next(topicsAndPartitions, u)) {
		size_t topicLen;
		const char *topic = hashtable_iter_key(u);
		hashtable_t *partitions = hashtable_iter_value(u);
		topicLen = 2 + strlen(topic) + 4;
		KafkaBufferReserve(buffer, topicLen);
		buffer->cur += string_pack(topic, buffer->cur);
		buffer->cur += uint32_pack(count_keys(partitions), buffer->cur);
		serialize_topic_partitions(partitions, buffer);
	}
	return buffer->cur - buffer->data;
}

inline size_t
request_header_pack(request_header_t *header, const char *client, uint8_t *ptr)
{
	size_t offset = 0;
	offset += uint32_pack(header->size, ptr);
	offset += uint16_pack(header->apikey, ptr+offset);
	offset += uint16_pack(header->apiversion, ptr+offset);
	offset += uint32_pack(header->correlation_id, ptr+offset);
	offset += string_pack(client, ptr+offset);
	return offset;
}
