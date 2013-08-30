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
	p += uint32_pack(kafka_message_size(m), p);
	p += sizeof crc; /* skip crc */
	p += uint8_pack(0, p); /* magic */
	p += uint8_pack(0, p); /* attrs */
	p += bytestring_pack(m->key, p);
	p += bytestring_pack(m->value, p);
	crc = crc32(0, ptr+crc_payload, p - (ptr+crc_payload));
	uint32_pack(crc, ptr+crc_offset);
	return p - ptr;
}

int32_t
kafka_message_serialize(struct kafka_message *m, uint8_t **out)
{
	uint8_t *buf;
	int32_t buflen;

	/* offset + size */
	buflen = sizeof(int64_t) + sizeof(int32_t) + kafka_message_size(m);
	buf = calloc(buflen, 1);
	kafka_message_serialize0(m, buf);
	*out = buf;
	return buflen;
}

size_t
request_message_header_pack(request_message_header_t *header,
			const char *client, uint8_t **out)
{
	uint8_t *buf, *ptr;
	buf = calloc(1, sizeof *header + 2 + strlen(client));
	ptr = buf;
	ptr += uint32_pack(header->size, ptr);
	ptr += uint16_pack(header->apikey, ptr);
	ptr += uint16_pack(header->apiversion, ptr);
	ptr += uint32_pack(header->correlation_id, ptr);
	ptr += string_pack(client, ptr);
	*out = buf;
	return ptr - buf;
}

size_t
serialize_topic_partitions(topic_partitions_t *topic, uint8_t **out)
{
	void *iter;
	uint8_t *buf, *ptr;
	unsigned u;
	size_t sz = 128;
	buf = calloc(sz, 1);
	ptr = buf;
	iter = hashtable_iter(topic->partitions);

	for (; iter; iter = hashtable_iter_next(topic->partitions, iter)) {
		int32_t msg_set_size = 0;
		partition_messages_t *partition = hashtable_iter_value(iter);
		uint8_t *msgSetSizePtr;

		ptr += uint32_pack(partition->partition, ptr);
		msgSetSizePtr = ptr;
		ptr += sizeof(int32_t); /* set this later */

		for (u = 0; u < vector_size(partition->messages); u++) {
			size_t msgSize, ptrOffset;
			struct kafka_message *msg;
			msg = vector_at(partition->messages, u);
			/* msgSize doesn't include offset+size header */
			msgSize = sizeof(int64_t) + sizeof(int32_t);
			msgSize += kafka_message_packed_size(msg);
			ptrOffset = ptr - buf;
			if (msgSize >= sz - ptrOffset) {
				size_t newSize = sz;
				do {
					newSize *= 2;
				} while (newSize - ptrOffset < msgSize);
				buf = realloc(buf, newSize);
				assert(buf);
				memset(&buf[sz], 0, newSize);
				sz = newSize;
				ptr = &buf[ptrOffset];
			}
			ptr += kafka_message_serialize0(msg, ptr);
			msg_set_size += msgSize;
		}

		uint32_pack(msg_set_size, msgSetSizePtr);
	}
//	print_bytes(buf, ptr - buf);
	*out = buf;
	return ptr - buf;
}

int
produce_request_serialize(produce_request_t *req, struct iovec **out)
{
}
