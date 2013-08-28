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

int32_t
kafka_message_size(struct kafka_message *m)
{
	/* crc, magic, attrs, keysize, valuesize */
	int32_t size = 14;
	if (m->key->len > 0)
		size += m->key->len;
	size += m->value->len;
	return size;
}

int32_t
kafka_message_serialize(struct kafka_message *m, uint8_t **out)
{
	message_header_t header;
	uint8_t *buf, *ptr, *crc;
	size_t buflen;

	memset(&header, 0, sizeof header);
	header.size = kafka_message_size(m);
	buflen = 8 + 4 + header.size;

	header.offset = 0;

	buf = calloc(buflen, 1);
	ptr = buf;
	crc = buf+12;

	ptr += uint64_pack(header.offset, ptr);
	ptr += uint32_pack(header.size, ptr);

	ptr += 4; /* skip crc */

	ptr += uint8_pack(header.magic, ptr);
	ptr += uint8_pack(header.attrs, ptr);

	ptr += bytestring_pack(m->key, ptr);
	ptr += bytestring_pack(m->value, ptr);
	header.crc = crc32(0, crc+4, ptr - (crc+4));
	uint32_pack(header.crc, crc);
	*out = buf;
	return ptr - buf;
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
serialize_topic_partitions(topic_partitions_t *topic, struct iovec **iovec, int i)
{
	struct iovec *iov = iovec[i];
	void *iter;
	iter = hashtable_iter(topic->partitions);
	for (; iter; hashtable_iter_next(topic->partitions, iter)) {
	}
}

int
produce_request_serialize(produce_request_t *req, struct iovec **out)
{
}
