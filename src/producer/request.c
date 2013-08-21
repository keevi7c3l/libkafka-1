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

#include <arpa/inet.h>
#include <string.h>
#include <assert.h>
#include "../kafka-private.h"

static inline size_t
uint8_pack(uint8_t value, uint8_t *ptr)
{
	memcpy(ptr, &value, 1);
	return 1;
}

static inline size_t
uint16_pack(uint16_t value, uint8_t *ptr)
{
	value = htons(value);
	memcpy(ptr, &value, 2);
	return 2;
}

static inline size_t
uint32_pack(uint32_t value, uint8_t *ptr)
{
	value = htonl(value);
	memcpy(ptr, &value, 4);
	return 4;
}

static inline size_t
uint64_pack(uint64_t value, uint8_t *ptr)
{
	uint32_t v;
	v = htonl((uint32_t)value);
	uint32_pack(v, ptr);
	v = htonl((uint32_t)(value >> 32));
	uint32_pack(v, ptr+4);
	return 8;
}

static inline size_t
string_pack(const char *str, uint8_t *ptr)
{
	uint16_t len = strlen(str);
	size_t offset = uint16_pack(len, ptr);
	memcpy(ptr + offset, str, len);
	return offset + len;
}

static inline size_t
bytestring_pack(bytestring_t *str, uint8_t *ptr)
{
	size_t offset = uint32_pack(str->len, ptr);
	if (str->len > 0) {
		memcpy(ptr + offset, str->data, str->len);
		return offset + str->len;
	}
	return offset;
}

uint8_t *
produce_request_serialize(produce_request_t *req, uint32_t *outlen)
{
	unsigned u;
	uint8_t *buf, *ptr;
	int16_t i16;
	int32_t i32;
        uint32_t len = 0;
	request_message_header_t *h = &req->header;
	produce_request_header_t *p = &req->pr_header;

	int32_t nTopics = 1, nPartitions = 1;
	h->size += 4 + 4;

	buf = calloc(h->size, 1);
	ptr = buf;

	ptr += uint32_pack(h->size, ptr);
	ptr += uint16_pack(h->apikey, ptr);
	ptr += uint16_pack(h->apiversion, ptr);
	ptr += uint32_pack(h->correlation_id, ptr);
	ptr += string_pack(h->client_id, ptr);

	ptr += uint16_pack(p->acks, ptr);
	ptr += uint32_pack(p->ttl, ptr);
	ptr += uint32_pack(nTopics, ptr);
	ptr += string_pack(p->topic, ptr);

	ptr += uint32_pack(nPartitions, ptr);
	ptr += uint32_pack(p->partition, ptr);
	ptr += uint32_pack(p->message_set_size, ptr);

	for (u = 0; u < req->_next; u++) {
		uint8_t *cs;
		kafka_message_t *m = req->messages[u];
		ptr += uint64_pack(m->offset, ptr);
		ptr += uint32_pack(m->size, ptr);

		cs = ptr;
		ptr += sizeof(int32_t);

		ptr += uint8_pack(m->header.magic, ptr);
		ptr += uint8_pack(m->header.attrs, ptr);
		ptr += bytestring_pack(m->key, ptr);
		ptr += bytestring_pack(m->value, ptr);

		m->header.checksum = crc32(0, cs+4, ptr - (cs+4));
		uint32_pack(m->header.checksum, cs);
	}

	assert((ptr - buf) == (h->size + 4));
	*outlen = ptr - buf;
	return buf;
}

produce_request_t *
produce_request_new(const char *topic, int partition)
{
	produce_request_t *req;
	req = calloc(1, sizeof *req);

	req->header.apikey = PRODUCE;
	req->header.apiversion = 0;
	req->header.correlation_id = 1;
	req->header.client_id = "foo";

	req->pr_header.acks = 1;
	req->pr_header.ttl = 0xFFFFFFFF;
	req->pr_header.topic = (char *)topic;
	req->pr_header.partition = partition;

	req->_length = 1;
	req->messages = calloc(req->_length, sizeof *req->messages);

	/* sizeof(apikey) + sizeof(apiversion) + sizeof(correlation_id) */
	req->header.size = 2 + 2 + 4;
	req->header.size += 2 + strlen(req->header.client_id);
	req->header.size += 2 + 4 + 4 + 4;
	req->header.size += 2 + strlen(topic);

	return req;
}

kafka_message_t *
kafka_message_new(uint8_t *payload, int32_t length)
{
	kafka_message_t *msg;
	msg = calloc(1, sizeof *msg);
	msg->size = sizeof(kafka_message_header_t);
	msg->header.magic = 0;

	msg->key = calloc(1, sizeof *msg->key);
	msg->key->len = -1;
	msg->size += 4;

	msg->value = calloc(1, sizeof *msg->value);
	msg->value->len = length;
	msg->value->data = payload;
	msg->size += 4 + length;
	return msg;
}

int
produce_request_append_message(produce_request_t *req, kafka_message_t *msg)
{
	unsigned u;
	size_t len;
	if (req->_next >= req->_length) {
		u = req->_length * 2;
		req->messages = realloc(req->messages, sizeof(*req->messages) * u);
		assert(req->messages);
		while (req->_length < u)
			req->messages[req->_length++] = NULL;
	}
	len = 8 + 4 + msg->size;
	req->header.size += len;
	req->pr_header.message_set_size += len;
	req->messages[req->_next++] = msg;
	return 0;
}
