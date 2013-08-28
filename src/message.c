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
#include <string.h>
#include <kafka.h>
#include "kafka-private.h"

static void bytestring_free(bytestring_t *s)
{
	if (s) {
		if (s->data)
			free(s->data);
		free(s);
	}
}

KAFKA_EXPORT struct kafka_message *
kafka_message_new(const char *topic, const char *key, const char *value)
{
	struct kafka_message *msg;
	if (!topic)
		return NULL;
	if (!value)
		return NULL;
	msg = calloc(1, sizeof *msg);
	msg->key = calloc(1, sizeof *msg->key);
	msg->key->len = -1;
	if (key) {
		msg->key->len = strlen(key);
		msg->key->data = strdup(key);
	}
	msg->value = calloc(1, sizeof *msg->value);
	msg->value->len = strlen(value);
	msg->value->data = strdup(value);
	msg->topic = strdup(topic);
	return msg;
}

KAFKA_EXPORT void
kafka_message_free(struct kafka_message *msg)
{
	if (msg) {
		bytestring_free(msg->key);
		bytestring_free(msg->value);
		if (msg->topic)
			free(msg->topic);
	}
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
