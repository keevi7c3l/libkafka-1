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

#include <kafka.h>
#include "kafka-private.h"

static void msg_free(void *ptr);

KAFKA_EXPORT struct kafka_message_set *
kafka_message_set_new(void)
{
	struct kafka_message_set *set;
	set = calloc(1, sizeof *set);
	set->messages = vector_new(0, msg_free);
	return set;
}

KAFKA_EXPORT void
kafka_message_set_free(struct kafka_message_set *set)
{
	if (set) {
		vector_free(set->messages);
		free(set);
	}
}

KAFKA_EXPORT size_t
kafka_message_set_append(struct kafka_message_set *set, struct kafka_message *msg)
{
	assert(set);
	vector_push_back(set->messages, msg);
	return vector_size(set->messages);
}

static void
msg_free(void *ptr)
{
	struct kafka_message *msg = (struct kafka_message *)ptr;
	kafka_message_free(msg);
}
