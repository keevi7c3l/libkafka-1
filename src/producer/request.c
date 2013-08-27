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
#include "../vector.h"

produce_request_t *
produce_request_new(void)
{
	produce_request_t *req;
	req = calloc(1, sizeof *req);
	req->acks = 1;
	req->ttl = 1500;
	req->topics_partitions = vector_new(1, NULL);
	return req;
}

void
produce_request_free(produce_request_t *r)
{
	unsigned u, v;
	if (r) {
		for (u = 0; u < vector_size(r->topics_partitions); u++) {
			topic_partitions_t *topic;
			topic = vector_at(r->topics_partitions, u);
			for (v = 0; v < vector_size(topic->partitions); v++) {
				partition_messages_t *pms;
				pms = vector_at(topic->partitions, v);
				vector_free(pms->buffers);
				free(pms);
			}
			vector_free(topic->partitions);
			free(topic);
		}
		vector_free(r->topics_partitions);
	}
}
