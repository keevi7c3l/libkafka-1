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
	req->topics_partitions = hashtable_create(jenkins, keycmp, free, NULL);
	return req;
}

void
produce_request_free(produce_request_t *r)
{
	void *u, *v;
	if (r) {
		u = hashtable_iter(r->topics_partitions);
		for (; u; u = hashtable_iter_next(r->topics_partitions, u)) {
			topic_partitions_t *topic;
			topic = hashtable_iter_value(u);
			v = hashtable_iter(topic->partitions);
			for (; v; v = hashtable_iter_next(topic->partitions, v)) {
				partition_messages_t *pms;
				pms = hashtable_iter_value(v);
				vector_free(pms->buffers);
				free(pms);
			}
			hashtable_destroy(topic->partitions);
			free(topic);
		}
		hashtable_destroy(r->topics_partitions);
	}
}
