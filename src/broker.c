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

#include <zookeeper/zookeeper.h>

#include "kafka-private.h"
#include "jansson/jansson.h"

json_t *
broker_map_new(zhandle_t *zh, struct String_vector *v)
{
	int i, rc;
	json_t *out = json_object();
	for (i = 0; i < v->count; i++) {
		char znode[256], buf[1024];
		int len;
		json_t *broker;
		json_error_t err;

		memset(znode, 0, sizeof znode);
		memset(buf, 0, sizeof buf);
		len = sizeof buf;

		snprintf(znode, sizeof znode, "/brokers/ids/%s", v->data[i]);

		rc = zoo_get(zh, znode, 0, buf, &len, NULL);
		assert((size_t)len < sizeof buf);
		if (rc != ZOK)
			return NULL;

		broker = json_loads(buf, 0, &err);
		if (!broker)
			return NULL;

		json_object_set(out, v->data[i], broker);
	}
	return out;
}
