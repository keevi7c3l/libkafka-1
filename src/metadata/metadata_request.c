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
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <kafka.h>
#include "../kafka-private.h"
#include "../serialize.h"
#include "../vector.h"

static void print_broker(broker_t *broker);

KAFKA_EXPORT struct metadata_request *
metadata_request_new(const char **topics, const char *client)
{
	uint8_t *ptr;
	struct metadata_request *r;
	r = calloc(1, sizeof *r);
	r->header.apikey = METADATA;
	r->header.correlation_id = 1;
	r->header.size = sizeof(request_header_t) - 4; /* don't count its own size property */
	r->header.size += 2 + strlen(client);
	r->header.size += sizeof r->numTopics;

	/* copy these over? */
	r->topics = (char **)topics;
	r->client = (char *)client;

	if (topics) {
		while (topics[r->numTopics]) {
			r->header.size += 2 + strlen(topics[r->numTopics++]);
			r->numTopics++;
		}
	}
	return r;
}

KAFKA_EXPORT size_t
metadata_request_to_buffer(struct metadata_request *r, uint8_t **out)
{
	int i;
	uint8_t *ptr;
	size_t u = 0;
	size_t len = r->header.size + 4;
	*out = calloc(len, 1);
	ptr = *out;
	ptr += request_header_pack(&r->header, r->client, ptr);
	ptr += uint32_pack(r->numTopics, ptr);
	for (i = 0; i < r->numTopics; i++) {
		ptr += string_pack(r->topics[i], ptr);
	}
	return len;
}

KAFKA_EXPORT int
metadata_request_write(int fd, struct metadata_request *r)
{
	int rc;
	size_t len;
	uint8_t *buffer;

	len = metadata_request_to_buffer(r, &buffer);

	do {
		rc = write(fd, buffer, len);
	} while (rc == -1 && errno == EINTR);

	if (rc == -1)
		return rc;

	assert(rc == len);
	free(buffer);
	return rc;
}

struct metadata_response *
topic_metadata_request(broker_t *broker, const char **topics)
{
	/**
	 * @param topics NULL-terminated list of strings
	 */
	int rc;
	struct metadata_request *req;
	req = metadata_request_new(topics, "libkafka");
	rc = metadata_request_write(broker->fd, req);
	if (rc == -1)
		return NULL;
	return metadata_response_read(broker->fd);
}

static void
print_broker(broker_t *broker)
{
	printf("%d %s:%d\n", broker->id, broker->hostname, broker->port);
}
