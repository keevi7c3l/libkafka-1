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
#include "../kafka-private.h"

void
topic_metadata_request(json_t *broker, const char **topics)
{
	/**
	 * @param topics NULL-terminated list of strings
	 */
	int32_t i, numTopics = 0;
	size_t len;
	uint8_t *ptr;
	KafkaBuffer *buffer;
	request_header_t header;
	const char *client = "libkafka";

	memset(&header, 0, sizeof header);
	header.apikey = METADATA;
	header.correlation_id = 1;

	len = sizeof header;
	len += 2 + strlen(client);

	len += 4; /* number of topics */
	if (topics) {
		while (topics[numTopics]) {
			len += 2 + strlen(topics[numTopics]);
			numTopics++;
		}
	}

	buffer = KafkaBufferNew(len);
	ptr = buffer->data;
	ptr += request_header_pack(&header, client, ptr);
	ptr += uint32_pack(numTopics, ptr);

	for (i = 0; i < numTopics; i++) {
		ptr += string_pack(topics[i], ptr);
	}

	buffer->len = ptr - buffer->data;
	/* write header size */
	uint32_pack(buffer->len - 4, &buffer->data[0]);

	printf("metadata request\n");
	print_bytes(buffer->data, buffer->len);

	int fd = json_integer_value(json_object_get(broker, "fd"));
	assert(write(fd, buffer->data, buffer->len) == buffer->len);
	KafkaBufferFree(buffer);

	char rbuf[1024];
	size_t bufsize;
	bufsize = read(fd, rbuf, sizeof rbuf);
	printf("metadata response:\n");
	printf("%ld\n", bufsize);
	print_bytes(rbuf, bufsize);
}
