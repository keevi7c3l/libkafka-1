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

#include <stdio.h>
#include <string.h>
#include <kafka.h>

int main(int argc, char **argv)
{
	int rc = 0;
	char *zkServer = "localhost:2181";
	struct kafka_producer *p;
	if (argc == 2) {
		zkServer = argv[1];
	}
	p = kafka_producer_new(zkServer);
	rc = kafka_producer_status(p);
	if (rc == KAFKA_OK) {
		struct kafka_message_set *set = kafka_message_set_new();
		kafka_message_set_append(set, kafka_message_new("test", "test1"));
		kafka_message_set_append(set, kafka_message_new("test", "test2"));
		kafka_message_set_append(set, kafka_message_new("test", "test3"));
		kafka_message_set_append(set, kafka_message_new("foobar", "foobar1"));
		kafka_message_set_append(set, kafka_message_new("foobar", "foobar2"));
		kafka_message_set_append(set, kafka_message_new("foobar", "foobar3"));

		if (kafka_producer_send_batch(p, set, KAFKA_REQUEST_FULL_SYNC) != KAFKA_OK) {
			fprintf(stderr, "batch request failed\n");
			rc = -1;
		}

		kafka_message_set_free(set);
		kafka_producer_free(p);
	} else {
		fprintf(stderr, "%s\n", kafka_status_string(rc));
	}
	return rc;
}
