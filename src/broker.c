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
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <zookeeper/zookeeper.h>

#include "kafka-private.h"
#include "jansson/jansson.h"

static unsigned long
gethostaddress(const struct hostent *h)
{
	/**
	 * returns host address in network byte order
	 */
	struct in_addr **list;
	list = (struct in_addr **)h->h_addr_list;
	return inet_addr(inet_ntoa(*list[0]));
}

static int
broker_connect(json_t *broker)
{
	int fd;
	unsigned slen;
	unsigned long hostaddr;
	struct sockaddr_in sin;
	struct hostent *he;
	const char *host;
	int port;
	host = json_string_value(json_object_get(broker, "host"));
	port = json_integer_value(json_object_get(broker, "port"));
	he = gethostbyname(host);
	if (!he)
		return -1;
	hostaddr = gethostaddress(he);
	slen = sizeof sin;
	memset(&sin, 0, slen);
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	sin.sin_addr.s_addr = hostaddr;
	fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd == -1)
		return -1;
	if (connect(fd, (struct sockaddr *)&sin, slen) == -1)
		return -1;
	return fd;
}

static json_t *
get_json_from_znode(zhandle_t *zh, const char *znode)
{
	int rc;
	json_t *js;
	json_error_t err;
	char buf[1024];
	int len = sizeof buf;
	memset(buf, 0, len);
	rc = zoo_get(zh, znode, 0, buf, &len, NULL);
	assert((size_t)len < sizeof buf);
	if (rc != ZOK)
		return NULL;
	return json_loads(buf, 0, &err);
}

json_t *
broker_map_new(zhandle_t *zh, struct String_vector *v)
{
	int i, rc;
	json_t *out = json_object();
	for (i = 0; i < v->count; i++) {
		int fd;
		char *znode;
		json_t *broker;
		json_error_t err;

		znode = string_builder("/brokers/ids/%s", v->data[i]);
		assert(znode);		
		broker = get_json_from_znode(zh, znode);
		free(znode);

		if (!broker)
			continue;

		fd = broker_connect(broker);
		if (fd != -1) {
			json_object_set(broker, "fd", json_integer(fd));
			json_object_set(out, v->data[i], broker);
		}
	}
	return out;
}

json_t *
topic_map_new(zhandle_t *zh, struct String_vector *v)
{
	int i, rc;
	json_t *out = json_object();
	for (i = 0; i < v->count; i++) {
		char *znode;
		json_t *topic;
		json_error_t err;

		znode = string_builder("/brokers/topics/%s", v->data[i]);
		assert(znode);
		topic = get_json_from_znode(zh, znode);
		free(znode);

		if (!topic)
			continue;

		json_object_set(out, v->data[i], topic);
	}
	return out;
}
