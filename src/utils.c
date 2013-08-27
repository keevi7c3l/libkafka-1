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

#include <zookeeper/zookeeper.h>
#include "kafka-private.h"

void free_String_vector(struct String_vector *v)
{
	if (v->data) {
		int i;
		for (i = 0; i < v->count; i++)
			free(v->data[i]);
		free(v->data);
		v->data = NULL;
	}
}

char *string_builder(const char *fmt, ...)
{
    /**
     * This function replaces this pattern of code:
     *
     * int sz = strlen(foo) + strlen(bar) + 2;
     * char *buf = malloc(sz);
     * snprintf(buf, sz, "%s/%s", foo, bar);
     *
     * In favor of:
     *
     * char *buf = string_builder("%s/%s", foo, bar);
     */
    int n, sz = 32;
    char *p, *np;
    va_list ap;

    if (!(p = malloc(sz)))
        return NULL;

    while (1) {
        va_start(ap, fmt);
        n = vsnprintf(p, sz, fmt, ap);
        va_end(ap);
        if (n > -1 && n < sz)
            break;
        if (n > -1)
            sz = n+1;
        else
            sz *= 2;
        if (!(np = realloc(p, sz))) {
            free(p);
            return NULL;
        }
        p = np;
    }
    return p;
}

void
print_bytes(uint8_t *buf, size_t len)
{
	uint8_t *ptr = buf;
	for (; ptr != &buf[len]; ptr++)
		printf("0x%02X ", *ptr);
	printf("\n");
}

char *
peel_topic(const char *path)
{
	/**
	 * Given a /brokers/topics/<topic> znode prefix, return the topic.
	 */
	size_t len;
	char *topic = NULL;
	const char *ptr, *end;

	if (!path || *path == '\0')
		goto err;
	len = strlen("/brokers/topics/");
	if (strncmp(path, "/brokers/topics/", len) != 0)
		goto err;

	ptr = end = path+len;

	while (*end && *end != '/')
		end++;
	if (end - ptr > 0) {
		topic = calloc(end - ptr + 1, 1);
		memcpy(topic, ptr, end - ptr);
	}
err:
	return topic;
}

char *
peel_partition(const char *path)
{
	/**
	 * Given a /brokers/topics/<topic>/partitions/<partition>/state znode,
	 * return the partition id.
	 */
	char *id = NULL;
	const char *ptr, *end;

	if (!path || *path == '\0')
		goto err;
	end = strrchr(path, '/');
	if (strncmp(end, "/state", 6) != 0)
		goto err;
	ptr = end-1;
	while (*ptr && *ptr != '/')
		ptr--;
	ptr++;
	if (end - ptr > 0) {
		id = calloc(end - ptr + 1, 1);
		memcpy(id, ptr, end - ptr);
	}
err:
	return id;
}

size_t
jenkins(const void *key)
{
    size_t hash, i;
    const char *k = (const char *)key;
    size_t len = strlen(k);
    for (hash = i = 0; i < len; i++) {
        hash += k[i];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

int
keycmp(const void *a, const void *b)
{
	const char **aa = (const char **)a;
	const char **bb = (const char **)b;
	return strcmp(*aa, *bb) == 0;
}
