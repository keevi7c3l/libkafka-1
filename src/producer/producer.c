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
#include <pthread.h>

#include <zookeeper/zookeeper.h>

#include <kafka.h>
#include "../kafka-private.h"

#include "../jansson/jansson.h"

static void init_watcher(zhandle_t *zp, int type, int state, const char *path, void *ctx);
static void watch_broker_ids(zhandle_t *zp, int type, int state, const char *path, void *ctx);

struct kafka_producer {
	unsigned magic;
#define KAFKA_PRODUCER_MAGIC 0xb5be14d0
	zhandle_t *zh;
	clientid_t cid;
	json_t *topics;
        json_t *brokers;
	pthread_mutex_t mtx;
};

KAFKA_EXPORT struct kafka_producer *
kafka_producer_new(const char *topic, const char *zkServer)
{
	int rc;
	struct kafka_producer *p;
	struct String_vector ids;

	p = calloc(1, sizeof *p);
	if (!p)
		return NULL;

	p->zh = zookeeper_init(zkServer, init_watcher, 10000, &p->cid, p, 0);
	if (!p->zh) {
		free(p);
		return NULL;
	}

	rc = zoo_wget_children(p->zh, "/brokers/ids", watch_broker_ids, p, &ids);
	if (rc != ZOK || ids.count == 0) {
		free(p);
		return NULL;
	}

	p->magic = KAFKA_PRODUCER_MAGIC;
	pthread_mutex_init(&p->mtx, NULL);

	pthread_mutex_lock(&p->mtx);
	p->topics = json_object();
	p->brokers = broker_map_new(p->zh, &ids);
	free_String_vector(&ids);

	return p;
}

KAFKA_EXPORT void
kafka_producer_free(struct kafka_producer *p)
{
	CHECK_OBJ_NOTNULL(p, KAFKA_PRODUCER_MAGIC);
	if (p->zh)
		zookeeper_close(p->zh);
	pthread_mutex_destroy(&p->mtx);
	free(p);
}

static void
init_watcher(zhandle_t *zp, int type, int state, const char *path, void *ctx)
{
	(void)path;
	const clientid_t *id;
	struct kafka_producer *p = (struct kafka_producer *)ctx;

	if (type == ZOO_SESSION_EVENT) {
		if (state == ZOO_CONNECTED_STATE) {
			id = zoo_client_id(zp);
			if (p->cid.client_id == 0 || p->cid.client_id != id->client_id) {
				p->cid = *id;
			}
	        } else if (state == ZOO_AUTH_FAILED_STATE) {
			zookeeper_close(zp);
			p->zh = 0;
		} else if (state == ZOO_EXPIRED_SESSION_STATE) {
			zookeeper_close(zp);
			p->zh = 0;
		}
	}
}

static void
watch_broker_ids(zhandle_t *zp, int type, int state, const char *path,
		void *ctx)
{
	/**
	 * re-map brokers
	 */
	(void)state;
	(void)path;
	struct String_vector ids;
	struct kafka_producer *p = (struct kafka_producer *)ctx;
	if (type == ZOO_CHILD_EVENT) {
		pthread_mutex_lock(&p->mtx);
		json_decref(p->brokers);
		zoo_wget_children(zp, "/brokers/ids", watch_broker_ids, p, &ids);
		p->brokers = broker_map_new(p->zh, &ids);
		pthread_mutex_unlock(&p->mtx);
	}
}
