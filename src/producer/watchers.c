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

#include <zookeeper/zookeeper.h>
#include "../kafka-private.h"

void
producer_init_watcher(zhandle_t *zp, int type, int state, const char *path,
		void *ctx)
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

void
producer_watch_broker_topics(zhandle_t *zp, int type, int state,
			const char *path, void *ctx)
{
	/**
	 * re-map topics
	 */
	(void)state;
	(void)path;
	struct String_vector topics;
	struct kafka_producer *p = (struct kafka_producer *)ctx;
	if (type == ZOO_CHILD_EVENT) {
		pthread_mutex_lock(&p->mtx);
		if (p->topics) {
			json_decref(p->topics);
		}
		zoo_wget_children(zp, "/brokers/topics",
				producer_watch_broker_topics, p, &topics);
		p->topics = topic_map_new(p->zh, &topics);
		pthread_mutex_unlock(&p->mtx);
	}
}

void watch_topic_partition_state(zhandle_t *zp, int type, int state,
				const char *path, void *ctx)
{
	(void)state;
	struct kafka_producer *p = (struct kafka_producer *)ctx;
	if (type == ZOO_CHANGED_EVENT) {
		char *topic;
		char *partition;
		json_t *t, *state;
		topic = peel_topic(path);
		if (!topic) {
			return;
		}
		partition = peel_partition(path);
		if (!partition) {
			free(topic);
			return;
		}

		pthread_mutex_lock(&p->mtx);

		t = json_object_get(p->topicsPartitions, topic);
		if (t) {
			state = wget_json_from_znode(zp, path,
						watch_topic_partition_state, p);

			json_t *part = json_object_get(t, partition);
			if (state) {
				json_object_set(t, partition, state);
			}
		}

		pthread_mutex_unlock(&p->mtx);

		free(topic);
		free(partition);
	}
}
