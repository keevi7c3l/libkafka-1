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
#include <pthread.h>

#include <zookeeper/zookeeper.h>

#include <kafka.h>
#include "../kafka-private.h"

#include "../jansson/jansson.h"

static int kp_init_brokers(struct kafka_producer *p);
static int kp_init_topics(struct kafka_producer *p);
static int kp_init_topics_partitions(struct kafka_producer *p);
static json_t *kp_broker_by_id(struct kafka_producer *p, int id);

KAFKA_EXPORT struct kafka_producer *
kafka_producer_new(const char *zkServer)
{
	int rc;
	struct kafka_producer *p;

	p = calloc(1, sizeof *p);
	if (!p)
		return NULL;

	zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
	p->zh = zookeeper_init(zkServer, producer_init_watcher, 10000, &p->cid, p, 0);
	if (!p->zh) {
		free(p);
		return NULL;
	}

	p->magic = KAFKA_PRODUCER_MAGIC;
	pthread_mutex_init(&p->mtx, NULL);

	if (kp_init_brokers(p) == -1) {
		kafka_producer_free(p);
		return NULL;
	}

	if (kp_init_topics(p) == -1) {
		kafka_producer_free(p);
		return NULL;
	}

	if (kp_init_topics_partitions(p) == -1) {
		kafka_producer_free(p);
		return NULL;
	}

	return p;
}

KAFKA_EXPORT int
kafka_producer_send(struct kafka_producer *p, const char *topic,
		const char *payload)
{
	int part, partBroker;
	void *iter;
	json_t *t, *partitions;
	json_t *broker;

	CHECK_OBJ_NOTNULL(p, KAFKA_PRODUCER_MAGIC);

	if (!payload)
		return -1;

	t = json_object_get(p->topics, topic);
	if (!t)
		return -1;
	partitions = json_object_get(t, "partitions");
	if (!partitions)
		return -1;

	iter = json_object_iter(partitions);
	for (; iter; iter = json_object_iter_next(partitions, iter)) {
		json_t *list;
		list = json_object_iter_value(iter);
		assert(json_array_size(list) > 0);
		/* get first broker/replica */
		part = atoi(json_object_iter_key(iter));
		partBroker = json_integer_value(json_array_get(list, 0));
		break;
	}

	broker = kp_broker_by_id(p, partBroker);
	if (broker) {
		int fd;
		produce_request_t *req;
		kafka_message_t *msg;
		uint32_t bufsize;
		uint8_t *buf;

		req = produce_request_new(topic, part);
		msg = kafka_message_new(payload);
		produce_request_append_message(req, msg);
		buf = produce_request_serialize(req, &bufsize);
		produce_request_free(req);

		fd = json_integer_value(json_object_get(broker, "fd"));
		printf("sending to broker: %s:%" JSON_INTEGER_FORMAT "\n",
			json_string_value(json_object_get(broker, "host")),
			json_integer_value(json_object_get(broker, "port")));
		assert(write(fd, buf, bufsize) == bufsize);

		char rbuf[1024];
		bufsize = read(fd, rbuf, sizeof rbuf);
		print_bytes(rbuf, bufsize);

		free(buf);
	}
	return 0;
}

KAFKA_EXPORT void
kafka_producer_free(struct kafka_producer *p)
{
	void *iter;
	CHECK_OBJ_NOTNULL(p, KAFKA_PRODUCER_MAGIC);
	if (p->zh)
		zookeeper_close(p->zh);
	json_decref(p->topics);

	iter = json_object_iter(p->brokers);
	for (; iter; iter = json_object_iter_next(p->brokers, iter)) {
		int fd;
		json_t *obj = json_object_iter_value(iter);
		json_t *v = json_object_get(obj, "fd");
		if (v) {
			fd = json_integer_value(v);
			close(fd);
		}
	}
	json_decref(p->brokers);
	json_decref(p->topics);
	pthread_mutex_destroy(&p->mtx);
	free(p);
}

static int
kp_init_brokers(struct kafka_producer *p)
{
	int rc;
	struct String_vector ids;
	rc = zoo_wget_children(p->zh, "/brokers/ids",
			producer_watch_broker_ids, p, &ids);
	if (rc != ZOK || ids.count == 0)
		return -1;
	pthread_mutex_lock(&p->mtx);
	p->brokers = broker_map_new(p->zh, &ids);
	pthread_mutex_unlock(&p->mtx);
	free_String_vector(&ids);
	return 0;
}

static int
kp_init_topics(struct kafka_producer *p)
{
	int rc;
	struct String_vector topics;
	rc = zoo_wget_children(p->zh, "/brokers/topics",
			producer_watch_broker_topics, p, &topics);
	if (rc != ZOK || topics.count == 0)
		return -1;
	pthread_mutex_lock(&p->mtx);
	p->topics = topic_map_new(p->zh, &topics);
	pthread_mutex_unlock(&p->mtx);
	free_String_vector(&topics);
	return 0;
}

static int
kp_init_topics_partitions(struct kafka_producer *p)
{
	int rc;
	void *iter;
	struct String_vector v;

	pthread_mutex_lock(&p->mtx);
	if (p->topicsPartitions)
		json_decref(p->topicsPartitions);

	p->topicsPartitions = json_object();
	iter = json_object_iter(p->topics);

	for (; iter; iter = json_object_iter_next(p->topics, iter)) {
		char *path;
		const char *topic = json_object_iter_key(iter);
		path = string_builder("/brokers/topics/%s/partitions", topic);
		/* TODO: figure out if I should watch this znode */
		rc = zoo_get_children(p->zh, path, 0, &v);
		free(path);
		if (rc == ZOK) {
			json_t *partitions = topic_partitions_map_new(p, topic, &v);
			json_object_set(p->topicsPartitions, topic, partitions);
		}
	}
	pthread_mutex_unlock(&p->mtx);
	return 0;
}

static json_t *
kp_broker_by_id(struct kafka_producer *p, int id)
{
	char buf[33];
	memset(buf, 0, sizeof buf);
	snprintf(buf, sizeof buf, "%d", id);
	return json_object_get(p->brokers, buf);
}
