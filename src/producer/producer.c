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
#include <time.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>

#include <zookeeper/zookeeper.h>

#include <kafka.h>
#include "../kafka-private.h"

#include "../jansson/jansson.h"

static json_t *bootstrap_brokers(zhandle_t *zh);
static broker_t *kp_broker_by_id(struct kafka_producer *p, int id);

KAFKA_EXPORT struct kafka_producer *
kafka_producer_new(const char *zkServer)
{
	/**
	 * @todo With the addition of metadata requests, getting metadata from
	 * zookeeper can go away.
	 */
	int rc;
	struct kafka_producer *p;
	json_t *brokers;

	srand(time(0));

	p = calloc(1, sizeof *p);
	if (!p)
		return NULL;

	p->res = KAFKA_OK;

	zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
	p->zh = zookeeper_init(zkServer, producer_init_watcher, 10000, &p->cid, p, 0);
	if (!p->zh) {
		p->res = KAFKA_ZOOKEEPER_INIT_ERROR;
		goto finish;
	}

	p->magic = KAFKA_PRODUCER_MAGIC;
	pthread_mutex_init(&p->mtx, NULL);

	brokers = bootstrap_brokers(p->zh);
	if (!brokers) {
		p->res = KAFKA_BROKER_INIT_ERROR;
		goto finish;
	}

	/* query for metadata */
	topic_metadata_response_t *metadata_resp = NULL;
	void *iter = json_object_iter(brokers);
	for (; iter; iter = json_object_iter_next(brokers, iter)) {
		json_t *obj = json_object_iter_value(iter);
		broker_t broker;
		memset(&broker, 0, sizeof broker);
		broker.hostname = (char *)json_string_value(json_object_get(obj, "host"));
		broker.port = json_integer_value(json_object_get(obj, "port"));
		broker.id = json_integer_value(json_object_get(obj, "id"));
		broker_connect(&broker);
		metadata_resp = topic_metadata_request(&broker, NULL);
		close(broker.fd);
		if (metadata_resp)
			break;
	}
	json_decref(brokers);

	if (!metadata_resp) {
		p->res = KAFKA_METADATA_ERROR;
		goto finish;
	}

	p->brokers = metadata_resp->brokers;
	p->metadata = metadata_resp->topicsMetadata;
finish:
	return p;
}

KAFKA_EXPORT int
kafka_producer_status(struct kafka_producer *p)
{
	if (!p)
		return KAFKA_PRODUCER_ERROR;
	CHECK_OBJ(p, KAFKA_PRODUCER_MAGIC);
	return p->res;
}

static partition_metadata_t *
pick_random_partition(struct kafka_producer *p, struct kafka_message *msg)
{
	int32_t part;
	topic_metadata_t *topic;
	char partStr[33];
	memset(partStr, 0, sizeof partStr);
	topic = hashtable_get(p->metadata, msg->topic);
	if (!topic)
		return NULL;
	part = rand() % topic->num_partitions;
	snprintf(partStr, sizeof partStr, "%d", part);
	return hashtable_get(topic->partitions, partStr);
}

static int
send_request(struct kafka_producer *p, broker_t *broker, produce_request_t *req)
{
	KafkaBuffer *buffer = KafkaBufferNew(0);
	produce_request_serialize(req, buffer);

	/* TODO: do actual sending in kafka_producer_send() */
	assert(write(broker->fd, buffer->data, buffer->len) == buffer->len);
	KafkaBufferFree(buffer);

	if (req->acks != KAFKA_REQUEST_ASYNC) {
		char rbuf[1024];
		size_t bufsize;
		bufsize = read(broker->fd, rbuf, sizeof rbuf);
	}
}

KAFKA_EXPORT int
kafka_producer_send(struct kafka_producer *p, struct kafka_message *msg,
		int16_t sync)
{
	/**
	 * Kafka provides a few different synchronization levels.
	 * - KAFKA_REQUEST_ASYNC: no ack response
	 * - KAFKA_REQUEST_SYNC: ack response after message is written to log
	 * - KAFKA_REQUEST_FULL_SYNC: ack response after full replication
	 */
	produce_request_t *req;
	topic_partitions_t *topic;
	partition_messages_t *part;
	CHECK_OBJ_NOTNULL(p, KAFKA_PRODUCER_MAGIC);

	/**
	 * (for many messages)
	 * for each message:
	 *   pick broker(msg) // need to know topic-partition
	 *   get request object for broker
	 *   serialize message into that request object
	 */

	partition_metadata_t *partition = pick_random_partition(p, msg);
	assert(partition);
	broker_t *broker = partition->leader;
	if (!broker)
		return -1;

	req = produce_request_new(sync);

	topic = calloc(1, sizeof *topic);
	topic->topic = msg->topic;
	topic->partitions = hashtable_create(jenkins, keycmp, free, NULL);

	part = calloc(1, sizeof *part);
	part->messages = vector_new(1, NULL);
	part->partition = partition->partition_id;

	vector_push_back(part->messages, msg);

	char partStr[33];
	memset(partStr, 0, sizeof partStr);
	snprintf(partStr, sizeof partStr, "%d", partition->partition_id);
	hashtable_set(topic->partitions, strdup(partStr), part);
	hashtable_set(req->topics_partitions, strdup(msg->topic), topic);

	send_request(p, broker, req);
	produce_request_free(req);
	return 0;
}

KAFKA_EXPORT void
kafka_producer_free(struct kafka_producer *p)
{
	void *i, *j;
	CHECK_OBJ_NOTNULL(p, KAFKA_PRODUCER_MAGIC);
	if (p->zh)
		zookeeper_close(p->zh);

	i = hashtable_iter(p->brokers);
	for (; i; i = hashtable_iter_next(p->brokers, i)) {
		int fd;
		broker_t *broker = hashtable_iter_value(i);
		if (broker) {
			close(broker->fd);
		}
	}

	/* TODO: setup value free in hashtable_create so this can go away */
	i = hashtable_iter(p->metadata);
	for (; i; i = hashtable_iter_next(p->metadata, i)) {
		topic_metadata_t *topic = hashtable_iter_value(i);
		j = hashtable_iter(topic->partitions);
		for (; j; j = hashtable_iter_next(topic->partitions, j)) {
			partition_metadata_t *part = hashtable_iter_value(j);
			hashtable_destroy(part->replicas);
			hashtable_destroy(part->isr);
		}
		hashtable_destroy(topic->partitions);
	}
	hashtable_destroy(p->brokers);
	hashtable_destroy(p->metadata);
	pthread_mutex_destroy(&p->mtx);
	free(p);
}

static json_t *
bootstrap_brokers(zhandle_t *zh)
{
	int rc, i;
	struct String_vector ids;
	json_t *js;
	rc = zoo_get_children(zh, "/brokers/ids", 0, &ids);
	if (rc != ZOK || ids.count == 0)
		return NULL;
	js = json_object();
	for (i = 0; i < ids.count; i++) {
		char *znode;
		json_t *broker;
		znode = string_builder("/brokers/ids/%s", ids.data[i]);
		assert(znode);
		broker = get_json_from_znode(zh, znode);
		free(znode);
		if (broker) {
			json_object_set(js, ids.data[i], broker);
		}
	}
	return js;
}

static broker_t *
kp_broker_by_id(struct kafka_producer *p, int id)
{
	char buf[33];
	memset(buf, 0, sizeof buf);
	snprintf(buf, sizeof buf, "%d", id);
	return hashtable_get(p->brokers, buf);
}
