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
#include <errno.h>
#include <time.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <poll.h>

#include <zookeeper/zookeeper.h>

#include <kafka.h>
#include "../kafka-private.h"

#include "../jansson/jansson.h"

static void producer_metadata_free(struct kafka_producer *p);
static int bootstrap_metadata(zhandle_t *zh, hashtable_t **brokers,
			hashtable_t **metadata);
static json_t *bootstrap_brokers(zhandle_t *zh);
static broker_t *kp_broker_by_id(struct kafka_producer *p, int id);

KAFKA_EXPORT struct kafka_producer *
kafka_producer_new(const char *zkServer)
{
	int rc;
	struct kafka_producer *p;
	topic_metadata_response_t *metadata_resp;

	srand(time(0));

	p = calloc(1, sizeof *p);
	if (!p)
		return NULL;

	p->res = KAFKA_OK;

	/* TODO: make this configurable */
	zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);

	if (zkServer) {
		p->zh = zookeeper_init(zkServer, producer_init_watcher, 10000,
				&p->cid, p, 0);
	} else {
		/* default to localhost */
		p->zh = zookeeper_init("localhost:2181", producer_init_watcher,
				10000, &p->cid, p, 0);
	}

	if (!p->zh) {
		p->res = KAFKA_ZOOKEEPER_INIT_ERROR;
		goto finish;
	}

	p->magic = KAFKA_PRODUCER_MAGIC;

	if (bootstrap_metadata(p->zh, &p->brokers, &p->metadata) == -1) {
		p->res = KAFKA_METADATA_ERROR;
		goto finish;
	}
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
topic_partition_leader(struct kafka_producer *p, const char *topic, int32_t partition_id)
{
	topic_metadata_t *topicMetadata;
	topicMetadata = hashtable_get(p->metadata, topic);
	if (topicMetadata) {
		return hashtable_get(topicMetadata->partitions, &partition_id);
	}
	return NULL;
}

static partition_metadata_t *
pick_random_topic_partition(struct kafka_producer *p, struct kafka_message *msg)
{
	int32_t part;
	topic_metadata_t *topic;
	topic = hashtable_get(p->metadata, msg->topic);
	if (!topic)
		return NULL;
	part = rand() % topic->num_partitions;
	return hashtable_get(topic->partitions, &part);
}

static int
parse_produce_response(KafkaBuffer *buffer)
{
	int32_t correlation_id, num_topics, i, j;
	buffer->cur += uint32_unpack(buffer->cur, &correlation_id);
	buffer->cur += uint32_unpack(buffer->cur, &num_topics);
	for (i = 0; i < num_topics; i++) {
		int32_t num_partitions;
		char *topic;
		buffer->cur += string_unpack(buffer->cur, &topic);
		buffer->cur += uint32_unpack(buffer->cur, &num_partitions);
		for (j = 0; j < num_partitions; j++) {
			int32_t partition;
			int16_t error;
			int64_t offset;
			buffer->cur += uint32_unpack(buffer->cur, &partition);
			buffer->cur += uint16_unpack(buffer->cur, &error);
			buffer->cur += uint64_unpack(buffer->cur, &offset);
			if (error)
				return error;
		}
	}
}

static int
send_request(struct kafka_producer *p, broker_t *broker, produce_request_t *req)
{
	int rc;
	int res = 0;
	KafkaBuffer *buffer = KafkaBufferNew(0);
	produce_request_serialize(req, buffer);

	/* TODO: do actual sending in kafka_producer_send() */
	do {
		rc = write(broker->fd, buffer->data, buffer->len);
	} while (rc == -1 && errno == EINTR);

	if (rc == -1) {
		res = -1;
		goto finish;
	}

	if (req->acks != KAFKA_REQUEST_ASYNC) {
		int32_t rlen = 0;

		rc = read(broker->fd, &rlen, 4);
		if (rc == -1) {
			res = -1;
			goto finish;
		}

		rlen = ntohl(rlen);

		if (rlen > 0) {
			KafkaBuffer *rbuf = KafkaBufferNew(rlen);
			rc = read(broker->fd, rbuf->data, rbuf->alloced);
			if (rc != rlen) {
				res = -1;
				KafkaBufferFree(rbuf);
				goto finish;
			}
			rbuf->len = rbuf->alloced;
			rbuf->cur = rbuf->data;
			/**
			 * TODO: need to return errors for each topic-partition
			 * error.
			 */
			rc = parse_produce_response(rbuf);
			if (rc != KAFKA_OK) {
				res = -1;
			}
			KafkaBufferFree(rbuf);
		}
	}
finish:
	KafkaBufferFree(buffer);
	return res;
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
	 *
	 * Async should be handled a bit differently. You need to build up a
	 * data structure of N messages and batch them all at once at a later
	 * time. Probably using a separate thread.
	 */
	int res;
	produce_request_t *req;
	topic_partitions_t *topic;
	partition_messages_t *part;
	partition_metadata_t *tp;

	CHECK_OBJ_NOTNULL(p, KAFKA_PRODUCER_MAGIC);

	/**
	 * This works for sending a single message but not when you're batching
	 * many messages to different topics partitions and brokers.
	 *
	 * You would have to rebuild every request object and reassign messages
	 * depending on which broker now controls which topic-partition (right?)
	 *
	 * Apache Kafka's (v0.8) producer library loops over each message and
	 * groups them into the appropriate buckets on send.
	 *
	 * See partitionAndCollate() in producer/async/DefaultEventHandler.scala
	 */

	tp = pick_random_topic_partition(p, msg);
	if (!tp || !tp->leader) {
		return -1;
	}

	req = produce_request_new(sync);
	topic = calloc(1, sizeof *topic);
	topic->topic = msg->topic;
	topic->partitions = hashtable_create(int32_hash, int32_cmp, NULL, NULL);

	part = calloc(1, sizeof *part);
	part->messages = vector_new(1, NULL);
	part->partition = tp->partition_id;

	vector_push_back(part->messages, msg);

	hashtable_set(topic->partitions, &tp->partition_id, part);
	hashtable_set(req->topics_partitions, strdup(msg->topic), topic);

	int retries = 4;

	while (retries > 0) {
		res = 0;
		int rc = send_request(p, tp->leader, req);
		if (rc == 0) {
			break;
		}

		fprintf(stderr, "%s\n", kafka_status_string(rc));

		if (rc == KAFKA_MESSAGE_SIZE_TOO_LARGE) {
			res = -1;
			break;
		}

		/* request failed, update metadata and try again */
		producer_metadata_free(p);
		if (bootstrap_metadata(p->zh, &p->brokers, &p->metadata) == -1) {
			res = -KAFKA_METADATA_ERROR;
			break;
		}

		/* pick new leader for partition */
		tp = topic_partition_leader(p, msg->topic, part->partition);
		if (!tp || !tp->leader) {
			res = -1;
			break;
		}
		retries--;
	}

	produce_request_free(req);
	if (retries == 0)
		res = -1;
	return res;
}

static void
producer_metadata_free(struct kafka_producer *p)
{
	void *i, *j;

	i = hashtable_iter(p->brokers);
	for (; i; i = hashtable_iter_next(p->brokers, i)) {
		int fd;
		broker_t *broker = hashtable_iter_value(i);
		if (broker) {
			close(broker->fd);
			free(broker->hostname);
			free(broker);
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
			free(part);
		}
		hashtable_destroy(topic->partitions);
		free(topic->topic);
		free(topic);
	}
	hashtable_destroy(p->brokers);
	hashtable_destroy(p->metadata);
}

KAFKA_EXPORT void
kafka_producer_free(struct kafka_producer *p)
{
	CHECK_OBJ_NOTNULL(p, KAFKA_PRODUCER_MAGIC);
	if (p->zh)
		zookeeper_close(p->zh);
	producer_metadata_free(p);
	free(p);
}

static int
bootstrap_metadata(zhandle_t *zh, hashtable_t **brokersOut, hashtable_t **metadataOut)
{
	int rc = 0;
	void *iter;
	json_t *brokers;
	brokers = bootstrap_brokers(zh);
	if (!brokers) {
		return -1;
	}

	/* query for metadata */
	iter = json_object_iter(brokers);
	for (; iter; iter = json_object_iter_next(brokers, iter)) {
		json_t *obj = json_object_iter_value(iter);
		broker_t broker;
		memset(&broker, 0, sizeof broker);
		broker.hostname = (char *)json_string_value(json_object_get(obj, "host"));
		broker.port = json_integer_value(json_object_get(obj, "port"));
		broker.id = json_integer_value(json_object_get(obj, "id"));
		broker_connect(&broker);
		/* TODO: check for "Leader Not Available" responses and wait/retry */
		rc = TopicMetadataRequest(&broker, NULL, brokersOut, metadataOut);
		close(broker.fd);
		if (rc == 0)
			break;
	}
	json_decref(brokers);
	return rc;
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
			/* steal reference to broker */
			json_object_set_new(js, ids.data[i], broker);
		}
	}
	return js;
}
