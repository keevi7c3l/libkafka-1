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

static partition_metadata_t *pick_random_topic_partition(struct kafka_producer *p,
							struct kafka_message *msg);

static hashtable_t *parse_produce_response(KafkaBuffer *buffer);
static int send_produce_request(struct kafka_producer *p, int32_t brokerId,
				hashtable_t *topics_partitions, int16_t sync,
				hashtable_t **failuresOut);

static hashtable_t *broker_message_map(struct kafka_producer *p,
				struct vector *messages);
static void broker_message_map_free(hashtable_t *map);

static int handle_topics_partitions_failures(hashtable_t *topicsPartitions,
					hashtable_t *failedRequest,
					struct vector *failedMessages);

static int dispatch(struct kafka_producer *p, struct vector *messages,
		int16_t sync, struct vector **out);

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
	CHECK_OBJ_NOTNULL(p, KAFKA_PRODUCER_MAGIC);

	struct vector *vec = vector_new(1, NULL);
	vector_push_back(vec, msg);
	int retries = 4;

	while (retries > 0) {
		struct vector *failures;
		res = dispatch(p, vec, sync, &failures);
		if (res == KAFKA_OK)
			break;

		/**
		 * Sometimes a failure happens because a broker just dies.
		 * In this case there will be no response, but res != KAFKA_OK.
		 * Just update metadata and retry the request.
		 */

		if (failures) {
			/* vector of messages that failed. simple enough to retry */
			vec = failures;
		}

		producer_metadata_free(p);
		if (bootstrap_metadata(p->zh, &p->brokers, &p->metadata) == -1) {
			res = KAFKA_METADATA_ERROR;
			break;
		}
		retries--;
	}

	if (retries == 0)
		res = -1;
	return res;
}

KAFKA_EXPORT int
kafka_producer_send_batch(struct kafka_producer *p,
			struct kafka_message_set *set, int16_t sync)
{
	int res, retries = 4;
	CHECK_OBJ_NOTNULL(p, KAFKA_PRODUCER_MAGIC);

	if (!set)
		return -1;
	struct vector *vec = set->messages;

	while (retries > 0) {
		struct vector *failures;
		res = dispatch(p, vec, sync, &failures);
		if (res == KAFKA_OK)
			break;

		if (failures) {
			/* vector of messages that failed. simple enough to retry */
			vec = failures;
		}

		producer_metadata_free(p);
		if (bootstrap_metadata(p->zh, &p->brokers, &p->metadata) == -1) {
			res = KAFKA_METADATA_ERROR;
			break;
		}
		retries--;
	}

	if (retries == 0)
		res = -1;
	return res;
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

static void
mark_failure(hashtable_t *failures, const char *topic, int32_t partition)
{
	int32_t *pid;
	struct vector *partitionFailures;
	assert(failures);
	partitionFailures = hashtable_get(failures, topic);
	if (!partitionFailures) {
		partitionFailures = vector_new(0, free);
		hashtable_set(failures, strdup(topic), partitionFailures);
	}
	pid = malloc(sizeof(int32_t));
	*pid = partition;
	vector_push_back(partitionFailures, pid);
}

static hashtable_t *
parse_produce_response(KafkaBuffer *buffer)
{
	hashtable_t *failures = NULL;
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

			if (error != KAFKA_OK) {
				fprintf(stderr, "%s\n", kafka_status_string(error));
				if (!failures) {
					failures = hashtable_create(jenkins, keycmp, free, NULL);
				}
				mark_failure(failures, topic, partition);
			}
		}
	}
	return failures;
}

static hashtable_t *
mark_every_failure(hashtable_t *topicsAndPartitions)
{
	/**
	 * Marks every topic partition as a failure.
	 */
	void *i, *j;
	hashtable_t *failures = hashtable_create(jenkins, keycmp, free, NULL);
	i = hashtable_iter(topicsAndPartitions);
	for (; i; i = hashtable_iter_next(topicsAndPartitions, i)) {
		const char *topic = hashtable_iter_key(i);
		hashtable_t *partitions = hashtable_iter_value(i);
		j = hashtable_iter(partitions);
		for (; j; j = hashtable_iter_next(partitions, j)) {
			int32_t pid = *(int32_t *)hashtable_iter_key(j);
			mark_failure(failures, topic, pid);
		}
	}
	return failures;
}

static int
send_produce_request(struct kafka_producer *p, int32_t brokerId,
		hashtable_t *topics_partitions, int16_t sync, hashtable_t **failuresOut)
{
	int rc;
	int res = 0;
	hashtable_t *failures = NULL;
	int32_t correlation_id = 0;
	size_t len;
	request_header_t header;
	const char *client = "libkafka";
	KafkaBuffer *buffer = KafkaBufferNew(0);

	memset(&header, 0, sizeof header);
	header.apikey = PRODUCE;
	header.correlation_id = correlation_id++;
	len = sizeof header;
	len += 2 + strlen(client);
	len += 2 + 4 + 4; /* acks, ttl, topics */

	KafkaBufferReserve(buffer, len);
	buffer->cur = buffer->data;
	buffer->cur += request_header_pack(&header, client, buffer->cur);
	buffer->cur += uint16_pack(sync, buffer->cur);
	buffer->cur += uint32_pack(1500, buffer->cur); /*ttl*/

	header.size = serialize_topics_and_partitions(topics_partitions, buffer);
	uint32_pack(header.size-4, &buffer->data[0]);

	broker_t *broker = hashtable_get(p->brokers, &brokerId);
	if (!broker) {
		failures = mark_every_failure(topics_partitions);
		res = -1;
		goto finish;
	}

	do {
		rc = write(broker->fd, buffer->data, buffer->cur - buffer->data);
	} while (rc == -1 && errno == EINTR);

	if (rc == -1) {
		failures = mark_every_failure(topics_partitions);
		res = -1;
		goto finish;
	}

	if (sync != KAFKA_REQUEST_ASYNC) {
		int32_t rlen = 0;
		rc = read(broker->fd, &rlen, 4);
		if (rc <= 0) {
			failures = mark_every_failure(topics_partitions);
			res = -1;
			goto finish;
		}

		rlen = ntohl(rlen);

		if (rlen > 0) {
			KafkaBuffer *rbuf = KafkaBufferNew(rlen);
			rc = read(broker->fd, rbuf->data, rbuf->alloced);
			if (rc != rlen) {
				failures = mark_every_failure(topics_partitions);
				res = -1;
				KafkaBufferFree(rbuf);
				goto finish;
			}
			rbuf->len = rbuf->alloced;
			rbuf->cur = rbuf->data;

			failures = parse_produce_response(rbuf);
			KafkaBufferFree(rbuf);
		}
	}
finish:
	KafkaBufferFree(buffer);
	*failuresOut = failures;
	return res;
}

static hashtable_t *
broker_message_map(struct kafka_producer *p, struct vector *messages)
{
	/**
	 * broker_message_map = { broker: { topic: { partition: [msg set] } } }
	 */
	int i;
	hashtable_t *map;

	map = hashtable_create(int32_hash, int32_cmp, free, NULL);
	for (i = 0; i < vector_size(messages); i++) {
		partition_metadata_t *pm;
		hashtable_t *topics;
		hashtable_t *topic_partitions;
		struct vector *msgSet;

		struct kafka_message *msg = vector_at(messages, i);

		/* TODO: make use of different partitioners */
		pm = pick_random_topic_partition(p, msg);

		int32_t *leaderId = malloc(sizeof(int32_t));
		int32_t *partId = malloc(sizeof(int32_t));
		*leaderId = pm->leader->id;
		*partId = pm->partition_id;

		topics = hashtable_get(map, leaderId);
		if (!topics) {
			topics = hashtable_create(jenkins, keycmp, free, NULL);
			hashtable_set(map, leaderId, topics);
		}

		topic_partitions = hashtable_get(topics, msg->topic);
		if (!topic_partitions) {
			topic_partitions = hashtable_create(int32_hash, int32_cmp, free, NULL);
			hashtable_set(topics, strdup(msg->topic), topic_partitions);
		}

		msgSet = hashtable_get(topic_partitions, partId);
		if (!msgSet) {
			msgSet = vector_new(0, NULL);
			hashtable_set(topic_partitions, partId, msgSet);
		}
		vector_push_back(msgSet, msg);
	}
	return map;
}

static void
broker_message_map_free(hashtable_t *map)
{
	void *i, *j, *k;
	if (map) {
		i = hashtable_iter(map);
		for (; i; i = hashtable_iter_next(map, i)) {
			hashtable_t *topics = hashtable_iter_value(i);
			j = hashtable_iter(topics);
			for (; j; j = hashtable_iter_next(topics, j)) {
				hashtable_t *partitions = hashtable_iter_value(j);
				k = hashtable_iter(partitions);
				for (; k; k = hashtable_iter_next(partitions, k)) {
					struct vector *vec = hashtable_iter_value(k);
					vector_free(vec);
				}
				hashtable_destroy(partitions);
			}
			hashtable_destroy(topics);
		}
		hashtable_destroy(map);
	}
}

static int
handle_topics_partitions_failures(hashtable_t *topicsPartitions,
				hashtable_t *failedRequest, struct vector *failedMessages)
{
	int i, j;
	void *topic_iter = hashtable_iter(failedRequest);
	for (; topic_iter; topic_iter = hashtable_iter_next(failedRequest, topic_iter)) {
		char *topic = hashtable_iter_key(topic_iter);
		struct vector *pids = hashtable_iter_value(topic_iter);
		hashtable_t *partitionsForTopic = hashtable_get(topicsPartitions, topic);

		for (i = 0; i < vector_size(pids); i++) {
			struct vector *msgSet;
			int32_t pid = *(int32_t *)vector_at(pids, i);
			msgSet = hashtable_get(partitionsForTopic, &pid);
			if (msgSet) {
				for (j = 0; j < vector_size(msgSet); j++) {
					vector_push_back(failedMessages, vector_at(msgSet, j));
				}
			}
		}
	}
	return 0;
}

static int
dispatch(struct kafka_producer *p, struct vector *messages, int16_t sync, struct vector **out)
{
	void *iter;
	int i;
	struct vector *failedMessages = NULL;
	hashtable_t *map = broker_message_map(p, messages);
	int res = 0;

	for (iter = hashtable_iter(map); iter; iter = hashtable_iter_next(map, iter)) {
		int32_t brokerId = *(int32_t *)hashtable_iter_key(iter);
		hashtable_t *topicsPartitions = hashtable_iter_value(iter);
		hashtable_t *failures;
		res = send_produce_request(p, brokerId, topicsPartitions, sync, &failures);

		if (failures) {
			res = -1;
			if (!failedMessages)
				failedMessages = vector_new(0, NULL);
			handle_topics_partitions_failures(topicsPartitions, failures, failedMessages);
		}
	}

	broker_message_map_free(map);
	*out = failedMessages;
	return res;
}
