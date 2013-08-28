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

static int kp_init_brokers(struct kafka_producer *p);
static int kp_init_topics(struct kafka_producer *p);
static int kp_init_topics_partitions(struct kafka_producer *p);
static json_t *kp_broker_by_id(struct kafka_producer *p, int id);

KAFKA_EXPORT struct kafka_producer *
kafka_producer_new(const char *zkServer)
{
	int rc;
	struct kafka_producer *p;

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

	if (kp_init_brokers(p) == -1) {
		p->res = KAFKA_BROKER_INIT_ERROR;
		goto finish;
	}

	if (kp_init_topics(p) == -1) {
		p->res = KAFKA_TOPICS_INIT_ERROR;
		goto finish;
	}

	if (kp_init_topics_partitions(p) == -1) {
		p->res = KAFKA_TOPICS_PARTITIONS_INIT_ERROR;
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

static int32_t
count_kafka_partitions(json_t *partitions)
{
	void *iter;
	int parts = 0;
	if (!partitions)
		return -1;
	iter = json_object_iter(partitions);
	for (; iter; iter = json_object_iter_next(partitions, iter)) {
		parts++;
	}
	return parts;
}

static int32_t
count_keys(hashtable_t *p)
{
	int32_t k = 0;
	void *iter = hashtable_iter(p);
	for (; iter; iter = hashtable_iter_next(p, iter)) {
		k++;
	}
	return k;
}

static int32_t
pick_random_partition(struct kafka_producer *p, struct kafka_message *msg)
{
	int32_t n;
	json_t *topic, *partitions;
	topic = json_object_get(p->topics, msg->topic);
	if (!topic)
		return -1;
	partitions = json_object_get(topic, "partitions");
	if (!partitions)
		return -1;
	n = count_kafka_partitions(partitions);
	if (n <= 0)
		return -1;
	return rand() % n;
}

static int
send_request(struct kafka_producer *p, json_t *broker, produce_request_t *req)
{
	int i, j, vlen;
	struct iovec *iov;
	void *u, *v;
	size_t len;
	uint8_t *buf, *ptr;
	request_message_header_t header;
	memset(&header, 0, sizeof header);

	vlen = 16;
	iov = calloc(vlen, sizeof *iov);
	i = 1; /* v[0] is reserved for request header */

	/* header.size gets updated at the end */
	header.size = 2 + 2 + 4; /* apikey + apiversion + correlation_id */
	header.apikey = PRODUCE;
	header.apiversion = 0;
	header.correlation_id = 1;

	const char *client = "foo";

	len = sizeof header;
	len += 2 + strlen(client);
	len += 2 + 4 + 4; /* acks, ttl, topics */

	buf = calloc(len, 1);
	ptr = buf;

	ptr += uint16_pack(req->acks, ptr);
	ptr += uint32_pack(req->ttl, ptr);
	ptr += uint32_pack(count_keys(req->topics_partitions), ptr);
	iov[i].iov_base = buf;
	iov[i].iov_len = ptr - buf;
	i++;

	u = hashtable_iter(req->topics_partitions);
	for (; u; u = hashtable_iter_next(req->topics_partitions, u)) {
		size_t topicLen;
		uint8_t *topicBuf;
		topic_partitions_t *topic;
		topic = hashtable_iter_value(u);

		/* prefix, str, sizeof num partitions */
		topicLen = 2 + strlen(topic->topic) + 4;
		topicBuf = calloc(topicLen, 1);

		ptr = topicBuf;
		ptr += string_pack(topic->topic, ptr);
		/* write number of partitions in this topic */
		ptr += uint32_pack(count_keys(topic->partitions), ptr);

		iov[i].iov_base = topicBuf;
		iov[i].iov_len = ptr - topicBuf;
		i++;

		v = hashtable_iter(topic->partitions);
		for (; v; v = hashtable_iter_next(topic->partitions, v)) {
			unsigned w;
			int32_t msg_set_size = 0;
			size_t partLen, msgLen;
			uint8_t *partBuf, *msgBuf;
			partition_messages_t *partition = hashtable_iter_value(v);
			partLen = 4 + 4; /* partition, message set size */
			j = i++; /* maintain pointer to this header */

			/* each message in the partition */
			for (w = 0; w < vector_size(partition->messages); w++) {
				struct kafka_message *msg;
				msg = vector_at(partition->messages, w);
				iov[i].iov_len = kafka_message_serialize(msg, &iov[i].iov_base);
				msg_set_size += iov[i].iov_len;
				i++;
			}

			iov[j].iov_base = calloc(partLen, 1);
			void *pp = iov[j].iov_base;
			pp += uint32_pack(partition->partition, pp);
			pp += uint32_pack(msg_set_size, pp);
			iov[j].iov_len = pp - iov[j].iov_base;
		}
	}

	unsigned uu;
	void *pp;
	for (j = 1; j < i; j++) {
		header.size += iov[j].iov_len;
	}
	header.size += 2 + strlen(client);

	iov[0].iov_len = request_message_header_pack(&header, client,
						&iov[0].iov_base);

	/* TODO: do actual sending in kafka_producer_send() */
	int fd;
	fd = json_integer_value(json_object_get(broker, "fd"));
	printf("sending to broker: %s:%" JSON_INTEGER_FORMAT "\n",
		json_string_value(json_object_get(broker, "host")),
		json_integer_value(json_object_get(broker, "port")));

	for (j = 0; j < i; j++) {
		int k;
		ptr = iov[j].iov_base;
		for (k = 0; k < iov[j].iov_len; k++, ptr++)
			printf("0x%02X ", *ptr);
	}
	printf("\n");

	assert(writev(fd, iov, i) == header.size + 4);

	for (j = 0; j < i; j++)
		free(iov[j].iov_base);
	free(iov);


	char rbuf[1024];
	size_t bufsize;
	bufsize = read(fd, rbuf, sizeof rbuf);
	printf("received:\n");
	print_bytes(rbuf, bufsize);
}

KAFKA_EXPORT int
kafka_producer_send(struct kafka_producer *p, struct kafka_message *msg)
{
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

	char partStr[33];
	memset(partStr, 0, sizeof partStr);
	int32_t topic_partition = pick_random_partition(p, msg);
	snprintf(partStr, sizeof partStr, "%d", topic_partition);
	json_t *t = json_object_get(p->topicsPartitions, msg->topic);
	if (!t)
		return -1;
	json_t *partition = json_object_get(t, partStr);
	int brokerId = json_integer_value(json_object_get(partition, "leader"));
	printf("BrokerId: %d\n", brokerId);
	json_t *broker = kp_broker_by_id(p, brokerId);
	if (!broker)
		return -1;
	req = produce_request_new();

	topic = calloc(1, sizeof *topic);
	topic->topic = msg->topic;
	topic->partitions = hashtable_create(jenkins, keycmp, free, NULL);

	part = calloc(1, sizeof *part);
	part->messages = vector_new(1, NULL);
	part->partition = topic_partition;
	printf("partition: %d\n", topic_partition);

	vector_push_back(part->messages, msg);

	hashtable_set(topic->partitions, strdup(partStr), part);
	hashtable_set(req->topics_partitions, strdup(msg->topic), topic);

	send_request(p, broker, req);
	produce_request_free(req);
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
