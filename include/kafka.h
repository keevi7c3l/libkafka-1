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

#ifndef _LIBKAFKA_H_
#define _LIBKAFKA_H_

#include <stdint.h>

#define KAFKA_OK                           0
#define KAFKA_UNKNOWN                      1
#define KAFKA_PRODUCER_ERROR               2
#define KAFKA_ZOOKEEPER_INIT_ERROR         3
#define KAFKA_BROKER_INIT_ERROR            4
#define KAFKA_TOPICS_INIT_ERROR            5
#define KAFKA_TOPICS_PARTITIONS_INIT_ERROR 6
#define KAFKA_METADATA_ERROR               7

#define KAFKA_REQUEST_ASYNC      0
#define KAFKA_REQUEST_SYNC       1
#define KAFKA_REQUEST_FULL_SYNC -1

struct kafka_producer;
struct kafka_message;

/* kafka.c */
const char *kafka_status_string(int status);

/* producer/producer.c */
struct kafka_producer *kafka_producer_new(const char *zkServer);
void kafka_producer_free(struct kafka_producer *p);
int kafka_producer_send(struct kafka_producer *p, struct kafka_message *msg,
			int16_t sync);
int kafka_producer_status(struct kafka_producer *p);

/* message.c */
struct kafka_message *kafka_message_new(const char *topic, const char *key,
					const char *value);
void kafka_message_free(struct kafka_message *msg);

#endif
