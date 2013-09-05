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
#include <assert.h>
#include "../kafka-private.h"

static hashtable_t *
map_partition_replicas(KafkaBuffer *buffer, hashtable_t *brokers)
{
	int32_t i, num_replicas;
	hashtable_t *r = hashtable_create(int32_hash, int32_cmp, free, NULL);
	buffer->cur += uint32_unpack(buffer->cur, &num_replicas);
	assert(buffer->cur < &buffer->data[buffer->len]);
	for (i = 0; i < num_replicas; i++) {
		int32_t *replica_id = calloc(1, sizeof(int32_t));
		buffer->cur += uint32_unpack(buffer->cur, replica_id);
		assert(buffer->cur < &buffer->data[buffer->len]);
		hashtable_set(r, replica_id, hashtable_get(brokers, replica_id));
	}
	return r;
}

static hashtable_t *
map_partition_isr(KafkaBuffer *buffer, hashtable_t *brokers)
{
	int32_t i, num_isr;
	hashtable_t *isr = hashtable_create(int32_hash, int32_cmp, free, NULL);
	buffer->cur += uint32_unpack(buffer->cur, &num_isr);
	assert(buffer->cur < &buffer->data[buffer->len]);
	for (i = 0; i < num_isr; i++) {
		int32_t *isr_id = calloc(1, sizeof(int32_t));
		buffer->cur += uint32_unpack(buffer->cur, isr_id);
		hashtable_set(isr, isr_id, hashtable_get(brokers, isr_id));
	}
	return isr;
}

partition_metadata_t *
partition_metadata_new(int32_t partition_id, broker_t *leader,
		hashtable_t *replicas, hashtable_t *isr, int16_t error)
{
	partition_metadata_t *p;
	p = calloc(1, sizeof *p);
	p->partition_id = partition_id;
	p->leader = leader;
	p->replicas = replicas;
	p->isr = isr;
	p->error = error;
	return p;
}

partition_metadata_t *
partition_metadata_from_buffer(KafkaBuffer *buffer, hashtable_t *brokers)
{
	int32_t i;
	int16_t err_code;
	int32_t partition_id;
	int32_t leader_id;
	broker_t *leader;
	hashtable_t *replicas, *isr;

	buffer->cur += uint16_unpack(buffer->cur, &err_code);
	buffer->cur += uint32_unpack(buffer->cur, &partition_id);
	buffer->cur += uint32_unpack(buffer->cur, &leader_id);

	leader = hashtable_get(brokers, &leader_id);
	replicas = map_partition_replicas(buffer, brokers);
	isr = map_partition_isr(buffer, brokers);

	return partition_metadata_new(partition_id, leader, replicas, isr,
				err_code);
}
