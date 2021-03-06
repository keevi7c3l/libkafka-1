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

#include <kafka.h>
#include "kafka-private.h"

typedef struct {
	char *status;
} kafka_status_t;

KAFKA_EXPORT const char *
kafka_status_string(int status)
{
	static kafka_status_t statuses[] = {
		{"Ok"},
		{"Offset out of range"},
		{"Invalid message"},
		{"Unknown topic or partition"},
		{"Invalid message size"},
		{"Leader not available"},
		{"Not leader for partition"},
		{"Request timed out"},
		{"Broker not available"},
		{"Replica not available"},
		{"Message size too large"},
		{"Stale controller epoch code"},
		{"Offset metadata too large code"},
		{"Producer Error"},
		{"Zookeeper Init Error"},
		{"Broker Init Error"},
		{"Topics Init Error"},
		{"Topics Partitions Init Error"}
	};

	if (status >= sizeof(statuses) / sizeof(kafka_status_t) ||
		status == KAFKA_UNKNOWN) {
		return "Kafka Unknown";
	}
	return statuses[status].status;
}
