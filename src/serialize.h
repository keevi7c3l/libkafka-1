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

#ifndef _LIBKAFKA_SERIALIZE_H_
#define _LIBKAFKA_SERIALIZE_H_

#include <stdint.h>
#include "kafka-private.h"

inline size_t uint8_pack(uint8_t value, uint8_t *ptr);
inline size_t uint16_pack(uint16_t value, uint8_t *ptr);
inline size_t uint32_pack(uint32_t value, uint8_t *ptr);
inline size_t uint64_pack(uint64_t value, uint8_t *ptr);
inline size_t string_pack(const char *str, uint8_t *ptr);
inline size_t bytestring_pack(bytestring_t *str, uint8_t *ptr);

int32_t kafka_message_serialize(struct kafka_message *m, uint8_t **out);
size_t request_message_header_pack(request_message_header_t *header,
				const char *client, uint8_t **out);
int produce_request_serialize(produce_request_t *req, struct iovec **out);

#endif
