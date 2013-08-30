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
#include "kafka-private.h"

KafkaBuffer *
KafkaBufferNew(size_t size)
{
	KafkaBuffer *buffer;
	buffer = calloc(1, sizeof *buffer);
	if (size > 0)
		buffer->alloced = size;
	else
		buffer->alloced = 1024;
	buffer->data = calloc(buffer->alloced, 1);
	return buffer;
}

size_t
KafkaBufferReserve(KafkaBuffer *buffer, size_t size)
{
	/**
	 * Make sure there's enough room in the buffer.
	 */
	if (size >= buffer->alloced - buffer->len) {
		do {
			KafkaBufferResize(buffer);
		} while (size >= buffer->alloced - buffer->len);
	}
	return buffer->alloced;
}

size_t
KafkaBufferResize(KafkaBuffer *buffer)
{
	size_t sz;
	uint8_t *ptr;
	sz = buffer->alloced * 2;
	ptr = realloc(buffer->data, sz);
	assert(ptr);
	buffer->data = ptr;
	memset(&buffer->data[buffer->alloced], 0, buffer->alloced);
	buffer->alloced = sz;
	return sz;
}
