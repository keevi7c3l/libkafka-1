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

#include "vector.h"
#include "kafka-private.h"

struct vector {
	unsigned magic;
#define VECTOR_MAGIC 0xc6e74bfcU
	void **array;
	unsigned length;
	unsigned next;
	vector_free_fn free_fn;
};

struct vector *
vector_new(unsigned size, vector_free_fn free_fn)
{
	struct vector *v;
	ALLOC_OBJ(v, VECTOR_MAGIC);
	if (!v)
		return NULL;
	v->free_fn = free_fn;
	if (size) {
		v->array = calloc(size, sizeof *v->array);
		v->length = size;
	} else {
		v->length = 64;
		v->array = calloc(v->length, sizeof *v->array);
	}
	assert(v->array != NULL);
	return v;
}

void
vector_free(struct vector *v)
{
	unsigned u;
	if (!v)
		return;
	CHECK_OBJ(v, VECTOR_MAGIC);
	for (u = 0; u < v->next; u++) {
		if (v->free_fn)
			v->free_fn(v->array[u]);
		v->array[u] = NULL;
	}
	free(v->array);
	v->array = NULL;
	FREE_OBJ(v);
}

void
vector_push_back(struct vector *v, void *ptr)
{
	unsigned u;
	CHECK_OBJ_NOTNULL(v, VECTOR_MAGIC);
	if (v->next >= v->length) {
		u = v->length * 2;
		v->array = realloc(v->array, u * sizeof *v->array);
		assert(v->array != NULL);
		while (v->length < u)
			v->array[v->length++] = NULL;
	}
	v->array[v->next++] = ptr;
}

void *
vector_pop_back(struct vector *v)
{
	void *ptr;
	CHECK_OBJ_NOTNULL(v, VECTOR_MAGIC);
	if (!v->next)
		return NULL;
	ptr = v->array[--v->next];
	v->array[v->next] = NULL;
	return ptr;
}

unsigned
vector_size(struct vector *v)
{
	CHECK_OBJ_NOTNULL(v, VECTOR_MAGIC);
	return v->next;
}

void *
vector_front(struct vector *v)
{
	CHECK_OBJ_NOTNULL(v, VECTOR_MAGIC);
	return v->array[0];
}

void *
vector_back(struct vector *v)
{
	CHECK_OBJ_NOTNULL(v, VECTOR_MAGIC);
	return v->array[v->next-1];
}

void *
vector_at(struct vector *v, unsigned u)
{
	void *ptr = NULL;
	CHECK_OBJ_NOTNULL(v, VECTOR_MAGIC);
	if (u < v->next)
		ptr = v->array[u];
	return ptr;
}

int
vector_empty(struct vector *v)
{
	CHECK_OBJ_NOTNULL(v, VECTOR_MAGIC);
	return v->next == 0;
}

void
vector_erase(struct vector *v, unsigned u)
{
	CHECK_OBJ_NOTNULL(v, VECTOR_MAGIC);
	if (u < v->next) {
		free(v->array[u]);
		v->array[u] = NULL;
		if (u < v->next-1)
			memmove(&v->array[u], &v->array[u+1], (v->next - u+1) * sizeof(void *));
		v->next--;
	}
}
