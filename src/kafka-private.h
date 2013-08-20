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

#ifndef _LIBKAFKA_PRIVATE_H_
#define _LIBKAFKA_PRIVATE_H_

#include <zookeeper/zookeeper.h>
#include "jansson/jansson.h"

#define KAFKA_EXPORT __attribute__((visibility("default")))

/* broker.c */
json_t *broker_map_new(zhandle_t *zh, struct String_vector *v);
json_t *topic_map_new(zhandle_t *zh, struct String_vector *v);

/* utils.c */
void free_String_vector(struct String_vector *v);
char *string_builder(const char *fmt, ...);

/**
 * OBJ stuff taken from miniobj.h in Varnish. Written by PHK.
 */

#define ALLOC_OBJ(ptr, type_magic)               \
    do {                                         \
        (ptr) = calloc(1, sizeof *(ptr));        \
        if ((ptr) != NULL)                       \
            (ptr)->magic = (type_magic);         \
    } while (0)

#define FREE_OBJ(ptr)                           \
    do {                                        \
        (ptr)->magic = (0);                     \
        free(ptr);                              \
    } while (0)

#define CHECK_OBJ(ptr, type_magic)              \
    do {                                        \
        assert((ptr)->magic == type_magic);     \
    } while (0)

#define CHECK_OBJ_NOTNULL(ptr, type_magic)      \
    do {                                        \
        assert((ptr) != NULL);                  \
        assert((ptr)->magic == type_magic);     \
    } while (0)

#define CAST_OBJ(to, from, type_magic)         \
    do {                                       \
        (to) = (from);                         \
        if ((to) != NULL)                      \
            CHECK_OBJ((to), (type_magic));     \
    } while (0)

#define CAST_OBJ_NOTNULL(to, from, type_magic)  \
    do {                                        \
        (to) = (from);                          \
        CHECK_OBJ_NOTNULL((to), (type_magic));  \
    } while (0)

#endif
