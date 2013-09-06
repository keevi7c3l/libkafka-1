ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}

AM_CPPFLAGS = \
	-include $(top_builddir)/config.h \
	-I$(top_srcdir)/include \
	-DSYSCONFDIR=\""$(sysconfdir)"\" \
	-DLIBEXECDIR=\""$(libexecdir)"\"

AM_CFLAGS =
	${my_CFLAGS} \
	-fvisibility=hidden \
	-ffunction-sections \
	-fdata-sections

LIBKAFKA_CURRENT=1
LIBKAFKA_REVISION=0
LIBKAFKA_AGE=0

lib_LTLIBRARIES = libkafka.la

libkafka_la_SOURCES = \
	kafka-private.h \
	kafka.c \
	broker.c \
	utils.c \
	crc32.c \
	message.c \
	serialize.c \
	buffer.c \
	metadata/partition_metadata.c \
	metadata/metadata_request.c \
	producer/producer.c \
	producer/watchers.c \
	vector.c \
	jansson/dump.c \
	jansson/error.c \
	jansson/hashtable.c \
	jansson/load.c \
	jansson/memory.c \
	jansson/pack_unpack.c \
	jansson/strbuffer.c \
	jansson/strconv.c \
	jansson/utf.c \
	jansson/value.c

libkafka_la_LDFLAGS = \
	$(AM_LDFLAGS) \
	-version-info $(LIBKAFKA_CURRENT):$(LIBKAFKA_REVISION):$(LIBKAFKA_AGE)

libkafka_la_DEPENDENCIES = ${top_srcdir}/src/libkafka.sym

clean-local:
	rm -f *~
