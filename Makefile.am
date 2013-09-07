SUBDIRS = src test
EXTRA_DIST =
CLEANFILES =
AM_MAKEFLAGS = --no-print-directory
EXTRA_DIST += src/libkafka.sym

pkginclude_HEADERS = include/kafka.h
pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libkafka.pc
EXTRA_DIST += libkafka.pc.in
CLEANFILES += libkafka.pc

TESTS = \
	test/produce_request \
	test/batch_produce_request

clean-local:
	rm -f *~
