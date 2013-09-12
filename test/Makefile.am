AM_CPPFLAGS = -I${top_srcdir}/include
AM_CFLAGS = -O2

check_PROGRAMS = \
	test_metadata_request \
	produce_request \
	batch_produce_request

test_metadata_request_SOURCES = test_metadata_request.c
test_metadata_request_LDADD = \
	$(top_builddir)/src/libkafka.la \
	-lzookeeper_mt

produce_request_SOURCES = produce_request.c
produce_request_LDADD = \
	$(top_builddir)/src/libkafka.la \
	-lzookeeper_mt

batch_produce_request_SOURCES = batch_produce_request.c
batch_produce_request_LDADD = \
	$(top_builddir)/src/libkafka.la \
	-lzookeeper_mt

clean-local:
	rm -f *~
