#!/bin/sh

topsrcdir="`dirname $0`"
cd "$topsrcdir"

make clean
make distclean
rm -rf \
aclocal.m4 \
autom4te.cache \
autoscan.log \
config.guess \
config.h \
config.h.in \
config.log \
config.sub \
configure \
configure.scan \
depcomp \
.deps \
install-sh \
.libs \
libtool \
ltmain.sh \
missing \
m4/*.m4

find . -type f -name 'Makefile' | xargs rm -f
find . -type f -name 'Makefile.in' | xargs rm -f
