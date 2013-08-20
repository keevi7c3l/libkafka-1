#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.60])
AC_INIT([libkafka], [0.1], [david@alwaysmovefast.com])
AC_CONFIG_SRCDIR([src/kafka.c])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([foreign 1.11 -Wall -Wno-portability silent-rules])
AC_PROG_CC_STDC
AC_USE_SYSTEM_EXTENSIONS
AM_SILENT_RULES([yes])

LT_INIT

my_CFLAGS="-Wall -O2 \
-Wmissing-declarations -Wmissing-prototypes \
-Wnested-externs -Wpointer-arith \
-Wsign-compare -Wchar-subscripts \
-Wstrict-prototypes -Wshadow \
-Wformat-security"
AC_SUBST([my_CFLAGS])

# Checks for programs.
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_MAKE_SET

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([inttypes.h limits.h locale.h stddef.h stdlib.h string.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_INT32_T
AC_TYPE_SIZE_T

# Checks for library functions.

AC_CHECK_FUNCS([clock_gettime], [have_clock_gettime=yes],
    [AC_CHECK_LIB([rt], [clock_gettime],
                  [have_clock_gettime=yes],
                  [have_clock_gettime=no])])
if test "x$have_clock_gettime" = xyes; then
   AC_DEFINE([HAVE_CLOCK_GETTIME], [1], [Define if you have the `clock_gettime` function.])
fi

AC_FUNC_STRTOD
AC_CHECK_FUNCS([localeconv memmove memset strchr strerror])

AC_CONFIG_FILES([Makefile
                 src/Makefile
                 test/Makefile
                 libkafka.pc])
AC_OUTPUT
