#!/bin/sh

rm -rf autom4te.cache

autoreconf --install --force --verbose -I m4
if [ $? -ne 0 ]; then
    echo "Warning: autoreconf failed"
    echo "Attempting to run the preparation steps individually\n"

    libtoolize -c -f
    aclocal -I m4
    autoconf -f
    autoheader
    automake -a -c -f
fi

echo "done\n"

echo "The build system is now prepared. To build, run:"
echo "    ./configure"
echo "    make"
