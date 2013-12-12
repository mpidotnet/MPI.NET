#!/bin/sh
# Copyright 2005 The Trustees of Indiana University.

# Use, modification and distribution is subject to the Boost Software
# License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)

#  Authors: Douglas Gregor
#           Andrew Lumsdaine

echo "--> Running aclocal"
aclocal
if test "$?" != "0" ; then
    echo "*** aclocal failed"
    exit 1
fi

if test "`grep AC_CONFIG_HEADER configure.ac`" != "" -o \
        "`grep AM_CONFIG_HEADER configure.ac`" != "" ; then
    # if we use CONFIG_HEADER, then make sure to run autoheader
    echo "--> Running autoheader"
    autoheader
    if test "$?" != "0" ; then
        echo "*** autoheader failed"
        exit 1
    fi
else
    echo "--> autoheader not needed"
fi

echo "--> Running autoconf"
autoconf
if test "$?" != "0" ; then
    echo "*** autoconf failed"
    exit 1
fi

echo "--> Running libtoolize"
if [ `uname` = "Darwin" ]; then
  glibtoolize --automake --copy
else
  libtoolize --automake --copy
fi

if test "$?" != "0" ; then
    echo "*** libtoolize failed"
    exit 1
fi

echo "--> Running automake"
automake -a --copy --include-deps
if test "$?" != "0" ; then
    echo "*** automake failed"
    exit 1
fi
