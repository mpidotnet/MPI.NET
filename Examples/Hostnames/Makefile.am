# Copyright (C) 2007  The Trustees of Indiana University
#
# Use, modification and distribution is subject to the Boost Software
# License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)
#  
# Authors: Douglas Gregor
#          Andrew Lumsdaine
CSHARP_FLAGS=@CSHARP_FLAGS@

noinst_SCRIPTS = Hostnames.exe
EXTRA_DIST = Hostnames.cs
CLEANFILES = Hostnames.exe

Hostnames.exe: $(srcdir)/Hostnames.cs ../../MPI/MPI.dll
	$(MCS) -out:$@ -target:exe $(CSHARP_FLAGS) -lib:../../MPI -reference:MPI $(srcdir)/Hostnames.cs

TESTS=$(noinst_SCRIPTS)
TESTS_ENVIRONMENT=$(SHELL) $(top_builddir)/Tests/runtest.sh $(top_builddir)
