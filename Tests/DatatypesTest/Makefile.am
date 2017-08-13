# Copyright (C) 2007  The Trustees of Indiana University
#
# Use, modification and distribution is subject to the Boost Software
# License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)
#  
# Authors: Douglas Gregor
#          Andrew Lumsdaine
CSHARP_FLAGS=@CSHARP_FLAGS@
MPIEXEC=@MPIEXEC@

check_SCRIPTS = DatatypesTest.exe
EXTRA_DIST = DatatypesTest.cs
CLEANFILES = DatatypesTest.exe

DatatypesTest.exe: $(srcdir)/DatatypesTest.cs ../../MPI/MPI.dll  ../../TestCommons/TestCommons.dll 
	$(MCS) -out:$@ -target:exe $(CSHARP_FLAGS) -unsafe -lib:../../MPI -reference:MPI.dll -lib:../../TestCommons -reference:TestCommons.dll $(srcdir)/DatatypesTest.cs

all: DatatypesTest.exe

TESTS=$(check_SCRIPTS)
TESTS_ENVIRONMENT=$(SHELL) $(top_builddir)/Tests/runtest.sh $(top_builddir)