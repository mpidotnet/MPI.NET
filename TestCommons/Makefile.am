# Copyright (C) 2007  The Trustees of Indiana University
#
# Use, modification and distribution is subject to the Boost Software
# License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)
#  
# Authors: Douglas Gregor
#          Andrew Lumsdaine
CSHARP_FLAGS=@CSHARP_FLAGS@

EXTRA_DIST = MPIDebug.cs
CLEANFILES = TestCommons.dll

TestCommons.dll: $(srcdir)/MPIDebug.cs ../MPI/MPI.dll
	$(MCS) -out:$@ -target:library $(CSHARP_FLAGS) -lib:../MPI -reference:MPI.dll $(srcdir)/MPIDebug.cs

all: TestCommons.dll
