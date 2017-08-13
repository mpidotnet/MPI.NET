# Copyright (C) 2007  The Trustees of Indiana University
#
# Use, modification and distribution is subject to the Boost Software
# License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)
#  
# Authors: Douglas Gregor
#          Andrew Lumsdaine
CSHARP_FLAGS=@CSHARP_FLAGS@ @CSHARP_DEFINES@
PERL=@PERL@
MPI_HEADER=@MPI_HEADER@

# C bridge support
if MPINET_CBRIDGE
lib_LTLIBRARIES = libmpinet.la
endif

SOURCES=$(srcdir)/Attribute.cs			\
	$(srcdir)/CartesianCommunicator.cs	\
	$(srcdir)/Communicator.cs		\
	$(srcdir)/Comparison.cs			\
	$(srcdir)/DatatypeCache.cs		\
	$(srcdir)/Environment.cs		\
	$(srcdir)/Exceptions.cs			\
	$(srcdir)/GraphCommunicator.cs		\
	$(srcdir)/Group.cs			\
	$(srcdir)/Intercommunicator.cs		\
	$(srcdir)/Intracommunicator.cs		\
	$(srcdir)/Operation.cs			\
	$(srcdir)/Request.cs			\
	$(srcdir)/RequestList.cs		\
	$(srcdir)/Status.cs			\
	$(srcdir)/TagAllocator.cs		\
	$(srcdir)/TopologicalCommunicator.cs	\
	$(srcdir)/UnmanagedMemoryStream.cs

# We want to install directly into /lib, but we can't write lib_SCRIPTS
pkglibexecdir = $(libdir)
pkglibexec_SCRIPTS = MPI.dll MPI.dll.config
EXTRA_DIST = $(SOURCES) Unsafe.cs Unsafe.pl MPI.dll.config.in
CLEANFILES = MPI.dll CustomUnsafe.cs cbridge.c

MPI.dll: $(SOURCES) CustomUnsafe.cs ../MPIUtils/MPIUtils.dll 
	$(MCS) -out:$@ -unsafe -target:library -lib:../MPIUtils -reference:MPIUtils.dll $(CSHARP_FLAGS) $(SOURCES) CustomUnsafe.cs 

CustomUnsafe.cs cbridge.c: $(srcdir)/Unsafe.cs $(srcdir)/Unsafe.pl $(MPI_HEADER)
	$(PERL) $(srcdir)/Unsafe.pl $(MPI_HEADER) $(srcdir)/Unsafe.cs CustomUnsafe.cs cbridge.c @DEFS@
# Note: We need SIZEOF_INT, etc. that are set in configure.
#
# Would be nice if we had a cleaner way of passing these values
# through than to pass ALL the definitions, but this works for now


# Use the MPI C compiler and libraries instead of the normal C
# compiler and libraries.
CC=@MPICC@
LIBS=@MPILIBS@ @LIBS@

# Force libtool to use the C tag, even though we're using the MPI C
# compiler.
LIBTOOL = @LIBTOOL@ --tag=CC

libmpinet_la_SOURCES = cbridge.c
libmpinet_la_LDFLAGS = -version-info 0:6:0

all: MPI.dll
