# Copyright (C) 2007  The Trustees of Indiana University
#
# Use, modification and distribution is subject to the Boost Software
# License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)
#  
# Authors: Douglas Gregor
#          Andrew Lumsdaine
SOURCES=Memory.il

# We want to install directly into /lib, but we can't write lib_SCRIPTS
pkglibexecdir = $(libdir)
pkglibexec_SCRIPTS = MPIUtils.dll
EXTRA_DIST = $(SOURCES) MPIUtils.snk
CLEANFILES = MPIUtils.dll

MPIUtils.dll: $(SOURCES) $(srcdir)/MPIUtils.snk
	$(ILASM) /dll /output:$@ /key:$(srcdir)/MPIUtils.snk $<
