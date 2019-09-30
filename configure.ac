dnl Copyright (C) 2007  The Trustees of Indiana University
dnl
dnl Use, modification and distribution is subject to the Boost Software
dnl License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
dnl http://www.boost.org/LICENSE_1_0.txt)
dnl  
dnl Authors: Douglas Gregor
dnl          Andrew Lumsdaine
AC_INIT(MPI.NET, 0.6.0)
AC_CONFIG_AUX_DIR(./Config)
AM_INIT_AUTOMAKE(foreign)

AC_PROG_INSTALL

dnl Libtool with shared libraries (only)
AC_LANG([C])
AC_ENABLE_SHARED
AC_DISABLE_STATIC
AC_PROG_LIBTOOL

dnl C# compiler
AC_PATH_PROG(CSC, csc, no)
AC_PATH_PROG(MCS, gmcs, no)
AC_PATH_PROG(MONO, mono, no)

dnl CIL assembler
AC_PATH_PROG(ILASM, ilasm, no)

dnl Other tools we need
AC_PATH_PROG(SED, sed, no)
AC_PATH_PROG(PERL, perl, no)

dnl Flags for the C# compiler
CSHARP_FLAGS="-optimize+"
CSHARP_DEFINES=""

CS="C#"
if test "x$CSC" = "xno" -a "x$MCS" = "xno"  ; then
	AC_MSG_ERROR([No $CS compiler found])
fi

if test "x$MCS" = "xno" ; then
	MCS=$CSC
else
  CSHARP_DEFINES="$CSHARP_DEFINES -define:BROKEN_IL_EMIT -define:BROKEN_NULL_DELEGATE"
fi

if test "x$ILASM" = "xno" ; then
        AC_MSG_ERROR([No CIL assembler found])
fi

if test "x$MONO" = "xno"; then
	AC_MSG_ERROR([No mono runtime found])
fi

dnl Detect MPI
ACX_MPI([], [AC_MSG_ERROR([No MPI implementation found])])
AC_PATH_PROG(MPIEXEC, mpiexec, no)

dnl Whether we need the C bridge to make the MPI.NET assembly work
need_cbridge="yes"

dnl Determine which MPI this is (if we can). This block of checks will 
dnl set three variables:
dnl   MPI_KIND          string describing which kind of MPI this is, e.g.,
dnl                     mpich2, openmpi, lam
dnl   MPI_COMPILEINFO   string passed to the wrapper compiler that requests
dnl                     the commands to pass on to the compiler
dnl   MPI_LINKINFO      string passed to the wrapper compiler that requests
dnl                     the commands to pass on to the linker
real_CC="$CC"
CC="$MPICC"
AC_MSG_CHECKING([which MPI is available])

dnl Check for MPICH2-based MPIs
AC_TRY_COMPILE([#include <mpi.h>
#ifndef MPICH2
#  error Not MPICH2
#endif
],[],
[AC_MSG_RESULT([MPICH2-based])
MPI_KIND="mpich2"
MPI_COMPILEINFO="-show"
MPI_LINKINFO="-show"
MPI_SHARED_LIB_NAME="mpich"
MPI_FUNC_PREFIX="PMPI"
],[])

dnl Check for Open MPI
if test "x$MPI_KIND" = "x" ; then
  AC_TRY_COMPILE([#include <mpi.h>
  #ifndef OPEN_MPI
    #  error Not OpenMPI
  #endif
],[],
[AC_MSG_RESULT([Open MPI])
MPI_KIND="openmpi"
MPI_COMPILEINFO="--showme:compile"
MPI_LINKINFO="--showme:link"
MPI_SHARED_LIB_NAME="mpi"
MPI_FUNC_PREFIX="MPI"
],[])
fi

dnl Check for LAM/MPI
if test "x$MPI_KIND" = "x" ; then
  AC_TRY_COMPILE([#include <mpi.h>
  #ifndef LAM_MPI
    #  error Not LAM/MPI
  #endif
],[],
[AC_MSG_RESULT([LAM/MPI])
MPI_KIND="lam"
MPI_COMPILEINFO="--showme"
MPI_LINKINFO="--showme"
MPI_SHARED_LIB_NAME="mpi"
MPI_FUNC_PREFIX="MPI"
],[])
fi
CC="$real_CC"

if test "x$MPI_SHARED_LIB_NAME" == "x" ; then
  MPI_SHARED_LIB_NAME="mpi"
  MPI_FUNC_PREFIX="MPI"
fi


dnl Extract the set of include directories added by the wrapper compiler
if test "x$MPI_COMPILEINFO" != "x" ; then
  if test "$MPI_KIND" = "openmpi" ; then
    dnl Open MPI makes this extremely easy with --showme:incdirs
    MPI_INCLUDEDIRS=`$MPICC --showme:incdirs`
  else
    dnl For all other MPIs, we have to parse -I lines. 
    MPI_INCLUDEDIRS=`$MPICC $MPI_COMPILEINFO|$SED -n -e "s/ -[[^I]][[^ ]]*//pg"|$SED -n -e "s/-I\([[^ ]]*\)/\1/pg"`
  fi
fi

dnl Find the actual file <mpi.h>, because we need to parse it to
dnl determine values of the various MPI constants.
if test "x$MPI_INCLUDEDIRS" != "x" ; then
  AC_MSG_CHECKING([for location of mpi.h])
  
  for dir in $MPI_INCLUDEDIRS ; do
    if test -f "$dir/mpi.h" ; then
      MPI_HEADER="$dir/mpi.h"
    fi
  done

  if test "x$MPI_HEADER" == "x" ; then
    AC_MSG_ERROR([not found!])
  else
    AC_MSG_RESULT($MPI_HEADER)
  fi
fi

if test "$MPI_KIND" = "mpich2" ; then
  dnl If it's MPICH2-based, make sure that MPICH2 was built as a shared 
  dnl library (which is not the default). The order of these
  dnl checks is *very* important, because we rely on the
  dnl fact that libpmpich depends on libmpich, and that information is
  dnl only available in the shared library version. So a static pmpich
  dnl library check will fail while the static mpich check passes; both
  dnl will pass when a shared MPICH is installed.
  dnl We do not need the C bridge at all; everything can be done from C#
  need_cbridge="no"

  real_CC="$CC"
  CC="$MPICC"
  AC_CHECK_LIB(pmpich,MPI_Init,[HAVE_LIBPMPICH=yes],[HAVE_LIBPMPICH=no],)
  AC_CHECK_LIB(mpich,PMPI_Init,[HAVE_LIBMPICH=yes],[HAVE_LIBMPICH=no],)

  if test "$HAVE_LIBPMPICH" = "no" ; then 
    if test "$HAVE_LIBMPICH" = "yes" ; then 
      AC_MSG_ERROR([ MPICH library is built statically. 
      Please install MPICH built as a shared library by configuring MPICH with 
      the --enable-sharedlibs=??? option)])
    fi
  fi
  CC="$real_CC"
elif test "$MPI_KIND" = "openmpi" ; then
  dnl With Open MPI, we need to explicitly determine which libraries
  dnl the MPI.NET C bridge will need to link against, because libtool
  dnl has the *extremely annoying* behavior that it completely ignores
  dnl the compiler wrapper when it is linking.
  ompi_libdirs=`$MPICC --showme:libdirs`
  ompi_libs=`$MPICC --showme:libs`
  for dir in $ompi_libdirs ; do
    MPILIBS="$MPILIBS -L$dir"
  done
  for lib in $ompi_libs ; do
    MPILIBS="$MPILIBS -l$lib"
  done
fi

dnl If we need a C bridge, it's because MPI handles are actually
dnl pointers
if test "$need_cbridge" = "yes" ; then
  CSHARP_DEFINES="$CSHARP_DEFINES -define:MPI_HANDLES_ARE_POINTERS"
fi

dnl Conditionals
AM_CONDITIONAL(MPINET_CBRIDGE, test "$need_cbridge" = "yes")

dnl Get size of int and size_t for Unsafe.pl
AC_CHECK_SIZEOF([int])
AC_CHECK_SIZEOF([long])
AC_CHECK_SIZEOF([long long])
AC_CHECK_SIZEOF([size_t])

dnl Variable substitutions
AC_SUBST(MCS)
AC_SUBST(ILASM)
AC_SUBST(MONO)
AC_SUBST(MPIEXEC)
AC_SUBST(PERL)
AC_SUBST(CSHARP_FLAGS)
AC_SUBST(CSHARP_DEFINES)
AC_SUBST(MPI_HEADER)
AC_SUBST(MPI_SHARED_LIB_NAME)
AC_SUBST(MPI_FUNC_PREFIX)

AC_CONFIG_FILES([MPI/MPI.dll.config Tests/runtest.sh],[],[])

AC_OUTPUT([
Makefile
MPI/Makefile
TestCommons/Makefile
Examples/Makefile
Examples/Hello/Makefile
Examples/Hostnames/Makefile
Examples/Pi/Makefile
Examples/PingPong/Makefile
Examples/Ring/Makefile
Benchmarks/Makefile
Benchmarks/Netpipe_cs/Makefile
Benchmarks/Netcoll_cs/Makefile
Tests/Makefile
Tests/AllgatherTest/Makefile
Tests/AllreduceTest/Makefile
Tests/AlltoallTest/Makefile
Tests/AttributesTest/Makefile
Tests/BroadcastTest/Makefile
Tests/CartTest/Makefile
Tests/DatatypesTest/Makefile
Tests/ExceptionTest/Makefile
Tests/ExclusiveScanTest/Makefile
Tests/GatherTest/Makefile
Tests/GraphTest/Makefile
Tests/IntercommunicatorTest/Makefile
Tests/NonblockingTest/Makefile
Tests/ReduceScatterTest/Makefile
Tests/ReduceTest/Makefile
Tests/ScanTest/Makefile
Tests/ScatterTest/Makefile
Tests/SendReceiveTest/Makefile
Tests/UnitTestOutcomeTest/Makefile
])
# TODO Tests/SpawnTest/Makefile
