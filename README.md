MPI.NET: High-performance C# Library for Message Passing
========================================================

A forked and modified version of the code for MPI.NET found on the Subversion repository found at http://www.osl.iu.edu/research/mpi.net/, trunk@rev 338

More information about MPI.NET is available at http://www.osl.iu.edu/research/mpi.net/

MPI.NET is a high-performance, easy-to-use implementation of the
Message Passing Interface (MPI) for Microsoft's .NET environment. MPI
is the de facto standard for writing parallel programs running on a
distributed memory system, such as a compute cluster, and is widely
implemented. Most MPI implementations provide support for writing MPI
programs in C, C++, and Fortran. MPI.NET provides support for all of
the .NET languages (especially C#), and includes significant
extensions (such as automatic serialization of objects) that make it
far easier to build parallel programs that run on clusters.

# Creating a NuGet package for MPI.NET

Tested with NuGet Version: 2.7.41115.310.
From the top folder of the source code:
```
cd MPI
nuget pack MPI.csproj -IncludeReferencedProjects -Prop Configuration=Release
```
This should produce a file such as MPI.1.1.1.0.nupkg that you can use on a local nuget feed.


The rest of this document are from the README as found in the original README. 
Some installation instruction may not apply anymore.

Installation on Windows
-----------------------

To install MPI.NET on Windows, users are encouraged to download the
MPI.NET SDK, which provides a complete graphical installer for the
MPI.NET libraries and supporting documentation. 

MPI.NET on Windows is available only for use with Microsoft's MPI,
MS-MPI, which is available as part of the Microsoft Compute Cluster
Pack in Windows Compute Cluster Server and as a separate download, the
Microsoft Compute Cluster Pack SDK. Please see the MPI.NET page for
more information about installation of one of these packages before
installing.

If you have downloaded the source code for MPI.NET and wish to build
it yourself, load the top-level solution "MPI" into Visual Studio and
select "build". You will need both Visual C# and Visual C++ installed
to build all of the libraries, examples, test programs, and
benchmarks. The Visual Studio projects are for Visual Studio 2008.


Installation on Unix
--------------------

Support for MPI.NET on Unix platforms is provided based on the Mono C# 
compiler and its toolchain. Due to some bugs in Mono's implementation
of the run-time code generation facilities in System.Reflection.Emit 
and general tuning issues with the Mono JIT, MPI.NET may not perform
as well as on Microsoft's .NET implementation. On the positive side,
Mono is free and clusters running Unix-like operating systems are
widely available.

On Unix, MPI.NET uses the standard GNU toolchain (autoconf, automake,
libtool) to configure itself to the installed MPI library. MPI.NET
is known to work with MPICH2, Open MPI, and LAM/MPI on Unix-like
systems. 

If you have retrieved MPI.NET from Subversion, run "sh autogen.sh"
from the top level to generate the configure script and its supporting
scripts. Otherwise, you can skip this step.

To build MPI.NET on Unix, use:

  ./configure [optional configure options]
  make

To install MPI.NET, use:

  make install

To run MPI.NET's regression tests, use:

  make check -k

Documentation generation is currently not available within Unix. 
However, the library is the same on Windows and on Unix; please refer 
to the MPI.NET web page for tutorial and reference documentation.


Revision History
----------------

October 6, 2008 -- Version 1.0
  * First major release of MPI.NET

June 19, 2008 -- Version 0.9.0
  * Documentation improvements:
     - Provide more detailed documentation for classes and clean up presentation.
     - Complete documentation mapping from the C MPI (in the MPI.Unsafe class) to MPI.NET.
  * Bug fixes:
     - Fixed potential crash when sending serialized data via a non-blocking send.
     - Fixed incorrect source/tag values in status returned from Communicator.Receive.
     
June 6, 2008 -- Version 0.8.0
  * Documentation improvements:
     - Clearly document the need to pre-allocate arrays of value types.
       
  * Bug fixes:
     - Fixed error in testing a non-blocking serialized send request that could
       cause crashes.
    
May 28, 2008 -- Version 0.7.0 (not officially released)
  * General improvements:
     - Installers built for integration into Microsoft Visual Studio 2008
     
  * Bug fixes:
     - Fixed errors in status information returned from non-blocking communication
     - Fixed cancellation issues with non-blocking communication
     - Miscellaneous documentation fixes

February 21, 2008 -- Version 0.6.0

  * New features:
    - Topology support (see GraphCommunicator and CartesianCommunicator)
    - Attribute support (see Communicator.Attributes and the Attribute Class)
    - MPI errors are translated into C# exceptions
    - Mono support for use on Unix-like platforms
    
  * Bug fixes:
    - Fixed premature unpinning of memory in non-blocking receives
      (thanks to Lorenzo Dematté for the fix)
    - Fixed bugs when some collectives were executed with a
      communicator of size 1
    - Fixed handling of finalization of MPI classes after the MPI
      environment has been finalized
    - Fixed creation of intercommunicators
    - Fixed all-to-all collective

November 11, 2007 -- Version 0.5.0
  
  * Initial public release
