MPI.NET: High-performance C# Library for Message Passing
========================================================

## Overview

As of 2014-04-25, MPI.NET is available as a NuGet package for Windows and Microsoft's HPC API at https://www.nuget.org/packages/MPI.NET

This codebase is a forked and modified version of the code for MPI.NET found on the Subversion repository found at http://www.osl.iu.edu/research/mpi.net/, trunk@rev 338. The main modifications relate to targetting .NET 4.0, and a few fixes for runing on some Linux distributions. Rather than keep these modifications in-house, I think MPI.NET deserves at least a NuGet package.

## About MPI.NET

The original information about MPI.NET is available at http://www.osl.iu.edu/research/mpi.net/. Some of it is reproduced in this document, adapted to the updates made. 

MPI.NET is a high-performance, easy-to-use implementation of the Message Passing Interface (MPI) for Microsoft's .NET environment. MPI is the de facto standard for writing parallel programs running on a distributed memory system, such as a compute cluster, and is widely implemented. Most MPI implementations provide support for writing MPI programs in C, C++, and Fortran. MPI.NET provides support for all of the .NET languages (especially C#), and includes significant extensions (such as automatic serialization of objects) that make it far easier to build parallel programs that run on clusters.

## Getting started

While the material includes setup instructions for outdated versions of Visual Studio, the tutorials at http://www.osl.iu.edu/research/mpi.net/documentation/tutorial/ remain informative.

The codebase also includes applications under the Tests folder that should give you an idea of how to use MPI.NET.

## Installation on Windows

The recommended way to use MPI.NET is to use the NuGet package at https://www.nuget.org/packages/MPI.NET.

You can read through the installation page at http://www.osl.iu.edu/research/mpi.net/documentation/tutorial/installation.php, but you'll need to adapt outdated instructions/links.

MPI.NET on Windows is available only for use with Microsoft's MPI, MS-MPI, which is available as part of the Microsoft Compute Cluster Pack in Windows Compute Cluster Server and as a separate download, the Microsoft Compute Cluster Pack SDK. Please see the MPI.NET page for more information about installation of one of these packages before installing.

If you clone/download the source code, you will see a solution file at the top level. It should be readable by most recent versions of visual studio. It is working with Visual Studio 2013 Express Edition. It should be straightforward to compile, except for one thing: MPIUtils is compiling from IL directly, and requires ilasm.exe to be found. The post-build event in the project file includes batch commands that try to find the correct ilams.exe from visual studio settings, but this may still fail on your machine.

You may try to build from the command line with e.g.:

```bat
cd C:\path\to\MPI.NET\Build
build.bat Debug Rebuild
```

To run all unit tests:

```bat
cd C:\path\to\MPI.NET\Tests
.\runtests.bat Debug
```

## Installation on Unix

Support for MPI.NET on Unix platforms is provided based on the Mono C# compiler and its toolchain. This present version of MPI.NET has been used on CentOS linux clusters using Mono 3.2.3 and 3.2.8. It is likely to work with recent versions of Mono.

On Unix, MPI.NET uses the standard GNU toolchain (autoconf, automake, libtool) to configure itself to the installed MPI library. MPI.NET is known to have worked with MPICH2, Open MPI, and LAM/MPI on Unix-like systems. 

Make sure you have the right toolchain. The sample below is taken from building on a Linux CentOS installation (I think...)

```bash
module list # make sure openmpi is loaded; otherwise load the version
cd ~/src
git clone https://github.com/jmp75/MPI.NET.git mpi.net
cd ~/src/mpi.net
# This will depend on your installation, but you may need to make sure you use the GNU toolchain
module unload intel-cc
module unload intel-fc
module load gcc
```

The most recent version compilation on Linux that I tested is against OpenMPI.
```bash
LOCAL_DIR=/usr/local
sh autogen.sh
./configure --prefix=$LOCAL_DIR
make
```

You may run the tests with the following command. Note that I had these tests somehow failing, but by the look of it because of shell scripting logistics. This may not be a showstopper if you get nothing passing.
```bash
make check -k
```

Finally:
```bash
make install
```

Documentation generation is currently not available within Unix. However, the library is the same on Windows and on Unix; please refer to the MPI.NET web page for tutorial and reference documentation.


# Technical notes

## Unit tests

```bat
runtests.bat Debug
:: or with options
runtests.bat Debug AllgatherTest
```


## Creating the NuGet package for MPI.NET

Tested with NuGet Version: 2.8.1
From the top folder of the source code, after compiling the solution in Release mode

```bat
cd MPI
nuget pack MPI.csproj -IncludeReferencedProjects -Prop Configuration=Release
```

## Troubleshooting

A placeholder to log issues

```
Unhandled Exception: System.IO.FileNotFoundException: Could not load file or assembly 'MPI, Version=1.3.0.0, Culture=neutral, PublicKeyToken=null' or one of its dependencies. The system cannot find the file specified.
   at AllgatherTest.Main(String[] args)
```

Prior to Aug 2017 the test projects were not referencing MPI and MPIUtils projects, hence dependencies were missing. Running the tests would thus not work. This should not be the case anymore.
