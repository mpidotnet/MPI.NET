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
:: or with options
.\runtests.bat Debug AllgatherTest
```

2017-08: I noticed some unit tests overall passing yet some debug assertions were false. There is now an option to abort tests early if something is amiss:

```bat
set ThrowMPIDebugException=True
set MPIDebugSysDiagnostic=
cd C:\path\to\MPI.NET\Tests
runtests.bat Debug > log.txt 2>&1
:: or with options
.\runtests.bat Debug AllgatherTest > log.txt 2>&1
```

If instead you want to see the standard behavior of `System.Diagnostic.Debug` use instead:

```bat
set ThrowMPIDebugException=
set MPIDebugSysDiagnostic=True
cd C:\path\to\MPI.NET\Tests
.\runtests.bat Debug
:: or with options
.\runtests.bat Debug AllgatherTest
```

## Installation on Unix

Support for MPI.NET on Unix platforms is provided based on the Mono C# compiler and its toolchain. The .NET landscape on Linux is fast changing, both with mono increasingly using open source .NET code from Microsoft and the emergence of .NET Core. "I" am not accross all the technologies

### Environments tested

The most recent tests by the maintainer on 2017-08 were on a Debian desktop:

```txt
uname -a
# Linux xxxxxxxx-bu 4.11.0-2-amd64 #1 SMP Debian 4.11.11-1 (2017-07-22) x86_64 GNU/Linux
mono --version
# Mono JIT compiler version 5.4.0.135 (tarball Thu Aug  3 19:11:58 UTC 2017)
csc -version
# 2.3.1.61919 (57c81319)
mcs --version
# Mono C# compiler version 5.4.0.135
##  openMPI 2.2.1
```

and on a Linux cluster

```txt
mono/4.4.2.11
openmpi/1.8.8-melanox-gcc
gcc/4.9.3
```


### Building

On Unix, MPI.NET uses the standard GNU toolchain (autoconf, automake, libtool) to configure itself to the installed MPI library. MPI.NET is known to have worked with MPICH2, Open MPI, and LAM/MPI on Unix-like systems. 

```bash
cd ~/src
git clone https://github.com/jmp75/MPI.NET.git mpi.net
cd ~/src/MPI.NET
```

Make sure you have the right toolchain. On a typical Linux cluster ou may need to do something like follows. You may not need to do that if you are running on a Linux desktop.

```bash
module list # make sure openmpi is loaded; otherwise load the version
# This will depend on your installation, but you may need to make sure you use the GNU toolchain
module unload intel-cc
module unload intel-fc
module load gcc
module avail openmpi
```

Depending on your system you may have a choice of openmpi versions. You probably want to use gcc compiled ones if available, e.g.:

```bash
module load openmpi/1.8.8-mellanox-gcc
```

For information on one linux cluster test system `module list`sh includes:

```txt
mono/4.4.2.11
openmpi/1.8.8-melanox-gcc
gcc/4.9.3
```

You may want to check that the openmpi headers are in the INCLUDE environment variable - `module load` may not have done it...

```bash
echo $INCLUDE
```

as I did notice the need sometimes to add e.g.

```bash
export INCLUDE=$INCLUDE:/apps/openmpi/1.8.8-mellanox-gcc/include
```

otherwhise `./configure` would fail at `checking for MPI_Init...`.

```bash
LOCAL_DIR=/usr/local # or where you can install if you cannot 'sudo make install'
LOCAL_DIR=/home/per202/local # or where you can install if you cannot 'sudo make install'
sh autogen.sh
./configure --prefix=$LOCAL_DIR
make
```

Unfortunately the environment of a Linux box is less uniform than for Windows, and you may well have issues compiling in your first try. See section Troubleshooting build on Linux for hints.

### Testing

```bash
cd path/to/MPI.NET
chmod +x ./Tests/runtest.sh
mkdir -p tmp
./Tests/runtests.sh > tmp/log.txt 2>&1 
```

To run one test at a time:

```bash
cd path/to/MPI.NET/Tests
./runtest.sh ../ BroadcastTest/BroadcastTest.exe
```

If you want to only run for a level of parallelism of 4 rather than a serie sof cases:

```bash
cd path/to/MPI.NET/Tests
./runtest.sh ../ BroadcastTest/BroadcastTest.exe "4"
```

TODO: Ideally would be accessible via the following but need to beat automake files into shape for it:

```bash
make check -k
```

### Deploy

Finally:
```bash
make install
# or 
sudo make install
```

Documentation generation is currently not available within Unix. However, the library is the same on Windows and on Unix; please refer to the MPI.NET web page for tutorial and reference documentation.

# Technical notes

## Creating the NuGet package for MPI.NET

Tested with NuGet Version: 2.8.1
From the top folder of the source code, after compiling the solution in Release mode

```bat
cd MPI
nuget pack MPI.csproj -IncludeReferencedProjects -Prop Configuration=Release
```

## Troubleshooting

A placeholder to log issues

### Could not load file or assembly 'MPI

```
Unhandled Exception: System.IO.FileNotFoundException: Could not load file or assembly 'MPI, Version=1.3.0.0, Culture=neutral, PublicKeyToken=null' or one of its dependencies. The system cannot find the file specified.
   at AllgatherTest.Main(String[] args)
```

Prior to Aug 2017 the test projects were not referencing MPI and MPIUtils projects, hence dependencies were missing. Running the tests would thus not work. This should not be the case anymore.

### error CS1061: 'Unsafe.MPI\_Status' does not contain a definition for 'MPI\_SOURCE'

```
Status.cs(61,31): error CS1061: 'Unsafe.MPI_Status' does not contain a definition for 'MPI_SOURCE' and no extension method 'MPI_SOURCE' accepting a first argument of type 'Unsafe.MPI_Status' could be found (are you missing a using directive or an assembly reference?)
```

On Linux some code is generated (CustomUnsafe.cs) by the script Unsafe.pl. It parses the 'mpi.h' header file of your distribution. Due to variations across MPI implementations and versions thereof, this is succeptible to issues. You may need to fix Unsafe.pl or fix CustomUnsafe.cs manually (beware however not to loose work as the latter is generated by the 'make' processs)