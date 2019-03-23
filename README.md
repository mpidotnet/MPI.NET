# MPI.NET: High-performance C# Library for Message Passing

MPI.NET is a high-performance, easy-to-use implementation of the Message Passing Interface (MPI) for Microsoft's .NET environment. MPI is the de facto standard for writing parallel programs running on a distributed memory system, such as a compute cluster, and is widely implemented. Most MPI implementations provide support for writing MPI programs in C, C++, and Fortran. MPI.NET provides support for all of the .NET languages (especially C#), and includes significant extensions (such as automatic serialization of objects) that make it far easier to build parallel programs that run on clusters.

## Contributions

MPI.NET was authored by Douglas Gregor, Andrew Lumsdaine, Ben Martin (and others TBC) (Copyright 2005-2008 The Trustees of Indiana University).

It was migrated in 2013 to [MPI.NET on github](https://github.com/jmp75/MPI.NET) with contributions from [J-M](https://github.com/jmp75) and others.

From 2018 onwards most contributions occur on the fork at [Microsoft/MPI.NET](https://github.com/Microsoft/MPI.NET).

Use, modification and distribution is subject to the Boost Software License, Version 1.0. (See accompanying file [LICENSE_1_0.txt](./LICENSE_1_0.txt) or copy at http://www.boost.org/LICENSE_1_0.txt)

## Getting started

The repository includes documentation [here](https://github.com/Microsoft/MPI.NET/tree/master/Documentation).  

The repository also include examples, available [here](https://github.com/Microsoft/MPI.NET/tree/master/Examples).

## Installation on Windows

The recommended way to use MPI.NET is to consume it as a submodule.

MPI.NET on Windows is available only for use with Microsoft's MPI, [MS-MPI](https://msdn.microsoft.com/en-us/library/bb524831), which is available as part of the Microsoft Compute Cluster Pack in Windows Compute Cluster Server and as a separate download, the Microsoft Compute Cluster Pack SDK. Please see the MPI.NET page for more information about installation of one of these packages before installing.

### From March 2019

```bat
cd C:\path\to\MPI.NET
dotnet restore MPI.sln
:: assuming msbuild is in your PATH
msbuild MPI.sln /p:Platform="Any CPU" /p:Configuration=Release /consoleloggerparameters:ErrorsOnly
```

### Deprecated instructions on Windows:

If you clone/download the source code, you will see a solution file at the top level. It should be straightforward to compile, except for one thing: MPIUtils is compiling from IL directly, and requires ilasm.exe to be found. The post-build event in the project file includes batch commands that try to find the correct ilasm.exe from visual studio settings, but this may still fail on your machine.

You may try to build from the command line with e.g.:

```bat
cd C:\path\to\MPI.NET\Build
build.bat Debug Rebuild
```

### Unit tests

To run all unit tests:

```bat
cd C:\path\to\MPI.NET
msbuild MPI.sln /p:Platform="Any CPU" /p:Configuration=Release /consoleloggerparameters:ErrorsOnly
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

Support for MPI.NET on Unix platforms is provided based on the Mono C# compiler and its toolchain. The .NET landscape on Linux is fast changing, both with mono increasingly using open source .NET code from Microsoft and the emergence of [.NET Core](https://github.com/dotnet/core).

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

## Technical notes

### Creating the NuGet package for MPI.NET

*This section is primarily a reminder to the package author.*

```bash
dotnet pack MPI/MPI.csproj --configuration Release --no-build --no-restore --output nupkgs
# Or for initial testing/debugging
dotnet pack MPI/MPI.csproj --configuration Debug --no-build --no-restore --output nupkgs
```

If you have an additional nuget package repository for tests:

```cmd
cp .\MPI\nupkgs\\MPI.NET.1.4.0.nupkg c:\local\nuget
```

### Troubleshooting

A placeholder to log issues

#### error CS1061: 'Unsafe.MPI\_Status' does not contain a definition for 'MPI\_SOURCE'

```text
Status.cs(61,31): error CS1061: 'Unsafe.MPI_Status' does not contain a definition for 'MPI_SOURCE' and no extension method 'MPI_SOURCE' accepting a first argument of type 'Unsafe.MPI_Status' could be found (are you missing a using directive or an assembly reference?)
```

On Linux some code is generated (CustomUnsafe.cs) by the script Unsafe.pl. It parses the 'mpi.h' header file of your distribution. Due to variations across MPI implementations and versions thereof, this is succeptible to issues. You may need to fix Unsafe.pl or fix CustomUnsafe.cs manually (beware however not to lose work as the latter is generated by the 'make' processs)
