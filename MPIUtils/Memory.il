// Copyright (C) 2007  The Trustees of Indiana University
//
// Use, modification and distribution is subject to the Boost Software
// License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//  
// Authors: Douglas Gregor
//          Andrew Lumsdaine
//
// Support for some extremely unsafe operations 

.assembly extern mscorlib
{
  .publickeytoken = (B7 7A 5C 56 19 34 E0 89 )                         // .z\V.4..
  .ver 2:0:0:0
}

.assembly MPIUtils
{
  .ver 0:1:0:0
}

.namespace MPIUtils
{
  .class public auto Memory extends [mscorlib]System.Object
  {
    .method public hidebysig static native int LoadAddress<T>(!!T& 'value') cil managed
    {
      // Code size       14 (0xe)
      .maxstack  1
      .locals init ([0] !!T& pinned ptr)
      ldarg.0
      stloc.0
      ldloc.0
      conv.i
      newobj     instance void [mscorlib]System.IntPtr::.ctor(void*)
      ret
    }

    .method public hidebysig static native int LoadAddressOfOut<T>([out] !!T& 'value') cil managed
    {
      // Code size       14 (0xe)
      .maxstack  1
      .locals init ([0] !!T& pinned ptr)
      ldarg.0
      stloc.0
      ldloc.0
      conv.i
      newobj     instance void [mscorlib]System.IntPtr::.ctor(void*)
      ret
    }
  }
}