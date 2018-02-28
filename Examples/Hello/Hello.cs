/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This example program prints the rank and size of the "world" communicator
 * on every process.
 */
using System;
using System.Text;
using MPI;

class Hello
{
    static void Main(string[] args)
    {
        MPI.Environment.Run(ref args, communicator =>
        {
            Console.WriteLine("Hello, from process number "
                                     + communicator.Rank + " of "
                                     + communicator.Size);
        });
    }
}
