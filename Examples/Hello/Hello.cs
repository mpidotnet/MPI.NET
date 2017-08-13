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
        using (new MPI.Environment(ref args))
        {
            System.Console.WriteLine("Hello, from process number " 
                + MPI.Communicator.world.Rank.ToString() + " of "
                + MPI.Communicator.world.Size.ToString());
        }
    }
}
