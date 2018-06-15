/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This example program collects the names of all of the hosts that the
 * MPI job is running on, and the root node (process 0) prints them.
 */
using System;
using MPI;

class Hostnames
{
    static void Main(string[] args)
    {
        MPI.Environment.Run(ref args, comm =>
        {
            string[] hostnames = comm.Gather(MPI.Environment.ProcessorName, 0);
            if (comm.Rank == 0)
            {
                Array.Sort(hostnames);
                foreach (string host in hostnames)
                    Console.WriteLine(host);
            }
        });
    }
}
