/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises Intracommunicator.Scan.
 */

using MPI.TestCommons;

class EnvironmentDisposalTest
{
    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        MPI.Environment.Run(ref args, comm => { int rank = comm.Rank; }, false);
        MPI.Environment.Run(ref args, comm => { int rank = comm.Rank; }, false);
        MPI.Environment.Run(ref args, comm => { int rank = comm.Rank; }, true);
    }
}