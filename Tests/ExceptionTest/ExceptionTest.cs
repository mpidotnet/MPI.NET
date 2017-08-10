/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises MPI.NET's exception mechanism.
 */
using System;
using MPI;
using MPI.TestCommons;

class ExceptionTest
{
    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            if (Communicator.world.Size != 2)
            {
                if (Communicator.world.Rank == 0)
                {
                    Console.Error.WriteLine("error: ExceptionTest.exe must be executed with two processes");
                    Console.Error.WriteLine("try: mpiexec -n 2 ExceptionTest.exe");
                }
                MPI.Environment.Abort(-1);
            }

            if (Communicator.world.Rank == 0)
            {
                int[] values = { 1, 2, 3, 4, 5 };
                Communicator.world.Send(values, 1, 0);
            }
            else
            {
                int[] values = new int[4]; // Too small!

                bool caught = false;
                try
                {
                    Communicator.world.Receive(0, 0, ref values);
                }
                catch (MPI.MessageTruncatedException e)
                {
                    caught = true;
                    Console.WriteLine("Caught expected exception: " + e.ToString());
                }

                if (!caught)
                    MPI.Environment.Abort(-1);
            }
        }
    }
}
