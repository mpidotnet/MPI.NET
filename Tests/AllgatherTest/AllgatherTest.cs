/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises Communicator.Allgather.
 */
using System;
using MPI;
using System.Diagnostics;
using MPI.TestCommons;

class AllgatherTest
{
    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            bool isRoot = (Communicator.world.Rank == 0);
            if (isRoot)
                System.Console.Write("Testing Allgather with integers...");
            int[] ranks = Communicator.world.Allgather(Communicator.world.Rank);
            MPIDebug.Assert(ranks.Length == Communicator.world.Size);
            for (int i = 0; i < ranks.Length; ++i)
                MPIDebug.Assert(ranks[i] == i);
            if (isRoot)
                System.Console.WriteLine(" done.");

            if (isRoot)
                System.Console.Write("Testing Allgather with strings...");
            string[] rankStrings = Communicator.world.Allgather(Communicator.world.Rank.ToString());
            MPIDebug.Assert(rankStrings.Length == Communicator.world.Size);
            for (int i = 0; i < rankStrings.Length; ++i)
                MPIDebug.Assert(rankStrings[i] == i.ToString());
            if (isRoot)
                System.Console.WriteLine(" done.");

            if (isRoot)
                System.Console.Write("Testing AllgatherFlattened with integers...");
            int size = Communicator.world.Size;
            int[] outData = new int[(size * size - size) / 2];
            int[] inData = new int[Communicator.world.Rank];
            int[] counts = new int[size];
            for (int i = 0; i < Communicator.world.Rank; i++)
                inData[i] = Communicator.world.Rank;
            for (int i = 0; i < size; i++)
                counts[i] = i;
            Communicator.world.AllgatherFlattened(inData, counts, ref outData);
            int p = 0;
            for (int i = 0; i < size; ++i)
            {
                if (counts[i] > 0)
                   for (int j = 0; j < i; j++)
                        MPIDebug.Assert(outData[p] == i);
                p += counts[i];
            }
            if (isRoot)
                System.Console.WriteLine(" done.");

            if (isRoot)
                System.Console.Write("Testing AllgatherFlattened with string...");
            string[] outData_s = null;
            string[] inData_s = new string[Communicator.world.Rank];
            for (int i = 0; i < Communicator.world.Rank; i++)
                inData_s[i] = Communicator.world.Rank.ToString();
            Communicator.world.AllgatherFlattened(inData_s, counts, ref outData_s);
            p = 0;
            for (int i = 0; i < size; ++i)
            {
                if (counts[i] > 0)
                    for (int j = 0; j < i; j++)
                        MPIDebug.Assert(outData_s[p] == i.ToString());
                p += counts[i];
            }
            if (isRoot)
                System.Console.WriteLine(" done.");

        }
    }
}
