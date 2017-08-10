/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test verifies the correctness of the Scatter collective operation.
 */
using System;
using MPI;
using System.Diagnostics;
using MPI.TestCommons;

class ScatterTest
{
    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            Intracommunicator world = Communicator.world;
            if (world.Rank == 0)
            {
                System.Console.Write("Testing scatter of integers...");
                int[] ranks = new int[world.Size];
                for (int i = 0; i < world.Size; ++i)
                    ranks[i] = i;

                int myRank = world.Scatter(ranks, 0);
                MPIDebug.Assert(myRank == 0);
                System.Console.WriteLine(" done.");

                System.Console.Write("Testing scatter of strings...");
                string[] rankStrings = new string[world.Size];
                for (int i = 0; i < world.Size; ++i)
                    rankStrings[i] = i.ToString();

                string myRankString = world.Scatter(rankStrings, 0);
                MPIDebug.Assert(myRankString == world.Rank.ToString());
                System.Console.WriteLine(" done.");
            }
            else
            {
                int myRank = world.Scatter<int>(null, 0);
                MPIDebug.Assert(myRank == world.Rank);

                string myRankString = world.Scatter<string>(null, 0);
                MPIDebug.Assert(myRankString == world.Rank.ToString());
            }

            if (world.Rank == 0)
            {
                System.Console.Write("Testing Scatter of bools...");
                bool[] odds = new bool[world.Size];
                for (int i = 0; i < world.Size; ++i)
                {
                    odds[i] = i % 2 == 1;
                }
                bool amIOdd = world.Scatter(odds, 0);
                MPIDebug.Assert(!amIOdd);
                System.Console.WriteLine(" done.");
            }
            else
            {
                bool amIOdd = world.Scatter<bool>(null, 0);
                MPIDebug.Assert(amIOdd == (world.Rank % 2 == 1));
            }

            world.Barrier();
            if (world.Rank == 0)
            {
                int size = world.Size;
                System.Console.Write("Testing ScatterFromFlattened of integers...");
                int[] inRanks = new int[(size * size - size) / 2];
                int[] outRanks = null;
                int[] counts = new int[size];
                int p = 0;
                for (int i = 0; i < world.Size; ++i)
                {
                    counts[i] = i;
                    for (int j = 0; j < i; j++)
                        inRanks[p + j] = i;
                    p += i;
                }
                world.ScatterFromFlattened(inRanks, counts, 0, ref outRanks);
                MPIDebug.Assert(outRanks.Length == 0);
                System.Console.WriteLine(" done.");               
            }
            else
            {
                int[] outRanks = null;
                int[] counts = new int[world.Size];
                for (int i = 0; i < world.Size; ++i)
                    counts[i] = i;

                world.ScatterFromFlattened(null, counts, 0, ref outRanks);
                for (int i = 0; i < world.Rank; i++)
                    MPIDebug.Assert(outRanks[i] == world.Rank);
            }

            if (world.Rank == 0)
            {
                int size = world.Size;
                System.Console.Write("Testing ScatterFromFlattened of strings...");
                string[] inRanks = new string[(size * size - size) / 2];
                string[] outRanks = null;
                int[] counts = new int[size];
                int p = 0;
                for (int i = 0; i < world.Size; ++i)
                {
                    counts[i] = i;
                    for (int j = 0; j < i; j++)
                        inRanks[p + j] = i.ToString();
                    p += i;
                }
                world.ScatterFromFlattened(inRanks, counts, 0, ref outRanks);
                MPIDebug.Assert(outRanks.Length == 0);
                System.Console.WriteLine(" done.");
            }
            else
            {
                string[] outRanks = null;
                int[] counts = new int[world.Size];
                for (int i = 0; i < world.Size; ++i)
                    counts[i] = i;

                world.ScatterFromFlattened(null, counts, 0, ref outRanks);
                for (int i = 0; i < world.Rank; i++)
                    MPIDebug.Assert(outRanks[i] == world.Rank.ToString());
            }

        }
    }
}
