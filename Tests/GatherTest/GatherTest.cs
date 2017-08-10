/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises Communicator.Gather.
 */
using System;
using MPI;
using System.Diagnostics;
using MPI.TestCommons;

class GatherTest
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
            {
                System.Console.Write("Testing Gather with integers...");
                int[] ranks = new int[Communicator.world.Size];
                Communicator.world.Gather(Communicator.world.Rank, 0, ref ranks);
                MPIDebug.Assert(ranks.Length == Communicator.world.Size);
                for (int i = 0; i < ranks.Length; ++i)
                    MPIDebug.Assert(ranks[i] == i);
                System.Console.WriteLine(" done.");
            }
            else
            {
                Communicator.world.Gather(Communicator.world.Rank, 0);
            }

            if (isRoot)
            {
                System.Console.Write("Testing Gather with strings...");
                string[] rankStrings = new string[Communicator.world.Size];
                Communicator.world.Gather(Communicator.world.Rank.ToString(), 0, ref rankStrings);
                MPIDebug.Assert(rankStrings.Length == Communicator.world.Size);
                for (int i = 0; i < rankStrings.Length; ++i)
                    MPIDebug.Assert(rankStrings[i] == i.ToString());
                System.Console.WriteLine(" done.");
            }
            else
            {
                Communicator.world.Gather(Communicator.world.Rank.ToString(), 0);
            }


            if (isRoot)
            {
                System.Console.Write("Testing uniform Gather with integers...");
                int[] outData = new int[Communicator.world.Size * Communicator.world.Size];
                int[] inData = new int[Communicator.world.Size];
                for (int i = 0; i < Communicator.world.Size; i++)
                    inData[i] = i;
                Communicator.world.GatherFlattened(inData, 0, ref outData);
                MPIDebug.Assert(outData.Length == Communicator.world.Size * Communicator.world.Size);
                for (int i = 0; i < Communicator.world.Size; ++i)
                    for (int j = 0; j < Communicator.world.Size; j++)
                        MPIDebug.Assert(outData[i * Communicator.world.Size + j] == j);
                System.Console.WriteLine(" done.");
            }
            else
            {
                int[] outData = null;
                int[] inData = new int[Communicator.world.Size];
                for (int i = 0; i < Communicator.world.Size; i++)
                    inData[i] = i;
                Communicator.world.GatherFlattened(inData, 0, ref outData);
            }

            if (isRoot)
            {
                System.Console.Write("Testing uniform GatherFlattened with strings...");
                string[] outData = new string[Communicator.world.Size * Communicator.world.Size];
                string[] inData = new string[Communicator.world.Size];
                for (int i = 0; i < Communicator.world.Size; i++)
                    inData[i] = i.ToString();
                Communicator.world.GatherFlattened(inData, 0, ref outData);
                MPIDebug.Assert(outData.Length == Communicator.world.Size * Communicator.world.Size);
                for (int i = 0; i < Communicator.world.Size; ++i)
                    for (int j = 0; j < Communicator.world.Size; j++)
                        MPIDebug.Assert(outData[i * Communicator.world.Size + j] == j.ToString());
                System.Console.WriteLine(" done.");
            }
            else
            {
                string[] outData = null;
                string[] inData = new string[Communicator.world.Size];
                for (int i = 0; i < Communicator.world.Size; i++)
                    inData[i] = i.ToString();
                Communicator.world.GatherFlattened(inData, 0, ref outData);
            }

            if (isRoot)
            {
                System.Console.Write("Testing GatherFlattened with integers...");
                int size = Communicator.world.Size;
                int[] outData = new int[(size * size + size)/2];
                int[] inData = new int[Communicator.world.Rank];
                int[] counts = new int[size];
                for (int i = 0; i < Communicator.world.Rank; i++)
                    inData[i] = Communicator.world.Rank;
                for (int i = 0; i < size; i++)
                    counts[i] = i;
                Communicator.world.GatherFlattened(inData, counts, 0, ref outData);
                int p = 0;
                for (int i = 0; i < size; ++i)
                {
                    if (counts[i] > 0)
                        for (int j = 0; j < i; j++)
                            MPIDebug.Assert(outData[p] == i);
                    p += counts[i];
                }               
                System.Console.WriteLine(" done.");
            }
            else
            {
                int[] counts = null;
                int[] outData = null;
                int[] inData = new int[Communicator.world.Rank];
                for (int i = 0; i < Communicator.world.Rank; i++)
                    inData[i] = Communicator.world.Rank;
                Communicator.world.GatherFlattened(inData, counts, 0, ref outData);
            }

            if (isRoot)
            {
                System.Console.Write("Testing GatherFlattened with strings...");
                int size = Communicator.world.Size;
                string[] outData = new string[(size * size + size) / 2];
                string[] inData = new string[Communicator.world.Rank];
                int[] counts = new int[size];
                for (int i = 0; i < Communicator.world.Rank; i++)
                    inData[i] = Communicator.world.Rank.ToString();
                for (int i = 0; i < size; i++)
                    counts[i] = i;
                Communicator.world.GatherFlattened(inData, counts, 0, ref outData);
                int p = 0;
                for (int i = 0; i < size; ++i)
                {
                    if (counts[i] > 0)
                        for (int j = 0; j < i; j++)
                            MPIDebug.Assert(outData[p] == i.ToString());
                            //System.Console.WriteLine(p + " " + outData[p]);
                    p += counts[i];
                }

                System.Console.WriteLine(" done.");
            }
            else
            {
                int[] counts = null;
                string[] outData = null;
                string[] inData = new string[Communicator.world.Rank];
                for (int i = 0; i < Communicator.world.Rank; i++)
                    inData[i] = Communicator.world.Rank.ToString();
                Communicator.world.GatherFlattened(inData, counts, 0, ref outData);
            }

        }
    }
}
