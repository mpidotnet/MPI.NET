/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This program tests the Alltoall operation in MPI.NET.
 */
using System;
using System.Text;
using MPI;
using System.Diagnostics;
using MPI.TestCommons;

class AlltoallTest
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
                System.Console.Write("Testing all-to-all on integers...");
            int[] sendInts = new int[world.Size];
            for (int dest = 0; dest < world.Size; ++dest)
                sendInts[dest] = world.Size * world.Rank + dest;
            int[] recvInts = world.Alltoall(sendInts);
            for (int source = 0; source < world.Size; ++source)
                MPIDebug.Assert(recvInts[source] == world.Size * source + world.Rank);
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

            if (world.Rank == 0)
                System.Console.Write("Testing all-to-all on strings...");
            string[] sendStrings = new string[world.Size];
            for (int dest = 0; dest < world.Size; ++dest)
                sendStrings[dest] = sendInts[dest].ToString();
            string[] recvStrings = world.Alltoall(sendStrings);
            for (int source = 0; source < world.Size; ++source)
                MPIDebug.Assert(recvStrings[source] == recvInts[source].ToString());
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

            if (world.Rank == 0)
                System.Console.Write("Testing AlltoallFlattened with integers...");
            int size = Communicator.world.Size;
            int rank = Communicator.world.Rank;
            int[] outData = new int[(size * size - size) / 2];
            int[] inData = new int[rank * size];
            int[] sendCounts = new int[size];
            int[] recvCounts = new int[size];
            for (int i = 0; i < rank*size; i++)
                inData[i] = rank;
            for (int i = 0; i < size; i++)
                sendCounts[i] = rank;
            for (int i = 0; i < size; i++)
                recvCounts[i] = i;
            Communicator.world.AlltoallFlattened(inData, sendCounts, recvCounts, ref outData);
            int p = 0;
            for (int i = 0; i < size; ++i)
            {
                if (recvCounts[i] > 0)
                    for (int j = 0; j < i; j++)
                        MPIDebug.Assert(outData[p] == i);
                p += recvCounts[i];
            }           
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

            if (world.Rank == 0)
                System.Console.Write("Testing AlltoallFlattened with strings...");
            string[] outData_s = new string[(size * size - size) / 2];
            string[] inData_s = new string[rank * size];
            for (int i = 0; i < rank * size; i++)
                inData_s[i] = rank.ToString();
            Communicator.world.AlltoallFlattened(inData_s, sendCounts, recvCounts, ref outData_s);
            p = 0;
            for (int i = 0; i < size; ++i)
            {
                if (recvCounts[i] > 0)
                    for (int j = 0; j < i; j++)
                        MPIDebug.Assert(outData_s[p] == i.ToString());
                p += recvCounts[i];
            } 
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

        }
    }
}
