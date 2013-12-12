/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises Communicator.ReduceScatter.
 */
using System;
using MPI;
using System.Diagnostics;

class ReduceScatterTest
{
    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            Intracommunicator world = MPI.Communicator.world;

            // Each process will receive Rank values 
            int[] counts = new int[world.Size];
            int sum = 0;
            for (int i = 0; i < world.Size; ++i)
            {
                counts[i] = i;
                sum += i;
            }

            if (world.Rank == 0)
                System.Console.Write("Testing reduce-scatter on integers...");
            int[] intValues = new int[sum];
            for (int rank = 0, index = 0; rank < world.Size; ++rank)
            {
                for (int i = 0; i < rank; ++i, ++index)
                    intValues[index] = i + world.Rank;
            }
            int[] intResults = world.ReduceScatter(intValues, Operation<int>.Add, counts);
            Debug.Assert(intResults.Length == world.Rank);
            for (int i = 0; i < world.Rank; ++i)
            {
                Debug.Assert(intResults[i] == world.Size * i + world.Size * (world.Size - 1) / 2);
            }
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

            if (world.Rank == 0)
                System.Console.Write("Testing reduce-scatter on strings...");
            string[] stringValues = new string[sum];
            for (int i = 0; i < sum; ++i)
                stringValues[i] = intValues[i].ToString();
            string[] stringResults = world.ReduceScatter(stringValues, Operation<string>.Add, counts);
            Debug.Assert(stringResults.Length == world.Rank);
            for (int i = 0; i < world.Rank; ++i)
            {
                string expected = "";
                for (int p = 0; p < world.Size; ++p)
                    expected += (i + p).ToString();
                Debug.Assert(stringResults[i] == expected);
            }
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");
        }
    }
}
