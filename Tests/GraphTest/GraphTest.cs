/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Ben Martin
 *          Andrew Lumsdaine
 * 
 * This test verifies that the GraphCommunicator is working properly.
 */
using System;
using System.Diagnostics;
using MPI;

namespace GraphTest
{
    static class GraphTest
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main(string[] args)
        {
            using (new MPI.Environment(ref args))
            {
                bool isRoot = (Communicator.world.Rank == 0);

                // Construct graph
                if (isRoot)
                    System.Console.WriteLine("Testing constructor...");
                int nprocesses = MPI.Communicator.world.Size;
                int[][] edges = new int[nprocesses][];
                int nneighbors = nprocesses - 1;

                for (int i = 0; i < nprocesses; i++)
                {
                    edges[i] = new int[3];
                    edges[i][0] = (i + 1) % nprocesses;
                    edges[i][1] = (i + 2) % nprocesses;
                    edges[i][2] = (i + 3) % nprocesses;
                }

                MPI.GraphCommunicator gc = new MPI.GraphCommunicator(MPI.Communicator.world, edges, false);
                if (isRoot)
                    System.Console.WriteLine(" done.");

                // Test Edges 
                if (isRoot)
                    System.Console.WriteLine("Testing Edges...");
                int[][] edges2;
                edges2 = gc.Edges;
                for (int j = 0; j < edges.Length; j++)
                {
                    for (int k = 0; k < edges[j].Length; k++)
                        Debug.Assert(edges[j][k] == edges2[j][k]);
                }
                if (isRoot)
                    System.Console.WriteLine(" done.");

                // Test Edges 
                if (isRoot)
                    System.Console.WriteLine("Testing NumEdges...");
                Debug.Assert(gc.NumEdges == 3 * nprocesses);
                if (isRoot)
                    System.Console.WriteLine(" done.");

                // Test Neighbors
                if (isRoot)
                    System.Console.WriteLine("Testing Neighbors...");
                Debug.Assert(gc.Neighbors[0] == edges[gc.Rank][0]);
                Debug.Assert(gc.Neighbors[1] == edges[gc.Rank][1]);
                Debug.Assert(gc.Neighbors[2] == edges[gc.Rank][2]);
                if (isRoot)
                    System.Console.WriteLine(" done.");


                // Test NeighborsOf()
                if (isRoot)
                    System.Console.WriteLine("Testing NeighborsOf()...");
                Debug.Assert(gc.NeighborsOf((gc.Rank + 1) % gc.Size)[0] == edges[(gc.Rank + 1) % gc.Size][0]);
                Debug.Assert(gc.NeighborsOf((gc.Rank + 1) % gc.Size)[1] == edges[(gc.Rank + 1) % gc.Size][1]);
                Debug.Assert(gc.NeighborsOf((gc.Rank + 1) % gc.Size)[2] == edges[(gc.Rank + 1) % gc.Size][2]);
                if (isRoot)
                    System.Console.WriteLine(" done.");


            }
        }
    }
}