/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Ben Martin
 *          Andrew Lumsdaine
 * 
 * This test verifies that the CartesianCommunicator is working properly.
 */
using System;
using System.Collections.Generic;
using System.Text;
using MPI;
using System.Diagnostics;
using MPI.TestCommons;

namespace CartTest
{
    class CartTest
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

                // Test helper function ComputeDimensions()
                if (isRoot)
                    System.Console.WriteLine("Testing ComputeDimensions()...");
                int nprocesses = MPI.Communicator.world.Size;
                int[] dims = { 0, 0 };
                CartesianCommunicator.ComputeDimensions(nprocesses, 2, ref dims);
                if (isRoot)
                    System.Console.WriteLine("Dims = " + dims[0] + ", " + dims[1]);
                if (isRoot)
                    System.Console.WriteLine(" done.");

                // Test constructor
                if (isRoot)
                    System.Console.WriteLine("Testing constructor...");
                bool[] periods = { true, true };
                MPI.Intracommunicator ic = (MPI.Intracommunicator)MPI.Communicator.world;
                MPI.CartesianCommunicator cc =  new CartesianCommunicator(ic, 2, dims, periods, false);
                MPIDebug.Assert(cc != null);
                if (isRoot)
                    System.Console.WriteLine(" done.");

                // Test accessors
                if (isRoot)
                    System.Console.WriteLine("Testing accessors...");
                int ndims = cc.Dimensions.Length;
                int[] coords = new int[ndims];
                coords = cc.Coordinates;
                int[] temp_dims = { 0, 0 };
                bool[] temp_periods = { false, false };
                int[] temp_coords = { 0, 0 };
                temp_dims = cc.Dimensions;
                temp_periods = cc.Periodic;
                MPIDebug.Assert(temp_dims[0] == dims[0] && temp_dims[1] == dims[1]);
                MPIDebug.Assert(temp_periods[0] == periods[0] && temp_periods[1] == periods[1]);
                temp_coords[0] = 0;
                temp_coords[1] = 0;
                temp_coords = cc.GetCartesianCoordinates(cc.Rank);
                int temp_rank = cc.GetCartesianRank(temp_coords);
                MPIDebug.Assert(temp_rank == cc.Rank);
                MPIDebug.Assert(temp_coords[0] == coords[0] && temp_coords[1] == coords[1]);
                //System.Console.WriteLine(temp_rank + " ?= " + cc.Rank + " for coords = " + temp_coords[0] + ", " + temp_coords[1]);

                // Test NumEdges
                int nedges = 0;
                nedges = dims[0] - 1;
                if (periods[0] == true)
                    nedges++;
                int nedges_slice;
                int nodes_so_far = dims[0];
                for (int i = 1; i < ndims; i++)
                {
                    nedges_slice = dims[i] - 1;
                    if (periods[i] == true)
                        nedges_slice++;
                    nedges_slice *= nodes_so_far;
                    nodes_so_far *= dims[i];
                    nedges = nedges * dims[i] + nedges_slice;
                }
                MPIDebug.Assert(nedges == cc.NumEdges);
                if (isRoot)
                    System.Console.WriteLine(" done.");

                // Test Shift() and communications
                if (isRoot)
                    System.Console.WriteLine("Testing Shift() and send/recv...");
                int source = 0;
                int dest = 0;
                cc.Shift(0, 1, out source, out dest);
                cc.Send(coords, dest, 0);
                //System.Console.WriteLine("Process at " + coords[0] + ", " + coords[1] + " (rank " + cc.Rank + ") will receive from " + source + " and send to " + dest);
                int[] recvcoords = new int[ndims];
                cc.Receive(source, 0, ref recvcoords);
                //System.Console.WriteLine("Coords = " + coords[0] + ", " + coords[1] + "; Received message from coords = " + recvcoords[0] + ", " + recvcoords[1]);
                MPIDebug.Assert(coords[0] == (recvcoords[0] + 1) % dims[0] && coords[1] == recvcoords[1]);
                if (isRoot)
                    System.Console.WriteLine(" done.");
                
                // Test Sub()
                if (isRoot)
                  System.Console.WriteLine("Testing Sub()...");
                int[] remain_dims = {1, 0};
                CartesianCommunicator subgrid = cc.Subgrid(remain_dims);
                int subgrid_ndims = subgrid.Dimensions.Length;
                MPIDebug.Assert(subgrid_ndims == 1);
                if (isRoot)
                    System.Console.WriteLine(" done.");

                // Test Neighbors
                if (isRoot)
                    System.Console.WriteLine("Testing Neighbors...");
                int[] neighbors = cc.Neighbors;

                /*
                if (isRoot)
                {
                    System.Console.Write("Neighbors: ");
                    foreach (int n in neighbors)
                        System.Console.Write(n + " ");
                    System.Console.WriteLine();
                }
                */

                List<int> local_neighbors = new List<int>();
                int[] neighbor_coords;

                for (int i = 0; i < ndims; i++)
                {
                    neighbor_coords = cc.Coordinates;
                    if (neighbor_coords[i] > 0)
                    {
                        neighbor_coords[i] = cc.Coordinates[i] - 1;
                        local_neighbors.Add(cc.GetCartesianRank(neighbor_coords));
                    }
                    else if (cc.Periodic[i] == true && cc.Dimensions[i] > 2)
                    {
                        neighbor_coords[i] = cc.Dimensions[i] - 1;
                        local_neighbors.Add(cc.GetCartesianRank(neighbor_coords));
                    }

                    neighbor_coords = cc.Coordinates;
                    if (neighbor_coords[i] < dims[i] - 1)
                    {
                        neighbor_coords[i] = cc.Coordinates[i] + 1;
                        local_neighbors.Add(cc.GetCartesianRank(neighbor_coords));
                    }
                    else if(cc.Periodic[i] == true && cc.Dimensions[i] > 2)
                    {
                        neighbor_coords[i] = 0;
                        local_neighbors.Add(cc.GetCartesianRank(neighbor_coords));
                    }
                }
                
                local_neighbors.Sort();
                Array.Sort(neighbors);

                MPIDebug.Assert(neighbors.Length == local_neighbors.Count);
                for (int i = 0; i < neighbors.Length; i++)
                    MPIDebug.Assert(neighbors[i] == local_neighbors[i]);
                
                //System.Console.WriteLine(neighbors.Length + " " + local_neighbors.Count);
                  
                if (isRoot)
                    System.Console.WriteLine(" done.");




                // Now test for 3d case
                ndims = 3;
                if (isRoot)
                    System.Console.WriteLine("Testing ComputeDimensions() with 3d...");
                nprocesses = MPI.Communicator.world.Size;
                dims = new int[3];
                dims[0] = 0;
                dims[1] = 0;
                dims[2] = 2;
                CartesianCommunicator.ComputeDimensions(nprocesses, 3, ref dims);
                if (isRoot)
                    System.Console.WriteLine("Dims = " + dims[0] + ", " + dims[1] + ", " + dims[2]);
                if (isRoot)
                    System.Console.WriteLine(" done.");

                // Test constructor
                if (isRoot)
                    System.Console.WriteLine("Testing constructor with 3d...");
                periods = new bool[3];
                periods[0] = true;
                periods[1] = true;
                periods[2] = true;
                ic = (MPI.Intracommunicator)MPI.Communicator.world;
                cc = new CartesianCommunicator(ic, 3, dims, periods, false);
                MPIDebug.Assert(cc != null);
                if (isRoot)
                    System.Console.WriteLine(" done.");


                // Test NumEdges
                if (isRoot)
                    System.Console.WriteLine("Testing NumEdges with 3d...");
                nedges = 0;
                nedges = dims[0] - 1;
                if (periods[0] == true)
                    nedges++;
                nodes_so_far = dims[0];
                for (int i = 1; i < ndims; i++)
                {
                    nedges_slice = dims[i] - 1;
                    if (periods[i] == true)
                        nedges_slice++;
                    nedges_slice *= nodes_so_far;
                    nodes_so_far *= dims[i];
                    nedges = nedges * dims[i] + nedges_slice;
                }
                MPIDebug.Assert(nedges == cc.NumEdges);
                if (isRoot)
                    System.Console.WriteLine(" done.");

                // Test Neighbors
                if (isRoot)
                    System.Console.WriteLine("Testing Neighbors with 3d...");
                neighbors = cc.Neighbors;

                /*
                if (isRoot)
                {
                    System.Console.Write("Neighbors: ");
                    foreach (int n in neighbors)
                        System.Console.Write(n + " ");
                    System.Console.WriteLine();
                }
                */
                
                local_neighbors = new List<int>();

                for (int i = 0; i < ndims; i++)
                {
                    neighbor_coords = cc.Coordinates;
                    if (neighbor_coords[i] > 0)
                    {
                        neighbor_coords[i] = cc.Coordinates[i] - 1;
                        local_neighbors.Add(cc.GetCartesianRank(neighbor_coords));
                    }
                    else if (cc.Periodic[i] == true && cc.Dimensions[i] > 2)
                    {
                        neighbor_coords[i] = cc.Dimensions[i] - 1;
                        local_neighbors.Add(cc.GetCartesianRank(neighbor_coords));
                    }

                    neighbor_coords = cc.Coordinates;
                    if (neighbor_coords[i] < dims[i] - 1)
                    {
                        neighbor_coords[i] = cc.Coordinates[i] + 1;
                        local_neighbors.Add(cc.GetCartesianRank(neighbor_coords));
                    }
                    else if(cc.Periodic[i] == true && cc.Dimensions[i] > 2)
                    {
                        neighbor_coords[i] = 0;
                        local_neighbors.Add(cc.GetCartesianRank(neighbor_coords));
                    }
                }
                
                local_neighbors.Sort();
                Array.Sort(neighbors);

                MPIDebug.Assert(neighbors.Length == local_neighbors.Count);
                for (int i = 0; i < neighbors.Length; i++)
                    MPIDebug.Assert(neighbors[i] == local_neighbors[i]);
                
                // System.Console.WriteLine(cc.Rank + ": " + cc.Coordinates[0] + ", " + cc.Coordinates[1] + ", " + cc.Coordinates[2]);

                if (isRoot)
                    System.Console.WriteLine(" done.");
































            }
        }
    }
}