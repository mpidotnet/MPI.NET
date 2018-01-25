/* Copyright (C) 2007, 2008  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Ben Martin
 *          Douglas Gregor
 *          Andrew Lumsdaine
 */
using System;
using System.Runtime.InteropServices;
using System.IO;

namespace MPI
{  
    // MPI data type definitions
#if MPI_HANDLES_ARE_POINTERS
    using MPI_Aint = IntPtr;
    using MPI_Comm = IntPtr;
    using MPI_Datatype = IntPtr;
    using MPI_Errhandler = IntPtr;
    using MPI_File = IntPtr;
    using MPI_Group = IntPtr;
    using MPI_Info = IntPtr;
    using MPI_Op = IntPtr;
    using MPI_Request = IntPtr;
    using MPI_User_function = IntPtr;
    using MPI_Win = IntPtr;
#else
    using MPI_Aint = IntPtr;
    using MPI_Comm = Int32;
    using MPI_Datatype = Int32;
    using MPI_Errhandler = Int32;
    using MPI_File = IntPtr;
    using MPI_Group = Int32;
    using MPI_Info = Int32;
    using MPI_Op = Int32;
    using MPI_Request = Int32;
    using MPI_User_function = IntPtr;
    using MPI_Win = Int32;
#endif

    /// <summary>
    ///   A GraphCommunicator is a form of <see cref="Intracommunicator"/> where the processes are
    ///   connected in an arbitrary graph topology.
    /// </summary>
    /// 
    /// <remarks>
    ///   With a GraphCommunicator, the topology of the processes is specified as an arbitrary graph, 
    ///   where each process will communicate with its neighbors in the graph. Graph topologies are
    ///   more general than the Cartesian topology provided by a <see cref="CartesianCommunicator"/>, 
    ///   but they also provide less information to the user and the underlying MPI implementation.
    /// </remarks>
    public class GraphCommunicator : TopologicalCommunicator
    {
        /// <summary>
        ///   Returns a recommended configuration for a new Graph communicator.
        /// </summary>
        /// <param name="oldcomm">
        ///   The existing communicator.
        /// </param>
        /// <param name="edges">
        ///   An array of edges as for the constructor <see cref="GraphCommunicator"/>.
        /// </param>
        /// <returns>
        ///   The new rank of the calling process.
        /// </returns>
        public static int Map(Intracommunicator oldcomm, int[][] edges)
        {
            int newrank;

            int nnodes = edges.Length;
            
            int nedges = 0;
            foreach (int[] arr in edges)
                checked { nedges += arr.Length; }
            int[] index = new int[nnodes];
            int[] edges_flat = new int[nedges];

            int j = 0;
            for (int i = 0; i < nnodes; i++)
            {
                index[i] = checked(j + edges[i].Length);
                Array.Copy(edges[i], 0, edges_flat, j, edges[i].Length);
                j = checked(j + edges[i].Length);
            }

            unsafe
            {
                fixed (int* indexPtr = index, edges_flatPtr = edges_flat)
                {
                    int errorCode = Unsafe.MPI_Graph_map(oldcomm.comm, nnodes, indexPtr, edges_flatPtr, out newrank);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }

            return newrank;
        }

        #region Communicator constructors
        internal GraphCommunicator()
            : base()
        {
        }

        /// <summary>
        ///   Constructs a new Graph communicator.
        /// </summary>
        /// <param name="oldComm">
        ///   An existing communicator from which to construct the Graph communicator.
        /// </param>
        /// <param name="edges">
        ///   A jagged array of adjacency information (so, for example, process i is adjacent to all 
        ///   of the processes whose ranks are listed in edges[i]).
        /// </param>
        /// <param name="reorder">
        ///   Whether the ranking may be reordered or not.
        /// </param>
        public GraphCommunicator(Intracommunicator oldComm, int[][] edges, bool reorder)
        {            
            int reorder_int = Convert.ToInt32(reorder);
            int nnodes = edges.Length;
            
            int nedges = 0;
            foreach (int[] arr in edges)
	            checked { nedges += arr.Length; }

            int[] index = new int[nnodes];
            int[] edges_flat = new int[nedges];

            int j = 0;
            for (int i = 0; i < nnodes; i++)
            {
                index[i] = checked(j + edges[i].Length);
                Array.Copy(edges[i], 0, edges_flat, j, edges[i].Length);
                j = checked(j + edges[i].Length);
            }

            unsafe
            {
                fixed (int* indexPtr = index, edges_flatPtr = edges_flat)
                    fixed(MPI_Comm* commPtr = &(this.comm))
                    {
                        int errorCode = Unsafe.MPI_Graph_create(oldComm.comm, nnodes, indexPtr, edges_flatPtr, reorder_int, commPtr);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
            }

            AttachToComm();
        }
        #endregion

        #region Accessors
        /// <summary>
        ///   The array of adjacency information. Entry x in Edges[i][j] indicates i is adjacent to x.
        /// </summary>
        public int[][] Edges
        {
            get
            {
                int[][] edges = new int[Size][];
                for (int rank = 0; rank < Size; ++rank)
                    edges[rank] = NeighborsOf(rank);

                return edges;
            }
        }

        /// <summary>
        ///   The number of edges in the communicator.
        /// </summary>
        override public int NumEdges
        {
            get
            {
                int nnodes, nedges;
                int errorCode = Unsafe.MPI_Graphdims_get(comm, out nnodes, out nedges);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                return nedges;
            }
        }

        /// <summary>
        ///   The neighbors of the current process.
        /// </summary>
        override public int[] Neighbors
        {
            get
            {
                return NeighborsOf(Rank);
            }
        }

        /// <summary>
        ///   Retrieve the neighbors of another process.
        /// </summary>
        /// <param name="rank">
        ///   The rank of the process to be queried.
        /// </param>
        /// <returns>
        ///   The neighbors of the process.
        /// </returns>
        override public int[] NeighborsOf(int rank)
        {
            int nneighbors;
            int errorCode = Unsafe.MPI_Graph_neighbors_count(comm, rank, out nneighbors);
            if (errorCode != Unsafe.MPI_SUCCESS)
                throw Environment.TranslateErrorIntoException(errorCode);

            int[] neighbors = new int[nneighbors];
            unsafe
            {
                fixed (int* neighborsPtr = neighbors)
                    errorCode = Unsafe.MPI_Graph_neighbors(comm, rank, nneighbors, neighborsPtr);
            }
            if (errorCode != Unsafe.MPI_SUCCESS)
                throw Environment.TranslateErrorIntoException(errorCode);

            return neighbors;
        }
        #endregion
    }
}
