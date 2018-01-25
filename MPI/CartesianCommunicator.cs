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
using System.Collections.Generic;

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
    ///   A CartesianCommunicator is a form of <see cref="Intracommunicator"/> whose 
    ///   processes are arranged in a grid of arbitrary dimensions.
    /// </summary>
    /// 
    /// <remarks>
    /// Each node in a CartesianCommunicator has 
    /// not only a rank but also coordinates indicating its place in an n-dimensional grid. 
    /// Grids may be specified as periodic (or not) in any dimension, allowing cylinder and 
    /// torus configurations as well as simple grids. Cartesian communicators are often used in
    /// applications where the data is distributed across a logical grid of the processes. These
    /// applications can become both simpler and more efficient due to the use of Cartesian 
    /// communicators, which provide the ability to translate between process coordinates and ranks
    /// and can provide improved placement of MPI processes on the processing elements on the
    /// parallel computer.
    /// </remarks>
    public class CartesianCommunicator : TopologicalCommunicator
    {
        /// <summary>
        ///   Suggests a size for each dimension for a Cartesian topology, given the number of nodes and dimensions.
        /// </summary>
        /// <param name="nnodes">
        ///   The number of nodes in the grid.
        /// </param>
        /// <param name="ndims">
        ///   The number of dimensions.
        /// </param>
        /// <param name="dims">
        ///   An array of size <paramref name="ndims"/> to store the suggested sizes. Nonzero entries will be
        ///   taken as given sizes.
        /// </param>
        public static void ComputeDimensions(int nnodes, int ndims, ref int[] dims)
        {
            unsafe
            {
                fixed (int* dimsPtr = dims)
                {
                    int errorCode = Unsafe.MPI_Dims_create(nnodes, ndims, dimsPtr);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                      throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
            return;
        }

        /// <summary>
        ///   Returns a recommended configuration for a new Cartesian grid.
        /// </summary>
        /// <param name="oldcomm">
        ///   The existing communicator.
        /// </param>
        /// <param name="ndims">
        ///   The number of dimensions for the Cartesian grid.
        /// </param>
        /// <param name="dims">
        ///   An array of length <paramref name="ndims"/> indicating the size of the grid in each dimension.
        /// </param>
        /// <param name="periods">
        ///   A logical array of length <paramref name="ndims"/> indicating whether the grid is periodic in any given dimension.
        /// </param>
        /// <returns>
        ///   The new rank of the calling process.
        /// </returns>
        public static int Map(Intracommunicator oldcomm, int ndims, int[] dims, bool[] periods)
        {
            int newrank;
            int[] periods_int = BoolToInt(periods);
            unsafe
            {
                fixed (int* dimsPtr = dims, periodsPtr = periods_int)
                {
                    int errorCode =Unsafe.MPI_Cart_map(oldcomm.comm, ndims, dimsPtr, periodsPtr, out newrank);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                      throw Environment.TranslateErrorIntoException(errorCode);
                }

            }
            return newrank;
        }

        #region Communicator constructors
        /// <summary>
        /// CartesianCommunicators can only be constructed from other communicators or adopted
        /// from low-level Cartesian communicators.
        /// </summary>
        internal CartesianCommunicator() : base()
        {
            Dims = null;
        }

        /// <summary>
        ///   Creates a <see cref="CartesianCommunicator"/>.
        /// </summary>
        /// <param name="oldComm">
        ///   An existing Intracommunicator from which to create the new communicator (e.g. <see cref="MPI.Communicator.world"/>).
        /// </param>
        /// <param name="ndims">
        ///   The number of dimensions for the Cartesian grid.
        /// </param>
        /// <param name="dims">
        ///   An array of length <paramref name="ndims"/> indicating the size of the grid in each dimension.
        /// </param>
        /// <param name="periods">
        ///   A logical array of length <paramref name="ndims"/> indicating whether the grid is periodic in any given dimension.
        /// </param>
        /// <param name="reorder">
        ///   Logical indicating whether ranks may be reordered or not.
        /// </param>
        public CartesianCommunicator(Intracommunicator oldComm, int ndims, int[] dims, bool[] periods, bool reorder)
        {
            int reorder_int = Convert.ToInt32(reorder);
            int[] periods_int = BoolToInt(periods);
            unsafe
            {
                fixed (int* dimsPtr = dims, periodsPtr = periods_int)
	            fixed (MPI_Comm* commPtr = &(this.comm))
                    {
                        int errorCode = Unsafe.MPI_Cart_create(oldComm.comm, ndims, dimsPtr, periodsPtr, reorder_int, commPtr);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
            }


            NDims = ndims;
            Dims = dims;
            Periods = periods;
            Coords = GetCartesianCoordinates(Rank);

            AttachToComm();
        }

        /// <summary>
        ///   Construct a lower dimensional subgrid from an existing CartesianCommunicator.
        /// </summary>
        /// <param name="remain_dims">
        ///    Logical array with an entry for each dimension indicating whether a dimension 
        ///    should be kept or dropped in the new communicator.
        /// </param>
        /// <returns>
        ///   The new lower-dimensional communicator.
        /// </returns>
        public CartesianCommunicator Subgrid(int[] remain_dims)
        {
            MPI_Comm newComm;

            unsafe
            {
                fixed (int* dimsPtr = remain_dims)
                {
                    int errorCode = Unsafe.MPI_Cart_sub(comm, dimsPtr, &newComm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                      throw Environment.TranslateErrorIntoException(errorCode);
                }
            }

            return (CartesianCommunicator)Communicator.Adopt(newComm);
        }
        #endregion

        #region Communicator management
 
        /// <summary>
        ///   Initializes values for Property accessors.
        /// </summary>
        internal override void AttachToComm()
        {
            base.AttachToComm();

            if (Dims != null)
                return;

            int ndims;
            unsafe
            {
                int errorCode = Unsafe.MPI_Cartdim_get(comm, &ndims);    
                if (errorCode != Unsafe.MPI_SUCCESS)
                  throw Environment.TranslateErrorIntoException(errorCode);
            }

            int[] dims = new int[ndims];
            int[] periods = new int[ndims];
            int[] coords = new int[ndims];
            unsafe
            {
                fixed (int* dimsPtr = dims, periodsPtr = periods, coordsPtr = coords)
                {
                    int errorCode = Unsafe.MPI_Cart_get(comm, ndims, dimsPtr, periodsPtr, coordsPtr);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                      throw Environment.TranslateErrorIntoException(errorCode);
                }
                
            }
            NDims = ndims;
            Dims = dims;
            Periods = IntToBool(periods);
            Coords = coords;
        }
        #endregion

        #region Accessors
        
        /// <summary>
        ///   The size in each dimension of the Cartesian grid.
        /// </summary>
        public int[] Dimensions
        {
            get
            {
                return (int[])Dims.Clone();
            }
        }

        /// <summary>
        ///   Whether the Cartesian grid is periodic in any given dimension.
        /// </summary>
        public bool[] Periodic
        {
            get
            {
                return (bool[])Periods.Clone();
            }
        }

        /// <summary>
        ///   The coordinates of the current process.
        /// </summary>
        public int[] Coordinates
        {
            get
            {
                return (int[])Coords.Clone();
            }
        }

        /// <summary>
        ///   Returns a process' rank given its coordinates in the CartesianCommunicator's grid.
        /// </summary>
        /// <param name="coords">
        ///   An integer array specifying the processes' coordinates.
        /// </param>
        /// <returns>
        ///   The processes' rank in the communicator.
        /// </returns>
        public int GetCartesianRank(int[] coords)
        {
            int rank;
            unsafe
            {
                fixed (int* coordsPtr = coords)
                {
                    int errorCode = Unsafe.MPI_Cart_rank(comm, coordsPtr, &rank);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                      throw Environment.TranslateErrorIntoException(errorCode);
                }

            }
            return rank;
        }

        /// <summary>
        ///   Provides the coordinates in the communicator's grid of a process, given its rank.
        /// </summary>
        /// <param name="rank">
        ///   The processes' rank in the communicator.
        /// </param>
        /// <returns>
        ///   An array of ints giving the coordinates in each dimension.
        /// </returns>
        public int[] GetCartesianCoordinates(int rank)
        {
            int maxdims = NDims;
            int[] coords = new int[maxdims];
            unsafe
            {
                fixed (int* coordsPtr = coords)
                {
                    int errorCode = Unsafe.MPI_Cart_coords(comm, rank, maxdims, coordsPtr);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                      throw Environment.TranslateErrorIntoException(errorCode);
                }

            }
            return coords;
        }

        /// <summary>
        ///   The number of edges in the communicator.
        /// </summary>
        override public int NumEdges
        {
            get
            {
                int nedges = 0;
                nedges = Dims[0] - 1;
                if (Periodic[0] == true)
                    nedges++;
                int nedges_slice;
                int nodes_so_far = Dims[0];
                for (int i = 1; i < NDims; i++) checked
                {
                    nedges_slice = Dims[i] - 1;
                    if (Periodic[i] == true)
                        nedges_slice++;
                    nedges_slice *= nodes_so_far;
                    nodes_so_far *= Dims[i];
                    nedges = nedges * Dims[i] + nedges_slice;
                }

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
        /// <param name="Rank">
        ///   The rank of the process to be queried.
        /// </param>
        /// <returns>
        ///   The neighbors of the process.
        /// </returns>
        override public int[] NeighborsOf(int Rank)
        {
            List<int> neighbors = new List<int>();
            int[] coords = GetCartesianCoordinates(Rank);
            int[] neighbor_coords = new int[coords.Length]; 
            for (int i = 0; i < NDims; i++)
            {
                Array.Copy(coords, neighbor_coords, coords.Length);
                if (coords[i] > 0)
                {
                    neighbor_coords[i] = coords[i] - 1;  
                    neighbors.Add(GetCartesianRank(neighbor_coords));
                }
                else if (Periodic[i] == true && Dims[i] > 2)
                {
                    neighbor_coords[i] = Dims[i] - 1;
                    neighbors.Add(GetCartesianRank(neighbor_coords));
                }
                
                if (coords[i] < Dims[i] - 1)
                {
                    neighbor_coords[i] = coords[i] + 1;
                    neighbors.Add(GetCartesianRank(neighbor_coords));
                }
                else if (Periodic[i] == true && Dims[i] > 2)
                {
                    neighbor_coords[i] = 0;
                    neighbors.Add(GetCartesianRank(neighbor_coords));
                }
            }

            return neighbors.ToArray();
        }

        #endregion

        /// <summary>
        ///   Finds the source and destination ranks necessary to shift data along the grid. 
        /// </summary>
        /// <param name="direction">
        ///   The dimension in which to shift.
        /// </param>
        /// <param name="disp">
        ///   The distance along the grid to shift. Positive values indicate upward shifts,
        ///    negative values downward shifts.
        /// </param>
        /// <param name="rank_source">
        ///   Will contain the rank of the source process.
        /// </param>
        /// <param name="rank_dest">
        ///   Will contain the rank of the destination process.
        /// </param>
        public void Shift(int direction, int disp, out int rank_source, out int rank_dest)
        {
            unsafe
            {
                fixed (int* rank_sourcePtr = &rank_source, rank_destPtr = &rank_dest)
                {
                    int errorCode = Unsafe.MPI_Cart_shift(comm, direction, disp, rank_sourcePtr, rank_destPtr);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
            return;
        }

        #region Protected Members
        /// <summary>
        ///   The number of dimensions of the communicator.
        /// </summary>
        private int NDims;

        /// <summary>
        ///   The dimensions of the communicator.
        /// </summary>
        private int[] Dims;

        /// <summary>
        ///   The peridocity of the communicator in each dimension.
        /// </summary>
        private bool[] Periods;

        /// <summary>
        ///   The coordinates in the communicator of the current process.
        /// </summary>
        private int[] Coords;
        #endregion
    }
}
