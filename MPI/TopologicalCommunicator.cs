/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Ben Martin
 *          Andrew Lumsdaine
 */
namespace MPI
{
    /// <summary>
    ///   A communicator with extra topological information that describes the typical
    ///   communication patterns among the processes.
    /// </summary>
    /// 
    /// <remarks>
    /// This class is abstract, as topological information in MPI is provided either via 
    /// a Cartesian topology (<see cref="CartesianCommunicator"/>) or an arbitrary Graph
    /// topology (<see cref="GraphCommunicator"/>).
    /// </remarks>
    abstract public class TopologicalCommunicator: Intracommunicator
    {
        /// <summary>
        ///   The neighbors of the current process.
        /// </summary>
        abstract public int[] Neighbors { get; }

        /// <summary>
        ///   Retrieve the neighbors of another process.
        /// </summary>
        /// <param name="rank">
        ///   The rank of the process to be queried.
        /// </param>
        /// <returns>
        ///   The neighbors of the process.
        /// </returns>
        abstract public int[] NeighborsOf(int rank);

        /// <summary>
        ///   The number of edges in the communicator.
        /// </summary>
        abstract public int NumEdges { get; }
    }
}
