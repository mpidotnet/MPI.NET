/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 */
using System;
using System.Text;

namespace MPI
{
    /// <summary>
    ///   The result of a comparison between two MPI objects.
    /// </summary>
    public enum Comparison : int
    {
        /// <summary>
        /// The two MPI objects are identical.
        /// </summary>
        Identical = Unsafe.MPI_IDENT,
        /// <summary>
        /// The two MPI objects are not identical, but they have the same properties.
        /// For example, two <see cref="Communicator"/>s that contain the same set of
        /// processes with the same ranks.
        /// </summary>
        Congruent = Unsafe.MPI_CONGRUENT,

        /// <summary>
        /// The two MPI objects are similar, but are not identical and do not match
        /// exactly. For example, two <see cref="Communicator"/>s that contain the 
        /// same set of processes but with different rank order.
        /// </summary>
        Similar = Unsafe.MPI_SIMILAR,

        /// <summary>
        /// The two MPI objects are different.
        /// </summary>
        Unequal = Unsafe.MPI_UNEQUAL
    }
}