/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * Contains MPI.NET-specific exception classes.
 */
using System;

namespace MPI
{
    /// <summary>
    /// An exception thrown when an MPI message has been truncated on receive. 
    /// </summary>
    public class MessageTruncatedException : Exception
    {
        /// <summary>
        /// Create a new exception stating that a received message has been truncated.
        /// </summary>
        /// <param name="message">The message associated with this exception.</param>
        public MessageTruncatedException(string message) : base(message) { }
    }
}