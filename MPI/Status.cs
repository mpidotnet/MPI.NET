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
    /// Contains information about a specific message transmitted via MPI.
    /// </summary>
    public class Status
    {
        /// <summary>
        ///   Constructs a <code>Status</code> object from a low-level <see cref="Unsafe.MPI_Status"/> structure.
        /// </summary>
        internal Status(Unsafe.MPI_Status status)
        {
            this.status = status;
        }

        /// <summary>
        /// The rank of the process that sent the message.
        /// </summary>
        public int Source
        {
            get
            {
                return status.MPI_SOURCE;
            }
        }

        /// <summary>
        /// The tag used to send the message.
        /// </summary>
        public int Tag
        {
            get
            {
                return status.MPI_TAG;
            }
        }

        /// <summary>
        /// Determine the number of elements transmitted by the communication
        /// operation associated with this object.
        /// </summary>
        /// <param name="type">
        ///   The type of data that will be stored in the message.
        /// </param>
        /// <returns>
        ///   If the type of the data is a value type, returns the number
        ///   of elements in the message. Otherwise, returns <c>null</c>,
        ///   because the number of elements stored in the message won't
        ///   be known until the message is received.
        /// </returns>
        public int? Count(Type type)
        {
            MPI_Datatype datatype = DatatypeCache.GetDatatype(type);
            if (datatype != Unsafe.MPI_DATATYPE_NULL)
            {
                int count;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Get_count(ref status, datatype, out count);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                return count;
            }
            return null;
        }

        /// <summary>
        /// Whether the communication was cancelled before it completed.
        /// </summary>
        public bool Cancelled
        {
            get
            {
                int flag;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Test_cancelled(ref status, out flag);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                return flag != 0;
            }
        }

        /// <summary>
        ///  The low-level MPI status object.
        /// </summary>
        internal Unsafe.MPI_Status status;
    }

    /// <summary>
    /// Information about a specific message that has already been
    /// transferred via MPI.
    /// </summary>
    public class CompletedStatus : Status
    {
        /// <summary>
        ///   Constructs a <code>Status</code> object from a low-level <see cref="Unsafe.MPI_Status"/> structure
        ///   and a count of the number of elements received.
        /// </summary>
        internal CompletedStatus(Unsafe.MPI_Status status, int count) : base(status)
        {
            this.count = count;
        }

        /// <summary>
        /// Determines the number of elements in the transmitted message.
        /// </summary>
        public int Count()
        {
            return count;
        }

        /// <summary>
        /// The number of elements in the message.
        /// </summary>
        protected int count;
    }
}
