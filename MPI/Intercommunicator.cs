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
using System.Collections.Generic;
using System.Runtime.InteropServices;

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
    ///   Intercommunicators are <see cref="Communicator"/>s that contain two disjoint groups of 
    ///   processes, an each process can communicate only with processes in the other group.
    /// </summary>
    ///  
    /// <remarks>
    ///   Intercommunicators effectively create a communication pattern that follows a bipartite graph.
    ///   Call the two disjoint groups of processes A and B. Any process in group A can send
    ///   a message to or receive a message from any process in group B, and vice-versa. However, there
    ///   is no way to use an intercommunicator to send messages among the processes within a group.
    ///   Intercommunicators are often useful in large MPI applications that tie together many, smaller
    ///   modules. Typically, each module will have its own intracommunicators and the modules will 
    ///   interact with each other via intercommunicators.
    /// </remarks>
    public class Intercommunicator : Communicator
    {
        #region Communicator constructors      
        /// <summary>
        /// Intercommunicators can only be constructed from other communicators or adopted
        /// from low-level intercommunicators.
        /// </summary>
        internal Intercommunicator() : base()
        {
        }

        /// <summary>
        /// Constructs a new intercommunicator that provides communication between two groups of processes,
        /// specified by intracommunicators. This is a collective operation involving all of the processes
        /// in the communicators passed via <paramref name="localComm"/>.
        /// </summary>
        /// <param name="localComm">
        ///   Intracommunicator whose processes will form the local group in the resulting 
        ///   intercommunication. Since there are two groups in an intercommunicator (each of which is
        ///   identified by an intracommunicator), each process will provide the communicator for its
        ///   own "local" group. Note that a process cannot be in both groups.
        /// </param>
        /// <param name="localLeader">
        ///   The rank of the "leader" in the local group. Each of the processes must provide
        ///   the same leader rank as all of the other processes within its group. 
        /// </param>
        /// <param name="bridgeComm">
        ///   A communicator that permits communication between the leaders of the two groups. 
        ///   This communicator is used internally to construct the intercommunicator, so there must
        ///   not be any pending communication that might interfere with point-to-pointer messages with
        ///   tag <paramref name="tag"/>. This parameter is only significant for the processes
        ///   that are leaders of their group.
        /// </param>
        /// <param name="remoteLeader">
        ///   The rank of the remote group's leader within the communicator <paramref name="bridgeComm"/>.
        /// </param>
        /// <param name="tag">
        ///   A tag value that indicates which intercommunicator is used to transmit setup messages across
        ///   the communicator <paramref name="bridgeComm"/>. Pick a tag value that will not interfere
        ///   with other communicator on <paramref name="bridgeComm"/>.
        /// </param>
        public Intercommunicator(Intracommunicator localComm, int localLeader,
                                 Intracommunicator bridgeComm, int remoteLeader,
                                 int tag) : base()
        {
            unsafe
            {
                int errorCode = Unsafe.MPI_Intercomm_create(localComm.comm, localLeader, bridgeComm.comm, remoteLeader, tag, out comm);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            AttachToComm();
        }
        #endregion

        #region Communicator management
        /// <summary>
        ///   Adopts a low-level MPI intercommunicator that was created with any of the low-level MPI facilities.
        ///   The resulting <c>Intercommunicator</c> object will manage the lifetime of the low-level MPI intercommunicator,
        ///   and will call <see cref="Unsafe.MPI_Comm_free"/> when it is disposed or finalized.
        /// </summary>
        /// <remarks>
        ///   This constructor should only be used in rare cases where the program 
        ///   is manipulating MPI intercommunicators through the low-level MPI interface.
        /// </remarks>
        new public static Intercommunicator Adopt(MPI_Comm comm)
        {
            Communicator result = Communicator.Adopt(comm);
            if (result == null)
                return null;
            else
                return (Intercommunicator)result;
        }

        internal override void AttachToComm()
        {
            base.AttachToComm();

            // Create the "shadow" communicator for sending extra data
            // across the wire.
            unsafe
            {
                int errorCode = Unsafe.MPI_Comm_dup(comm, out shadowComm);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

            }

            // Moved the following line to try to fix https://github.com/jmp75/MPI.NET/issues/2
            // before the unsafe section above, shadowComm == Unsafe.MPI_COMM_NULL, likely cause of the crash.
            Unsafe.MPI_Errhandler_set(shadowComm, Unsafe.MPI_ERRORS_RETURN);
        }

        public override void Dispose()
        {
            // Free any non-predefined communicators
            if (comm != Unsafe.MPI_COMM_SELF && comm != Unsafe.MPI_COMM_WORLD && comm != Unsafe.MPI_COMM_NULL)
            {
                unsafe
                {
                    int errorCode = Unsafe.MPI_Comm_free(ref shadowComm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }

            base.Dispose();
        }

        ~Intercommunicator()
        {
            // Free any non-predefined communicators
            if (comm != Unsafe.MPI_COMM_SELF && comm != Unsafe.MPI_COMM_WORLD && comm != Unsafe.MPI_COMM_NULL)
            {
                if (!Environment.Finalized)
                {
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Comm_free(ref shadowComm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
                shadowComm = Unsafe.MPI_COMM_NULL;
            }
        }

        /// <summary>
        /// Merge all of the processes in both groups of an intercommunicator into a single
        /// intracommunicator. While the intercommunicator only allows communication among processes
        /// in different groups, the merged intracommunicator allows communication among all of the 
        /// processes in either group, regardless of which group the source and target were in.
        /// </summary>
        /// <param name="upper">
        ///   All of the processes within each group should provide the same value. If one group's
        ///   processes provide the value <c>true</c> while the other group's processes provide the value
        ///   <c>false</c>, the group providing the value <c>true</c> will receive the higher-numbered
        ///   ranks in the resulting intracommunicator. If all processes provide the same value, 
        ///   the order of the union is arbitrary.
        /// </param>
        /// <returns>The new intracommunicator.</returns>
        public Intracommunicator Merge(bool upper)
        {
            int high = 0;
            if (upper)
                high = 1;

            MPI_Comm newintracomm;
            unsafe
            {
                int errorCode = Unsafe.MPI_Intercomm_merge(comm, high, out newintracomm);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            return Intracommunicator.Adopt(newintracomm);
        }
        #endregion

        #region Accessors
        /// <summary>
        ///   Returns the number of processes in the remote group of this intercommunicator. 
        /// </summary>
        public int RemoteSize
        {
            get
            {
                int size;
                int errorCode = Unsafe.MPI_Comm_remote_size(comm, out size);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                return size;
            }
        }

        /// <summary>
        /// Retrieve the group containing all of the remote processes in this intercommunicator.
        /// </summary>
        public MPI.Group RemoteGroup
        {
            get
            {
                MPI_Group group;
                int errorCode = Unsafe.MPI_Comm_remote_group(comm, out group);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                return MPI.Group.Adopt(group);
            }
        }

        #endregion

        #region Collective Communication
        /// <summary>
        /// Indicates that this process is the root for a collective operation. The root process should
        /// pass this constant in as the value for root for calls to collective operations.
        /// Other processes in the same group as root should use <see cref="Null">Null</see>. Processes
        /// in the group sending to the root should use the root process' local rank.
        /// </summary>
        public const int Root = MPI.Unsafe.MPI_ROOT;

        /// <summary>
        /// For a collective operation, indicates that this process is the same group as the root but is 
        /// not the root (and thus is not receiving any data). These processes should pass this is as
        /// the root for calls to collective communication procedures. Processes
        /// in the group sending to the root should use the root process' local rank. The root process
        /// should pass in <see cref="Root">Root</see>.
        /// </summary>
        public const int Null = MPI.Unsafe.MPI_PROC_NULL;

        /// <summary>
        /// A tag on the <see cref="shadowComm"/> reserved for communications that are part of collective operations.
        /// </summary>
        internal const int collectiveTag = 0;

        /// <summary>
        /// Secondary low-level MPI communicator that will be used for communication of
        /// data within MPI.NET, such as collectives implemented over point-to-point or
        /// the data message associated with a Send of serialized data.
        /// </summary>
        internal MPI_Comm shadowComm = Unsafe.MPI_COMM_NULL;

        /// <summary>
        /// Gathers individual values provided by each processor from the other group into an array of values in which the
        /// ith element of the array corresponds to the value provided by the processor with rank i.
        /// The resulting array of values is available to all of the processors. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">
        ///   The value supplied by this processor, which will be placed into the element with index <see cref="Communicator.Rank"/>
        ///   in the array sent to processes in the other group.
        /// </param>
        /// <returns> 
        ///   An array of values, where the ith value in the array is a copy of the <paramref name="value"/>
        ///   argument provided by the processor from the other group with rank i.
        /// </returns>
        public T[] Allgather<T>(T value)
        {
            T[] result = null;
            Allgather(value, ref result);
            return result;
        }

        /// <summary>
        /// Gathers individual values provided by each processor from the other group into an array of values in which the
        /// ith element of the array corresponds to the value provided by the processor with rank i.
        /// The resulting array of values is available to all of the processors.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValue">
        ///   The value supplied by this processor, which will be placed into the element with index <see cref="Communicator.Rank"/>
        ///   in the array sent to processes in the other group.
        /// </param>
        /// <param name="outValues">
        ///   An array of values, where the ith value in the array is a copy of the <paramref name="inValue"/>
        ///   argument provided by the processor from the other group with rank i.
        ///   Supply this argument when you have pre-allocated space for the resulting array.
        /// </param>
        public void Allgather<T>(T inValue, ref T[] outValues)
        {
            if (outValues == null || outValues.Length != RemoteSize)
                outValues = new T[RemoteSize];

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                using (UnmanagedMemoryStream inStream = new UnmanagedMemoryStream())
                {
                    // Serialize the data to a stream
                    Serialize(inStream, inValue);

                    // Get the lengths
                    int[] recvLengths = null;
                    int totalRecvLengths = 0;
                    int[] recvDispls = new int[RemoteSize];
                    Allgather(Convert.ToInt32(inStream.Length), ref recvLengths);
                    recvDispls[0] = 0;
                    totalRecvLengths = recvLengths[0];
                    for (int i = 1; i < RemoteSize; i++) checked
                    {
                        recvDispls[i] = recvDispls[i - 1] + recvLengths[i - 1];
                        totalRecvLengths += recvLengths[i];
                    }

                    using (UnmanagedMemoryStream outStream = new UnmanagedMemoryStream(totalRecvLengths))
                    {
                        int errorCode;
                        unsafe
                        {
                            errorCode = Unsafe.MPI_Allgatherv(inStream.Buffer, Convert.ToInt32(inStream.Length), Unsafe.MPI_BYTE,
                                                             outStream.Buffer, recvLengths, recvDispls, Unsafe.MPI_BYTE, comm);
                        }
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);

                        // De-serialize the data
                        for (int source = 0; source < RemoteSize; ++source)
                                outValues[source] = Deserialize<T>(outStream);
                    }
                }
            }
            else
            {
                GCHandle handle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                int errorCode = Unsafe.MPI_Allgather(Memory.LoadAddress(ref inValue), 1, datatype, 
                                                     Marshal.UnsafeAddrOfPinnedArrayElement(outValues, 0), 1, datatype, comm);
                handle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        /// <summary>
        ///   Gathers invidividual values provided by each processor from the other group into an array of values.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The values supplied by this processor.
        /// </param>
        /// <param name="count">
        ///   The number of items to be received by each process in this group.
        /// </param>
        /// <param name="outValues">
        ///   An array of values supplied by all processes of the other group. Supply this argument when 
        ///   you have pre-allocated space for the resulting array.
        /// </param>
        public void AllgatherFlattened<T>(T[] inValues, int count, ref T[] outValues)
        {
            int[] counts = new int[Size];
            for (int i = 0; i < Size; i++)
                counts[i] = count;
            AllgatherFlattened(inValues, counts, ref outValues);
        }

        /// <summary>
        /// Gathers invididual values provided by each processor into an array of values.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The values supplied by this processor.
        /// </param>
        /// <param name="counts">
        ///   The numbers of items to be received by each process in this group.
        /// </param>
        /// <param name="outValues">
        ///   An array of values supplied by all processes. Supply this argument when 
        ///   you have pre-allocated space for the resulting array.
        /// </param>
        public void AllgatherFlattened<T>(T[] inValues, int[] counts, ref T[] outValues)
        {
            if (counts.Length != RemoteSize)
                throw new ArgumentException("counts should be the size of the remote group", "counts");

            int totalCounts = 0;
            for (int i = 0; i < counts.Length; i++) checked
            {
                totalCounts += counts[i];
            }
            if (outValues.Length != totalCounts)
                outValues = new T[totalCounts];
    
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                using (UnmanagedMemoryStream inStream = new UnmanagedMemoryStream())
                {
                    // Serialize the data to a stream
                    for (int source = 0; source < inValues.Length; ++source)
                        Serialize(inStream, inValues[source]);

                    // Get the lengths
                    int[] recvLengths = null;
                    int totalRecvLengths = 0;
                    int[] recvDispls = new int[RemoteSize];
                    Allgather(Convert.ToInt32(inStream.Length), ref recvLengths);
                    recvDispls[0] = 0;
                    totalRecvLengths = recvLengths[0];
                    for (int i = 1; i < RemoteSize; i++) checked
                    {
                        recvDispls[i] = recvDispls[i - 1] + recvLengths[i - 1];
                        totalRecvLengths += recvLengths[i];
                    }

                    using (UnmanagedMemoryStream outStream = new UnmanagedMemoryStream(totalRecvLengths))
                    {
                        int errorCode;
                        unsafe
                        {
                            errorCode = Unsafe.MPI_Allgatherv(inStream.Buffer, Convert.ToInt32(inStream.Length), Unsafe.MPI_BYTE,
                                                             outStream.Buffer, recvLengths, recvDispls, Unsafe.MPI_BYTE, comm);
                        }
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);

                        // De-serialize the data
                        for (int source = 0; source < totalCounts; ++source)
                            outValues[source] = Deserialize<T>(outStream);
                    }
                }
            }
            else
            {
                int size = 0;
                int[] displs = new int[counts.Length];
                displs[0] = 0;
                for (int i = 1; i < counts.Length; i++) checked
                {
                    displs[i] = displs[i - 1] + counts[i - 1];
                    size += counts[i];
                }
                if (outValues == null || outValues.Length != size)
                    outValues = new T[size];

                // Pin the array while we are gathering into it.
                GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                int errorCode = Unsafe.MPI_Allgatherv(inHandle.AddrOfPinnedObject(), inValues.Length, datatype,
                                      outHandle.AddrOfPinnedObject(), counts, displs, datatype, comm);
                inHandle.Free();
                outHandle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        /// <summary>
        ///   <c>Allreduce</c> is a collective algorithm that combines the values supplied by each process of the other group
        ///   into a single value available to all processes. The values are combined in a user-defined way, specified via 
        ///   a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> are the values provided by the 
        ///   N processes in the communicator, the result will be the value <c>value1 op value2 op ... op valueN</c>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">The local value that will be combined with the values provided by other processes
        ///   and sent to each process in the other group.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <returns>The result of the reduction. The same value will be returned to all processes in a group.</returns>
        public T Allreduce<T>(T value, ReductionOperation<T> op)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                T result;
                T[] temp = null;
                temp = Allgather(value);

                result = temp[0];
                for (int i = 1; i < RemoteSize; i++)
                        result = op(result, temp[i]);
                return result;
            }
            else
            {
                T result;
                using (Operation<T> mpiOp = new Operation<T>(op))
                {
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Allreduce(Memory.LoadAddress(ref value), Memory.LoadAddressOfOut(out result),
                                                             1, datatype, mpiOp.Op, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
                return result;
            }
        }

        /// <summary>
        ///   <c>Allreduce</c> is a collective algorithm that combines the values supplied by each process of the other group
        ///   into a single value available to all processes. The values are combined in a user-defined way, specified via 
        ///   a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> are the values provided by the 
        ///   N processes in the communicator, the result will be the value <c>value1 op value2 op ... op valueN</c>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">The local values that will be combined with the values provided by other processes
        ///   and sent to each process in the other group.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <returns>
        ///   The values that result from combining all of the values in <paramref name="values"/> (as provided by the 
        ///   processes of the other group)
        ///   element-by-element.
        /// </returns>
        public T[] Allreduce<T>(T[] values, ReductionOperation<T> op)
        {
            T[] result = null;
            Allreduce(values, op, ref result);
            return result;
        }

        /// <summary>
        ///   <c>Allreduce</c> is a collective algorithm that combines the values supplied by each process of the other group
        ///   into a single value available to all processes. The values are combined in a user-defined way, specified via 
        ///   a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> are the values provided by the 
        ///   N processes in the communicator, the result will be the value <c>value1 op value2 op ... op valueN</c>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The local values that will be combined with the values provided by other processes
        ///   and sent to each process in the other group.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <param name="outValues">
        ///   The values that result from combining all of the values in <paramref name="inValues"/> (as provided by the 
        ///   processes of the other group)
        ///   element-by-element. If needed, this array will be resized to the same size as <paramref name="inValues"/>.
        ///   Supply this argument when you have pre-allocated space for the resulting array.
        /// </param>
        public void Allreduce<T>(T[] inValues, ReductionOperation<T> op, ref T[] outValues)
        {           
            // Make sure the outgoing array is the right size
            if (outValues == null || outValues.Length != inValues.Length)
                outValues = new T[inValues.Length];

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                T[][] temp = null;
                Allgather<T[]>(inValues, ref temp);

                for (int i = 0; i < inValues.Length; i++)
                    outValues[i] = temp[0][i];

                for (int i = 1; i < RemoteSize; i++)
                {
                    for (int j = 0; j < temp[i].Length; j++)
                        outValues[j] = op(outValues[j], temp[i][j]);
                }               
            }
            else
            {
                using (Operation<T> mpiOp = new Operation<T>(op))
                {
                    GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                    GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                    int errorCode;
                    unsafe
                    {
                        errorCode = Unsafe.MPI_Allreduce(Marshal.UnsafeAddrOfPinnedArrayElement(inValues, 0), 
                                                         Marshal.UnsafeAddrOfPinnedArrayElement(outValues, 0),
                                                         inValues.Length, datatype, mpiOp.Op, comm);
                    }
                    inHandle.Free();
                    outHandle.Free();
                    
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }


        /// <summary>
        /// Collective operation in which every process in one group sends data to every process of the other group. 
        /// <c>Alltoall</c> differs from <see cref="Allgather&lt;T&gt;(T)"/> in that a given process can send different
        /// data to all of the other processes, rather than contributing the same piece of data to all
        /// processes. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   The array of values that will be sent to each process in the other group. The ith value in this array
        ///   will be sent to the process with rank i.
        /// </param>
        /// <returns>
        ///   An array of values received from all of the processes in the other group. The jth value in this
        ///   array will be the value sent to the calling process from the process with rank j.
        /// </returns>
        public T[] Alltoall<T>(T[] values)
        {
            T[] result = null;
            Alltoall(values, ref result);
            return result;
        }

        /// <summary>
        /// Collective operation in which every process in one group sends data to every process of the other group. 
        /// <c>Alltoall</c> differs from <see cref="Allgather&lt;T&gt;(T)"/> in that a given process can send different
        /// data to all of the other processes, rather than contributing the same piece of data to all
        /// processes. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The array of values that will be sent to each process. The ith value in this array
        ///   will be sent to the process with rank i.
        /// </param>
        /// <param name="outValues">
        ///   An array of values received from all of the processes in the other group. The jth value in this
        ///   array will be the value sent to the calling process from the process with rank j.
        ///   Supply this argument when you have pre-allocated space for the resulting array.
        /// </param>
        public void Alltoall<T>(T[] inValues, ref T[] outValues)
        {
            // Make sure the outgoing array is the right size
            if (outValues == null || outValues.Length != inValues.Length)
                outValues = new T[inValues.Length];

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                int[] sendCounts = new int[RemoteSize];
                int[] sendOffsets = new int[RemoteSize];

                using (UnmanagedMemoryStream sendStream = new UnmanagedMemoryStream())
                {
                    // Serialize all of the outgoing data to the outgoing stream
                    for (int dest = 0; dest < RemoteSize; ++dest)
                    {
                        sendOffsets[dest] = Convert.ToInt32(sendStream.Length);
                        Serialize(sendStream, inValues[dest]);
                        sendCounts[dest] = checked(Convert.ToInt32(sendStream.Length) - sendOffsets[dest]);
                    }

                    // Use all-to-all on integers to tell every process how much data
                    // it will be receiving.
                    int[] recvCounts = Alltoall(sendCounts);

                    // Compute the offsets at which each of the streams will be received
                    int[] recvOffsets = new int[RemoteSize];
                    recvOffsets[0] = 0;
                    for (int i = 1; i < RemoteSize; ++i) checked
                    {
                        recvOffsets[i] = recvOffsets[i - 1] + recvCounts[i - 1];
                    }

                    // Total length of the receive buffer
                    int recvLength = checked(recvOffsets[RemoteSize - 1] + recvCounts[RemoteSize - 1]);

                    using (UnmanagedMemoryStream recvStream = new UnmanagedMemoryStream(recvLength))
                    {
                        // Build receive buffer and exchange all of the data
                        unsafe
                        {
                            int errorCode = Unsafe.MPI_Alltoallv(sendStream.Buffer, sendCounts, sendOffsets, Unsafe.MPI_BYTE,
                                                                 recvStream.Buffer, recvCounts, recvOffsets, Unsafe.MPI_BYTE, comm);
                            if (errorCode != Unsafe.MPI_SUCCESS)
                                throw Environment.TranslateErrorIntoException(errorCode);
                        }

                        // De-serialize the received data
                        for (int source = 0; source < RemoteSize; ++source)
                        {
                            // Seek to the proper location in the stream and de-serialize
                            recvStream.Position = recvOffsets[source];
                            outValues[source] = Deserialize<T>(recvStream);
                            
                        }
                    }
                }
            }
            else
            {
                GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                int errorCode;
                unsafe
                {
                    errorCode = Unsafe.MPI_Alltoall(Marshal.UnsafeAddrOfPinnedArrayElement(inValues, 0), 1, datatype,
                                                    Marshal.UnsafeAddrOfPinnedArrayElement(outValues, 0), 1, datatype, comm);
                }
                inHandle.Free();
                outHandle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        /// <summary>
        /// Collective operation in which every process in one group sends data to every process of the other group. 
        /// <c>Alltoall</c> differs from <see cref="Allgather&lt;T&gt;(T)"/> in that a given process can send different
        /// data to all of the other processes, rather than contributing the same piece of data to all
        /// processes.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The array of values that will be sent to each process. <paramref name="sendCounts">sendCounts</paramref>[i]
        ///   worth of data will be sent to process i.
        /// </param>
        /// <param name="sendCounts">
        ///   The numbers of items to be sent to each process in the other group.
        /// </param>
        /// <param name="recvCounts">
        ///   The numbers of items to be received by each process in this group.
        /// </param>
        /// <param name="outValues">
        ///   The array of values received from all of the other processes.
        /// </param>
        public void AlltoallFlattened<T>(T[] inValues, int[] sendCounts, int[] recvCounts, ref T[] outValues)
        {
            if (sendCounts.Length != RemoteSize)
                throw new ArgumentException("sendCounts should be the size of the remote group", "sendCounts");
            if (recvCounts.Length != RemoteSize)
                throw new ArgumentException("recvCounts should be the size of the remote group", "recvCounts");

            int totalCounts = 0;
            for (int i = 0; i < recvCounts.Length; i++) checked
            {
                totalCounts += recvCounts[i];
            }
            // Make sure the outgoing array is the right size
            if (outValues == null || outValues.Length != totalCounts)
                outValues = new T[totalCounts];

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                T[] temp;
                int currentLocation;
 
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                int[] sendCountsSerialized = new int[RemoteSize];
                int[] sendOffsets = new int[RemoteSize];

                using (UnmanagedMemoryStream sendStream = new UnmanagedMemoryStream())
                {
                    currentLocation = 0;
                    // Serialize all of the outgoing data to the outgoing stream
                    for (int dest = 0; dest < RemoteSize; ++dest) checked
                    {
                        sendOffsets[dest] = Convert.ToInt32(sendStream.Length);
                        temp = new T[sendCounts[dest]];
                        Array.Copy(inValues, currentLocation, temp, 0, sendCounts[dest]);
                        currentLocation += sendCounts[dest];
                        Serialize(sendStream, temp);

                        sendCountsSerialized[dest] = Convert.ToInt32(sendStream.Length) - sendOffsets[dest];
                    }

                    // Use all-to-all on integers to tell every process how much data
                    // it will be receiving.
                    int[] recvCountsSerialized = Alltoall(sendCountsSerialized);

                    // Compute the offsets at which each of the streams will be received
                    int[] recvOffsets = new int[RemoteSize];
                    recvOffsets[0] = 0;
                    for (int i = 1; i < RemoteSize; ++i) checked
                    {
                        recvOffsets[i] = recvOffsets[i - 1] + recvCountsSerialized[i - 1];
                    }

                    // Total length of the receive buffer
                    int recvLength = checked(recvOffsets[RemoteSize - 1] + recvCountsSerialized[RemoteSize - 1]);

                    using (UnmanagedMemoryStream recvStream = new UnmanagedMemoryStream(recvLength))
                    {
                        // Build receive buffer and exchange all of the data
                        unsafe
                        {
                            int errorCode = Unsafe.MPI_Alltoallv(sendStream.Buffer, sendCountsSerialized, sendOffsets, Unsafe.MPI_BYTE,
                                                                 recvStream.Buffer, recvCountsSerialized, recvOffsets, Unsafe.MPI_BYTE, comm);
                            if (errorCode != Unsafe.MPI_SUCCESS)
                                throw Environment.TranslateErrorIntoException(errorCode);
                        }

                        // De-serialize the received data
                        currentLocation = 0;
                        for (int source = 0; source < RemoteSize; ++source) checked
                        {
                            // Seek to the proper location in the stream and de-serialize
                            recvStream.Position = recvOffsets[source];
                            temp = Deserialize<T[]>(recvStream);
                            Array.Copy(temp, 0, outValues, currentLocation, temp.Length);
                            currentLocation += temp.Length;
                        }
                    }
                }
            }
            else
            {
                int[] sendDispls = new int[RemoteSize];
                int[] recvDispls = new int[RemoteSize];
                sendDispls[0] = 0;
                recvDispls[0] = 0;
                for (int i = 1; i < RemoteSize; i++) checked
                {
                    sendDispls[i] = sendDispls[i - 1] + sendCounts[i - 1];
                    recvDispls[i] = recvDispls[i - 1] + recvCounts[i - 1];
                }

                GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                int errorCode;
                unsafe
                {
                    errorCode = Unsafe.MPI_Alltoallv(Marshal.UnsafeAddrOfPinnedArrayElement(inValues, 0), sendCounts, sendDispls, datatype,
                                                    Marshal.UnsafeAddrOfPinnedArrayElement(outValues, 0), recvCounts, recvDispls, datatype, comm);
                }
                inHandle.Free();
                outHandle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        /// <summary>
        /// Broadcast a value from the <paramref name="root"/> process to all processes in the other group.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">
        ///   The value to be broadcast. At the <paramref name="root"/> process, this value is
        ///   read (but not written). At leaf group processes, this value will be replaced with
        ///   the value at the root. At non-root processes in the root group, this value is not read or written.
        /// </param>
        /// <param name="root">
        ///   The rank of the process that is broadcasting the value out to
        ///   all of the processes in the non-root group.
        /// </param>
        public void Broadcast<T>(ref T value, int root)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL && root == Null)
            {
                unsafe
                {
                    int length = 0;
                    int errorCode = Unsafe.MPI_Bcast(new IntPtr(&length), 1, Unsafe.MPI_INT, Null, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                    errorCode = Unsafe.MPI_Bcast(new IntPtr(0), 0, Unsafe.MPI_BYTE, Null, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
            else 
                Broadcast_impl<T>((root == Root), ref value, root);

        }

        /// <summary>
        /// Broadcast an array from the <paramref name="root"/> process to all processes in the other group.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   The array of values to be broadcast. At the <paramref name="root"/> process, this value is
        ///   read (but not written); at processes in the leaf group, this value will be replaced with
        ///   the value at the root; and at non-root root group processes, this value is not read or written.
        ///   Note that the receiving processes must already have allocated
        ///   enough space in the array to receive data from the root, prior to calling <c>Broadcast</c>.
        /// </param>
        /// <param name="root">
        ///   The rank of the process that is broadcasting the values out to
        ///   all of the processes in the non-root group.
        /// </param>
        public void Broadcast<T>(ref T[] values, int root)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL && root == Null)
            {
                unsafe
                {
                    int length = 0;
                    int errorCode = Unsafe.MPI_Bcast(new IntPtr(&length), 1, Unsafe.MPI_INT, Null, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                    errorCode = Unsafe.MPI_Bcast(new IntPtr(0), 0, Unsafe.MPI_BYTE, Null, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
            else if (root == Null)
            {
                unsafe
                {
                    int errorCode = Unsafe.MPI_Bcast(new IntPtr(0), 0, datatype, Null, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
            else
                Broadcast_impl<T>((root == Root), ref values, root);            
        }

        /// <summary>
        ///   Gather the values from each process in the non-root group into an array of values at the 
        ///   <paramref name="root"/> process. On the root process, the pth element of the result
        ///   will be equal to the <paramref name="value"/> parameter of the process
        ///   with rank <c>p</c> in the other group when this routine returns.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">The value contributed by this process. Only significant at non-root group processes.</param>
        /// <param name="root">
        ///   Used to indicate the process gathering the data.
        ///   At the root, should be <see cref="Intercommunicator.Root"/>. At leaf group processes
        ///   should be the rank of the root process in the root group. At non-root processes in the root group,
        ///   should be <see cref="Intercommunicator.Null"/>.
        /// </param>
        /// <returns>
        ///   At the root, an array containing the <paramref name="value"/>s supplied by each of the processes
        ///   in the non-root group. All other processes receive <c>null</c>.
        /// </returns>
        public T[] Gather<T>(T value, int root)
        {
            T[] result = null;
            Gather(value, root, ref result);
            return result;
        }

        /// <summary>
        ///   Gather the values from each process in the non-root group into an array of values at the 
        ///   <paramref name="root"/> process. On the root process, the pth element of the result
        ///   will be equal to the <paramref name="inValue"/> parameter of the process
        ///   with rank <c>p</c> in the other group when this routine returns. Use this variant of the routine 
        ///   when you want to pre-allocate storage for the <paramref name="outValues"/> array.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValue">The value contributed by this process. Only significant at processes 
        ///   in the leaf (non-root) group.
        /// </param>
        /// <param name="root">
        ///   Used to indicate the process gathering the data.
        ///   At the root, should be <see cref="Intercommunicator.Root"/>. At leaf group processes
        ///   should be the rank of the root process in the root group. At non-root processes in the root group,
        ///   should be <see cref="Intercommunicator.Null"/>.
        /// </param>
        /// <param name="outValues">
        ///   An array that will store the values contributed by each process.
        ///   This value is only significant at the <paramref name="root"/>, and
        ///   can be omitted by non-root processes.
        ///   Supply this argument when you have pre-allocated space for the resulting array.
        /// </param>
        public void Gather<T>(T inValue, int root, ref T[] outValues)
        {
            Gather_impl<T>((root == Root), RemoteSize, inValue, root, ref outValues);
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        ///   Use this variant at the root process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="count">
        ///   The number of elements contributed by each process of the leaf group.
        /// </param>
        /// <param name="outValues">
        ///   An array in which to store the aggregated, gathered values.  If null or too short, a new array will be allocated.
        /// </param>
        public void GatherFlattened<T>(int count, ref T[] outValues)
        {
            if (outValues == null || outValues.Length < checked(count * RemoteSize))
                outValues = new T[checked(count * RemoteSize)];


            int[] counts = new int[Size];
            for (int i = 0; i < Size; i++)
                counts[i] = count;

            GatherFlattened_impl<T>(true, RemoteSize, new T[0], counts, Root, ref outValues);
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        ///   Use this variant at the root process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="count">
        ///   The number of elements contributed by each process of the leaf group.
        /// </param>
        public T[] GatherFlattened<T>(int count)
        {
            T[] outValues = new T[checked(RemoteSize * count)];

            int[] counts = new int[Size];
            for (int i = 0; i < Size; i++)
                counts[i] = count;

            GatherFlattened_impl<T>(true, RemoteSize, new T[0], counts, Root, ref outValues);

            return outValues;
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        ///   Use this variant at leaf group processes.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The values to be gathered.</param>
        /// <param name="root">
        ///   Used to indicate the process gathering the data.
        ///   At the root, should be <see cref="Intercommunicator.Root"/>. At leaf group processes
        ///   should be the rank of the root process in the root group. At non-root processes in the root group,
        ///   should be <see cref="Intercommunicator.Null"/>.
        /// </param>
        public void GatherFlattened<T>(T[] inValues, int root)
        {
            T[] temp = new T[0];
            GatherFlattened_impl<T>(false, Size, inValues, new int[0], root, ref temp);
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        ///   Use this variant at non-root processes in the root group.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        public void GatherFlattened<T>()
        {
            T[] temp = new T[0];
            GatherFlattened_impl<T>(false, RemoteSize, new T[0], new int[0], Null, ref temp);
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        ///   Use this variant at the root process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="counts">
        ///   The number of elements to be received from each process in the leaf group.
        /// </param>
        /// <param name="outValues">
        ///   The aggregated, gathered values. Supply this parameter if you have preallocated space for the operation.
        /// </param>
        public void GatherFlattened<T>(int[] counts, ref T[] outValues)
        {
            GatherFlattened_impl<T>(true, RemoteSize, new T[0], counts, Root, ref outValues);
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        ///   Use this variant at the root process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="counts">
        ///   The number of elements to be received from each process in the leaf group.
        /// </param>
        public T[] GatherFlattened<T>(int[] counts)
        {           
            int totalCounts = 0;
            for (int i = 0; i < counts.Length; i++) checked
            {
                totalCounts += counts[i];
            }
            T[] outValues = new T[totalCounts];

            GatherFlattened_impl<T>(true, RemoteSize, new T[0], counts, Root, ref outValues);
            
            return outValues;
        }

        /// <summary>
        ///   <c>Reduce</c> is a collective algorithm that combines the values supplied by each 
        ///   process in the leaf group into a 
        ///   single value available at the designated <paramref name="root"/> process. The values are combined 
        ///   in a user-defined way, specified via a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> 
        ///   are the values provided by the N processes in the communicator, the result will be the value 
        ///   <c>value1 op value2 op ... op valueN</c>. This result is only
        ///   available to the <paramref name="root"/> process. If all processes need the result of the reduction,
        ///   use <see cref="Intercommunicator.Allreduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;)"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">The local value that will be combined with the values provided by other leaf group processes.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <param name="root">
        ///   The rank of the process that is the root of the reduction operation, which will receive the result
        ///   of the reduction operation.
        /// </param>
        /// <returns>
        ///   On the root, returns the result of the reduction operation. The other processes receive a default value.
        /// </returns>
        public T Reduce<T>(T value, ReductionOperation<T> op, int root)
        {
            return Reduce_impl<T>((root == Root), RemoteSize, value, op, root);
        }

        /// <summary>
        ///   <c>Reduce</c> is a collective algorithm that combines the values supplied by each 
        ///   process in the leaf group into a 
        ///   single value available at the designated <paramref name="root"/> process. This particular variant of
        ///   <see cref="Reduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;, int)"/> applies to each of the elements
        ///   of the provided arrays. Each process must provide arrays of the same length, and the values at each
        ///   array index are combined 
        ///   in a user-defined way, specified via a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> 
        ///   are the ith values provided by the N processes in the communicator, the ith result will be the value 
        ///   <c>value1 op value2 op ... op valueN</c>. The resulting array is only
        ///   available to the <paramref name="root"/> process. If all processes need the result of the reduction,
        ///   use <see cref="Intercommunicator.Allreduce&lt;T&gt;(T[], ReductionOperation&lt;T&gt;, ref T[])"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">The local values that will be combined with the values provided by other leaf group processes.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <param name="root">
        ///   On leaf group processes, the rank of the process that is the root of the reduction operation, to which this routine will
        ///   return the result of the reduction operation. On the root process, should be <see cref="Root"/>. On non-root processes 
        ///   in the root group, should be <see cref="Null"/>.
        /// </param>
        /// <returns>
        ///   On the root, an array that contains the result of elementwise reduction on the <paramref name="values"/>
        ///   arrays provided by each process. On all other processes, <c>null</c>.
        /// </returns>
        public T[] Reduce<T>(T[] values, ReductionOperation<T> op, int root)
        {
            T[] result = null;
            Reduce(values, op, root, ref result);
            return result;
        }

        /// <summary>
        ///   <c>Reduce</c> is a collective algorithm that combines the values supplied by each 
        ///   process in the leaf group into a 
        ///   single value available at the designated <paramref name="root"/> process. This particular variant of
        ///   <see cref="Reduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;, int)"/> applies to each of the elements
        ///   of the provided arrays. Each process must provide arrays of the same length, and the values at each
        ///   array index are combined 
        ///   in a user-defined way, specified via a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> 
        ///   are the ith values provided by the N processes in the communicator, the ith result will be the value 
        ///   <c>value1 op value2 op ... op valueN</c>. The resulting array is only
        ///   available to the <paramref name="root"/> process. If all processes need the result of the reduction,
        ///   use <see cref="Intercommunicator.Allreduce&lt;T&gt;(T[], ReductionOperation&lt;T&gt;, ref T[])"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The local values that will be combined with the values provided by other leaf group processes.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <param name="root">
        ///   At leaf group processes, the rank of the process that is the root of the reduction operation, which will receive the result
        ///   of the reduction operation in its <paramref name="outValues"/> argument.
        ///   At the root, should be <see cref="Intercommunicator.Root"/>. At non-root processes in the root group,
        ///   should be <see cref="Intercommunicator.Null"/>.
        /// </param>
        /// <param name="outValues">
        ///   The variable that will receive the results of the reduction operation. Only the <paramref name="root"/>
        ///   process will receive the actual result; all other processes will receive a default-initialized value.
        ///   When this collective operation returns, <paramref name="outValues"/> will be of the same length as
        ///   <paramref name="inValues"/>.
        /// </param>
        public void Reduce<T>(T[] inValues, ReductionOperation<T> op, int root, ref T[] outValues)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (root != Root)
            {
                if (datatype == Unsafe.MPI_DATATYPE_NULL)
                {
                    if (inValues == null)
                        inValues = new T[0];

                    // Gather values at the root
                    Gather(inValues, root);
                }
                else
                {
                    // Use the low-level MPI reduction operation from a non-root
                    using (Operation<T> mpiOp = new Operation<T>(op))
                    {
                        int errorCode;
                        if (inValues != null)
                        {
                            GCHandle handle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                            unsafe
                            {
                                errorCode = Unsafe.MPI_Reduce(Marshal.UnsafeAddrOfPinnedArrayElement(inValues, 0), new IntPtr(0),
                                                  inValues.Length, datatype, mpiOp.Op, root, comm);
                            }
                            handle.Free();
                        }
                        else
                        {
                            // For some reason, passing in new IntPtr(0) for both inValues and outValues did not work on Windows,
                            // so to let users continue to pass in null as they would expect, we'll use an empty array if we have to
                            inValues = new T[0];
                            GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                            unsafe
                            {
                                errorCode = Unsafe.MPI_Reduce(Marshal.UnsafeAddrOfPinnedArrayElement(inValues, 0), new IntPtr(0),
                                                  0, datatype, mpiOp.Op, root, comm);
                            }
                            inHandle.Free();
                            
                        }
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
            }
            else
            {
                if (outValues == null || outValues.Length == 0)
                    throw new ArgumentException("outValues array must be preallocated at the root", "outValues");

                // Make sure the resulting array is long enough
                //if (outValues == null || outValues.Length != inValues.Length)
                //    outValues = new T[inValues.Length];

                if (datatype == Unsafe.MPI_DATATYPE_NULL)
                {
                    // Gather into a temporary array
                    T[][] values = new T[RemoteSize][];
                    Gather(inValues, root, ref values);

                    // Perform reduction locally
                    for (int i = 0; i < outValues.Length; ++i)
                    {
                        outValues[i] = values[0][i];
                        for (int p = 1; p < RemoteSize; ++p)
                            outValues[i] = op(outValues[i], values[p][i]);
                    }
                }
                else
                {
                    // Use the low-level MPI reduction operation from the root
                    using (Operation<T> mpiOp = new Operation<T>(op))
                    {
                        GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                        int errorCode;
                        unsafe
                        {
                            errorCode = Unsafe.MPI_Reduce(new IntPtr(0),
                                                          Marshal.UnsafeAddrOfPinnedArrayElement(outValues, 0),
                                                          outValues.Length, datatype, mpiOp.Op, root, comm);
                        }
                        outHandle.Free();

                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
            }
        }

        /// <summary>
        /// A collective operation that first performs a reduction on the given <paramref name="values"/> 
        /// (see <see cref="Intracommunicator.Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int)"/> from one group
        /// and then scatters the results by sending some elements to each process of the other group. 
        /// The reduction will be performed on the entire array of <paramref name="values"/> (like the array form of 
        /// <see cref="Intracommunicator.Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int)"/>). Then, the array will
        /// be scattered, with process i receiving <paramref name="counts"/>[i] elements. The process
        /// with rank 0 will receive the first <c>counts[0]</c> elements, the process with rank 1 will 
        /// receive the next <c>counts[1]</c> elements, and so on.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   An array of values that will be reduced. This array must be the same length at every process within a group.
        /// </param>
        /// <param name="op">
        ///   The operation used to combine the elements in <paramref name="values"/>.
        ///   This operation must be associative.
        /// </param>
        /// <param name="counts">
        ///   An array whose ith element states the number of elements from the reduced result 
        ///   that will be returned to the process with rank i. This array should be the same at every
        ///   process within a group (though the count values within the array need not be the same).
        ///   The sum of all counts for one group must be the same as the sume of all counts for the other group.
        /// </param>
        /// <returns>
        ///   An array of length <c>counts[Rank]</c> containing the reduced results destined for
        ///   the calling process.
        /// </returns>
        public T[] ReduceScatter<T>(T[] values, ReductionOperation<T> op, int[] counts)
        {
            T[] result = null;
            ReduceScatter(values, op, counts, ref result);
            return result;
        }

        /// <summary>
        /// A collective operation that first performs a reduction on the given <paramref name="inValues"/> 
        /// (see <see cref="Intracommunicator.Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int)"/> from one group
        /// and then scatters the results by sending some elements to each process of the other group. 
        /// The reduction will be performed on the entire array of <paramref name="inValues"/> (like the array form of 
        /// <see cref="Intracommunicator.Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int)"/>). Then, the array will
        /// be scattered, with process i receiving <paramref name="counts"/>[i] elements. The process
        /// with rank 0 will receive the first <c>counts[0]</c> elements, the process with rank 1 will 
        /// receive the next <c>counts[1]</c> elements, and so on.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   An array of values that will be reduced. This array must be the same length at every process within a group.
        /// </param>
        /// <param name="op">
        ///   The operation used to combine the elements in <paramref name="inValues"/>.
        ///   This operation must be associative.
        /// </param>
        /// <param name="counts">
        ///   An array whose ith element states the number of elements from the reduced result 
        ///   that will be returned to the process with rank i. This array should be the same at every
        ///   process within a group (though the count values within the array need not be the same).
        ///   The sum of all counts for one group must be the same as the sume of all counts for the other group.
        /// </param>
        /// <param name="outValues">
        ///   An array of length <c>counts[Rank]</c> containing the reduced results destined for
        ///   the calling process.
        /// </param>
        public void ReduceScatter<T>(T[] inValues, ReductionOperation<T> op, int[] counts, ref T[] outValues)
        {            
            // Make sure the outgoing array is the right size
            if (outValues == null || outValues.Length != counts[Rank])
                outValues = new T[counts[Rank]];

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                Unsafe.MPI_Status mpiStatus;
                MPI_Request mpiRequest;
                int recvCount;
                int errorCode;

                if (Rank == 0)
                {
                    // First figure out how much data we need to store
                    int totalCounts = 0;
                    for (int i = 0; i < Size; i++) checked
                    {
                        totalCounts += counts[i];
                    }

                    // Next we need to know the counts on the remote group
                    int[] remoteCounts = new int[RemoteSize];
                    unsafe
                    {
                        IntPtr inPtr = Marshal.UnsafeAddrOfPinnedArrayElement(counts, 0);
                        IntPtr outPtr = Marshal.UnsafeAddrOfPinnedArrayElement(remoteCounts, 0);
                        errorCode = Unsafe.MPI_Sendrecv(inPtr, counts.Length, Unsafe.MPI_INT, 0, collectiveTag,
                                                        outPtr, remoteCounts.Length, Unsafe.MPI_INT, 0, collectiveTag, shadowComm, out mpiStatus);
                    }

                    T[][] values = new T[RemoteSize][]; // for holding received values
                    T[] accValues = new T[totalCounts]; // for holding accumulated values
                    T[] remoteAccValues; // for holding received accumulated values
                    for (int i = 0; i < RemoteSize; i++)
                        values[i] = null;
                    T[][] sendValues = new T[RemoteSize][]; // for holding values to send; rearranged remoteAccValues
                    for (int i = 0; i < RemoteSize; i++)
                        sendValues[i] = new T[remoteCounts[i]];

                    // Get data from other root first
                    values[0] = new T[totalCounts];
                    using (UnmanagedMemoryStream sendStream = new UnmanagedMemoryStream())
                    {

                        Serialize(sendStream, inValues);
                        unsafe
                        {
                            errorCode = Unsafe.MPI_Isend(sendStream.Buffer, Convert.ToInt32(sendStream.Length), Unsafe.MPI_BYTE, 0, collectiveTag, shadowComm, out mpiRequest);
                            if (errorCode != Unsafe.MPI_SUCCESS)
                                throw Environment.TranslateErrorIntoException(errorCode);
                        }
                    }
                    Unsafe.MPI_Probe(0, collectiveTag, shadowComm, out mpiStatus);
                    errorCode = Unsafe.MPI_Get_count(ref mpiStatus, Unsafe.MPI_BYTE, out recvCount);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                    using (UnmanagedMemoryStream recvStream = new UnmanagedMemoryStream(recvCount))
                    {
                        unsafe
                        {
                            errorCode = Unsafe.MPI_Recv(recvStream.Buffer, recvCount, Unsafe.MPI_BYTE, 0, collectiveTag, shadowComm, out mpiStatus);
                        }
                        values[0] = Deserialize<T[]>(recvStream);
                        Unsafe.MPI_Wait(ref mpiRequest, out mpiStatus);
                    }
                    for (int j = 0; j < totalCounts; j++)
                        accValues[j] = op(accValues[j], values[0][j]);

                    // Now get data from all other remote processes
                    // Interleave receives and reduction, so we don't wait
                    for (int i = 1; i < RemoteSize; i++)
                    {
                        values[i] = new T[totalCounts];
                        Unsafe.MPI_Probe(i, collectiveTag, shadowComm, out mpiStatus);
                        errorCode = Unsafe.MPI_Get_count(ref mpiStatus, Unsafe.MPI_BYTE, out recvCount);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                        using (UnmanagedMemoryStream recvStream = new UnmanagedMemoryStream(recvCount))
                        {
                            unsafe
                            {
                                errorCode = Unsafe.MPI_Recv(recvStream.Buffer, recvCount, Unsafe.MPI_BYTE, i, collectiveTag, shadowComm, out mpiStatus);
                            }
                            values[i] = Deserialize<T[]>(recvStream);
                        }
                        Unsafe.MPI_Wait(ref mpiRequest, out mpiStatus);

                        for (int j = 0; j < totalCounts; j++)
                            accValues[j] = op(accValues[j], values[i][j]);
                    }

                    // Now we need to exhange data with the other root, 
                    // so that it can send this data to our group
                    remoteAccValues = new T[totalCounts]; // totalCounts should be same as totaled remoteCounts - SHOULD
                    using (UnmanagedMemoryStream sendStream = new UnmanagedMemoryStream())
                    {

                        Serialize(sendStream, accValues);
                        unsafe
                        {
                            errorCode = Unsafe.MPI_Isend(sendStream.Buffer, Convert.ToInt32(sendStream.Length), Unsafe.MPI_BYTE, 0, collectiveTag, shadowComm, out mpiRequest);
                            if (errorCode != Unsafe.MPI_SUCCESS)
                                throw Environment.TranslateErrorIntoException(errorCode);
                        }
                    }
                    Unsafe.MPI_Probe(0, collectiveTag, shadowComm, out mpiStatus);
                    errorCode = Unsafe.MPI_Get_count(ref mpiStatus, Unsafe.MPI_BYTE, out recvCount);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                    using (UnmanagedMemoryStream recvStream = new UnmanagedMemoryStream(recvCount))
                    {
                        unsafe
                        {
                            errorCode = Unsafe.MPI_Recv(recvStream.Buffer, recvCount, Unsafe.MPI_BYTE, 0, collectiveTag, shadowComm, out mpiStatus);
                        }
                        remoteAccValues = Deserialize<T[]>(recvStream);
                        Unsafe.MPI_Wait(ref mpiRequest, out mpiStatus);
                    }

                    // Rearrange remoteAccValues into a shape that's more useful for sending
                    int currentPos = 0;
                    for (int i = 0; i < remoteCounts.Length; i++) checked
                    {
                        System.Array.Copy(remoteAccValues, currentPos, sendValues[i], 0, remoteCounts[i]);
                        currentPos += remoteCounts[i];
                    }

                    // Skip sending to save on communications
                    System.Array.Copy(accValues, 0, outValues, 0, counts[0]);


                    for (int i = 1; i < RemoteSize; i++)
                    {
                        using (UnmanagedMemoryStream sendStream = new UnmanagedMemoryStream())
                        {
                            Serialize(sendStream, sendValues[i]);
                            unsafe
                            {
                                errorCode = Unsafe.MPI_Send(sendStream.Buffer, Convert.ToInt32(sendStream.Length), Unsafe.MPI_BYTE, i, collectiveTag, shadowComm);
                                if (errorCode != Unsafe.MPI_SUCCESS)
                                    throw Environment.TranslateErrorIntoException(errorCode);
                            }
                        }

                    }
                }
                else
                {
                    using (UnmanagedMemoryStream sendStream = new UnmanagedMemoryStream())
                    {
                        Serialize(sendStream, inValues);
                        unsafe
                        {
                            errorCode = Unsafe.MPI_Send(sendStream.Buffer, Convert.ToInt32(sendStream.Length), Unsafe.MPI_BYTE, 0, collectiveTag, shadowComm);
                            if (errorCode != Unsafe.MPI_SUCCESS)
                                throw Environment.TranslateErrorIntoException(errorCode);
                        }
                    }
                    Unsafe.MPI_Probe(0, collectiveTag, shadowComm, out mpiStatus);
                    errorCode = Unsafe.MPI_Get_count(ref mpiStatus, Unsafe.MPI_BYTE, out recvCount);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                    using (UnmanagedMemoryStream recvStream = new UnmanagedMemoryStream(recvCount))
                    {
                        unsafe
                        {
                            errorCode = Unsafe.MPI_Recv(recvStream.Buffer, recvCount, Unsafe.MPI_BYTE, 0, collectiveTag, shadowComm, out mpiStatus);
                        }
                        outValues = Deserialize<T[]>(recvStream);
                    }
                }
            
            }
            else
            {
                // Use the low-level MPI reduce-scatter operation from the root
                using (Operation<T> mpiOp = new Operation<T>(op))
                {
                    GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                    GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                    int errorCode;
                    unsafe
                    {
                        errorCode = Unsafe.MPI_Reduce_scatter(Marshal.UnsafeAddrOfPinnedArrayElement(inValues, 0),
                                                                  Marshal.UnsafeAddrOfPinnedArrayElement(outValues, 0),
                                                                  counts, datatype, mpiOp.Op, comm);
                    }
                    inHandle.Free();
                    outHandle.Free();

                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        /// Scatters an array of values by sending the ith value of the array to processor i of the other group. 
        /// This variant of <c>Scatter</c> can only be called by the root process. Other processes
        /// should call the non-root variant of <see cref="Scatter&lt;T&gt;(int)"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   An array of values of length <see cref="RemoteSize"/>. 
        ///   The ith value of this array (at the root process) will be sent to the ith processor of the other group.
        /// </param>
        public void Scatter<T>(T[] values)
        {
            if ((values == null || values.Length != RemoteSize))
            {
                throw new ArgumentException("MPI.Intercommunicator.Scatter: values must contain one value for each process.");
            }

            
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the values for transmission.
                using (UnmanagedMemoryStream stream = new UnmanagedMemoryStream())
                {
                    int[] counts = new int[RemoteSize];
                    int[] offsets = new int[RemoteSize];

                    for (int dest = 0; dest < RemoteSize; ++dest)
                    {
                        // Serialize this value to the stream
                        offsets[dest] = Convert.ToInt32(stream.Length);
                        Serialize(stream, values[dest]);
                        counts[dest] = checked(Convert.ToInt32(stream.Length) - offsets[dest]);
                    }

                    // Scatter the byte counts
                    Scatter(counts);

                    // Scatter the data
                    unsafe
                    {
                        int errorCode = MPI.Unsafe.MPI_Scatterv(stream.Buffer, counts, offsets, Unsafe.MPI_BYTE,
                                                                new IntPtr(), 0, Unsafe.MPI_BYTE, Root, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }

            }
            else
            {
                unsafe
                {
                    GCHandle handle = GCHandle.Alloc(values, GCHandleType.Pinned);
                    int errorCode = Unsafe.MPI_Scatter(Marshal.UnsafeAddrOfPinnedArrayElement(values, 0), 1, datatype,
                                                       new IntPtr(0), 0, datatype, Root, comm);
                    handle.Free();
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        /// Scatters an array of values by sending the ith value of the array to processor i. 
        /// This variant of <c>Scatter</c> can only be called by a leaf group process. The root process
        /// should call the root-only variant of <see cref="Scatter&lt;T&gt;(T[])"/>. Other 
        /// root group processes should call <see cref="Scatter&lt;T&gt;()"/>
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="root">
        ///   Rank of the "root" process, which will supply the array of values to be scattered.
        /// </param>
        /// <returns>
        /// </returns>
        public T Scatter<T>(int root)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // The number of serialized bytes we'll receive
                int bytes = Scatter<int>(root);

                using (UnmanagedMemoryStream stream = new UnmanagedMemoryStream(bytes))
                {
                    // Receive serialized data
                    unsafe
                    {
                        int errorCode = MPI.Unsafe.MPI_Scatterv(new IntPtr(), null, null, Unsafe.MPI_BYTE,
                                                                stream.Buffer, bytes, Unsafe.MPI_BYTE, root, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }

                    return Deserialize<T>(stream);
                }
            }
            else
            {
                T result;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Scatter(new IntPtr(0), 0, datatype, Memory.LoadAddressOfOut(out result), 1, datatype, root, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                return result;
            }
        }

        /// <summary>
        /// Scatters an array of values by sending the ith value of the array to processor i. 
        /// This variant of <c>Scatter</c> can only be called by a non-root process in the root group. 
        /// The root process should either call the root-only variant of <see cref="Scatter&lt;T&gt;(T[])"/>.
        /// Leaf group processes should call <see cref="Scatter&lt;T&gt;(int)"/>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        public void Scatter<T>()
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // This process does not receive the amount of data to be sent, but it must participate in the call
                Scatter<int>();

                // Receive serialized data
                unsafe
                {
                    int errorCode = MPI.Unsafe.MPI_Scatterv(new IntPtr(), null, null, Unsafe.MPI_BYTE,
                                                            new IntPtr(), 0, Unsafe.MPI_BYTE, Null, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
            else
            {
                unsafe
                {
                    int errorCode = Unsafe.MPI_Scatter(new IntPtr(0), 0, datatype, new IntPtr(0), 0, datatype, Null, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes of the other group, 
        ///   where multiple items are sent to each process.
        ///   This version should be called by the root process.
        ///   (If the number of items to be sent is different at each process, see 
        ///   <see cref="ScatterFromFlattened&lt;T&gt;(T[], int[], int, ref T[])"/>
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The array to be scattered. Only significant at the root.</param>
        /// <param name="count">
        ///   The number of items to be received by each leaf group process. If T is a value type (primitive or
        ///   structure) count must be the same at each process (not just at the root). If T must 
        ///   be serialized, count is ignored at processes other than the root.
        /// </param>
        public void ScatterFromFlattened<T>(T[] inValues, int count)
        {
            T[] temp = new T[0];
            ScatterFromFlattened<T>(inValues, count, Root, ref temp);
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes of the other group, where the number of data items sent to 
        ///   each process may differ. This version should be called by the root process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The array to be scattered.</param>
        /// <param name="counts">
        ///   The number of items to be received by each process of the leaf group.
        /// </param>
        public void ScatterFromFlattened<T>(T[] inValues, int[] counts)
        {
            T[] temp = new T[0];
            ScatterFromFlattened<T>(inValues, counts, Root, ref temp);
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes of the other group, where multiple items are sent to each process.
        ///   This version should be called by leaf group processes.
        ///   (If the number of items to be sent is different at each process, see 
        ///   <see cref="ScatterFromFlattened&lt;T&gt;(T[], int[], int, ref T[])"/>
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="count">
        ///   The number of items to be received by each process of the leaf group. If T is a value type (primitive or
        ///   structure) count must be the same at every process . If T must 
        ///   be serialized, count is ignored at processes other than the root.
        /// </param>
        /// <param name="root">The rank of the root process.</param>
        /// <param name="outValues">The array to write to at the receiving process. Does not have to be preallocated.</param>
        public void ScatterFromFlattened<T>(int count, int root, ref T[] outValues)
        {
            ScatterFromFlattened<T>(null, count, root, ref outValues);
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes of the other group, where the number of data items sent to 
        ///   each process may differ. This version should be called by members of the leaf group.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="counts">
        ///   The number of items to be received by each process. If T must 
        ///   be serialized (i.e. is not a value type), counts is ignored at processes other than the root.
        /// </param>
        /// <param name="root">The rank of the root process.</param>
        /// <param name="outValues">The array to write to at the receiving process. Does not have to be preallocated.</param>
        public void ScatterFromFlattened<T>(int[] counts, int root, ref T[] outValues)
        {
            ScatterFromFlattened<T>(null, counts, root, ref outValues);
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes of the other group, where the number of data items sent to 
        ///   each process may differ. This version should be called by processes in the root group,
        ///   other than the root.
        /// </summary>
        public void ScatterFromFlattened<T>()
        {
            T[] temp = new T[0];
            ScatterFromFlattened<T>(null, new int[0], Null, ref temp);
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes of the other group, where multiple items are sent to each process.
        ///   (If the number of items to be sent is different, see 
        ///   <see cref="ScatterFromFlattened&lt;T&gt;(T[], int[], int, ref T[])"/>
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The array to be scattered. Only significant at the root.</param>
        /// <param name="count">
        ///   The number of items to be received by each process. If T is a value type (primitive or
        ///   structure) count must be the same at every process. If T must 
        ///   be serialized, count is ignored at processes other than the root.
        /// </param>
        /// <param name="root">
        ///   Used to indicate the process gathering the data.
        ///   At the root, should be <see cref="Intercommunicator.Root"/>. At leaf group processes
        ///   should be the rank of the root process in the root group. At non-root processes in the root group,
        ///   should be <see cref="Intercommunicator.Null"/>.
        /// </param>
        /// <param name="outValues">The array to write to at the receiving process. Does not have to be preallocated.</param>
        public void ScatterFromFlattened<T>(T[] inValues, int count, int root, ref T[] outValues)
        {
            int[] counts = new int[Size];
            for (int i = 0; i < Size; i++)
                counts[i] = count;
            ScatterFromFlattened<T>(inValues, counts, root, ref outValues);
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes of the other group, where the number of data items sent to 
        ///   each process may differ.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The array to be scattered. Only significant at the root.</param>
        /// <param name="counts">
        ///   The number of items to be received by each process. If T must 
        ///   be serialized (i.e. is not a value type), counts is ignored at processes other than the root.
        /// </param>
        /// <param name="root">
        ///   Used to indicate the process gathering the data.
        ///   At the root, should be <see cref="Intercommunicator.Root"/>. At leaf group processes
        ///   should be the rank of the root process in the root group. At non-root processes in the root group,
        ///   should be <see cref="Intercommunicator.Null"/>.
        /// </param>
        /// <param name="outValues">The array to write to at the receiving process. Does not have to be preallocated.</param>
        public void ScatterFromFlattened<T>(T[] inValues, int[] counts, int root, ref T[] outValues)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                if (Root == root)
                {
                    T[][] tempIn = new T[RemoteSize][];
                    int inLocation = 0;
                    for (int i = 0; i < RemoteSize; i++) checked
                    {
                        tempIn[i] = new T[counts[i]];
                        Array.Copy(inValues, inLocation, tempIn[i], 0, counts[i]);
                        inLocation += counts[i];
                    }
                    Scatter<T[]>(tempIn);

                }
                else if (Null == root)
                    Scatter<T[]>();
                else
                    outValues = Scatter<T[]>(root);
            }
            else
            {
                if (root != Null && root != Root)
                {
                    if (outValues == null || outValues.Length != counts[Rank])
                        outValues = new T[counts[Rank]];
                }

                if (Root == root)
                {
                    int[] displs = new int[counts.Length];
                    displs[0] = 0;
                    for (int i = 1; i < counts.Length; i++) checked
                    {
                        displs[i] = displs[i - 1] + counts[i - 1];
                    }

                    // Pin the array while we are scattering it.
                    GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                    int errorCode;
                    unsafe
                    {
                        errorCode = Unsafe.MPI_Scatterv(Marshal.UnsafeAddrOfPinnedArrayElement(inValues, 0), counts, displs, datatype,
                                          new IntPtr(0), 0, datatype, root, comm);
                    }
                    inHandle.Free();

                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                else if (Null == root)
                {
                    int errorCode;
                    unsafe
                    {
                        errorCode = Unsafe.MPI_Scatterv(new IntPtr(0), counts, new int[0], datatype,
                                          new IntPtr(0), 0, datatype, root, comm);
                    }

                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                else
                {
                    // Pin the array while we are scattering it.
                    GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                    int errorCode;
                    unsafe
                    {
                        errorCode = Unsafe.MPI_Scatterv(new IntPtr(0), counts, new int[0], datatype,
                                          Marshal.UnsafeAddrOfPinnedArrayElement(outValues, 0), counts[Rank], datatype, root, comm);
                    }
                    outHandle.Free();

                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }
        #endregion
    }
}
