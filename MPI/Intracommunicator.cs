/* Copyright (C) 2007, 2008  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 */
using System;
using System.Runtime.InteropServices;
using System.IO;
using System.Text;

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
    ///   Intracommunicators provide communication among a set of MPI processes.
    /// </summary>
    ///   
    /// <remarks>
    ///   Intracommunicators are the most commonly used form of communicator in MPI. 
    ///   Each intracommunicator contains a set of processes, each of which is identified by its
    ///   "rank" within the communicator. The ranks are numbered 0 through Size-1. 
    ///   Any process in the communicator can send a message to another process
    ///   within the communicator or receive a message from any other process in 
    ///   the communicator. Intracommunicators also support a variety of collective operations
    ///   that involve all of the processes in the communicator. Most MPI communication occurs
    ///   within intracommunicators, with very few MPI programs requiring intercommunicators.
    /// </remarks>
    public class Intracommunicator : Communicator
    {       
        #region Communicator constructors
        /// <summary>
        /// Intracommunicators can only be constructed from other communicators or adopted
        /// from low-level intracommunicators.
        /// </summary>
        internal Intracommunicator() : base()
        {
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
        new public static Intracommunicator Adopt(MPI_Comm comm)
        {
            Communicator result = Communicator.Adopt(comm);
            if (result == null)
                return null;
            else
                return (Intracommunicator)result;
        }
        #endregion

        #region Collective communication
        /// <summary>
        /// Gathers invididual values provided by each processor into an array of values in which the
        /// ith element of the array corresponds to the value provided by the processor with rank i.
        /// The resulting array of values is available to all of the processors. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">
        ///   The value supplied by this processor, which will be placed into the element with index <see cref="Communicator.Rank"/>
        ///   in the returned array.
        /// </param>
        /// <returns> 
        ///   An array of values, where the ith value in the array is a copy of the <paramref name="value"/>
        ///   argument provided by the processor with rank i.
        /// </returns>
        public T[] Allgather<T>(T value)
        {
            T[] result = null;
            Allgather(value, ref result);
            return result;
        }

        /// <summary>
        /// Gathers invididual values provided by each processor into an array of values in which the
        /// ith element of the array corresponds to the value provided by the processor with rank i.
        /// The resulting array of values is available 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValue">
        ///   The value supplied by this processor, which will be placed into the element with index <see cref="Communicator.Rank"/>
        ///   in the returned array.
        /// </param>
        /// <param name="outValues">
        ///   An array of values, where the ith value in the array is a copy of the <paramref name="inValue"/>
        ///   argument provided by the processor with rank i.
        ///   Supply this argument when you have pre-allocated space for the resulting array.
        /// </param>
        public void Allgather<T>(T inValue, ref T[] outValues)
        {
            if (outValues == null || outValues.Length != Size)
                outValues = new T[Size];

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // Perform a gather to the middle node
                Gather(inValue, Size / 2, ref outValues);

                // Broadcast the results to all nodes
                Broadcast(ref outValues, Size / 2);
            }
            else
            {
                GCHandle handle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                int errorCode = Unsafe.MPI_Allgather(Memory.LoadAddress(ref inValue), 1, datatype,
                                                     handle.AddrOfPinnedObject(), 1, datatype, comm);
                handle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        /// <summary>
        /// Gathers invididual values provided by each processor into an array of values.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The values supplied by this processor.
        /// </param>
        /// <param name="count">
        ///   The number of items to be received by each process.
        /// </param>
        public T[] AllgatherFlattened<T>(T[] inValues, int count)
        {
            T[] result = null;
            AllgatherFlattened(inValues, count, ref result);
            return result;
        }

        /// <summary>
        /// Gathers invididual values provided by each processor into an array of values.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The values supplied by this processor.
        /// </param>
        /// <param name="counts">
        ///   The numbers of items to be received by each process.
        /// </param>
        public T[] AllgatherFlattened<T>(T[] inValues, int[] counts)
        {
            T[] result = null;
            AllgatherFlattened(inValues, counts, ref result);
            return result;
        }

        /// <summary>
        /// Gathers invididual values provided by each processor into an array of values.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The values supplied by this processor.
        /// </param>
        /// <param name="count">
        ///   The number of items to be received by each process.
        /// </param>
        /// <param name="outValues">
        ///   An array of values supplied by all processes. Supply this argument when 
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
        ///   The numbers of items to be received by each process.
        /// </param>
        /// <param name="outValues">
        ///   An array of values supplied by all processes. Supply this argument when 
        ///   you have pre-allocated space for the resulting array.
        /// </param>
        public void AllgatherFlattened<T>(T[] inValues, int[] counts, ref T[] outValues)
        {
            if (counts.Length != Size)
                throw new ArgumentException($"counts.Length ({counts.Length}) != Communicator.Size ({Size})");
            
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // Perform a gather to the middle node
                GatherFlattened(inValues, counts, Size / 2, ref outValues);

                // Broadcast the results to all nodes
                Broadcast(ref outValues, Size / 2);
            }
            else
            {
                int size = counts[0];
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
        ///   <c>Allreduce</c> is a collective algorithm that combines the values stored by each process into a 
        ///   single value available to all processes. The values are combined in a user-defined way, specified via 
        ///   a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> are the values provided by the 
        ///   N processes in the communicator, the result will be the value <c>value1 op value2 op ... op valueN</c>.
        /// 
        ///   An <c>Allreduce</c> is equivalent to a <see cref="Reduce&lt;T&gt;(T, MPI.ReductionOperation&lt;T&gt;, int)"/> 
        ///   followed by a <see cref="Broadcast&lt;T&gt;(ref T, int)"/>.
        /// </summary>
        /// <example>
        ///   This example computes the sum of the ranks of all of the processes using 
        ///   <see cref="Intracommunicator.Allreduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;)"/>.
        ///   <code>
        /// using System;
        /// using MPI;
        /// 
        /// class Allreduce
        /// {
        ///   static void Main(string[] args)
        ///   {
        ///     using (MPI.Environment env = new MPI.Environment(ref args))
        ///     {
        ///       Communicator world = Communicator.world;
        ///
        ///       int sum = world.Allreduce(world.Rank, Operation&lt;int&gt;.Add);
        ///       System.Console.WriteLine("Sum of ranks = " + sum);
        ///     }
        ///   }
        /// };
        ///   </code>
        /// </example>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">The local value that will be combined with the values provided by other processes.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <returns>The result of the reduction. The same value will be returned to all processes.</returns>
        public T Allreduce<T>(T value, ReductionOperation<T> op)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // Perform a reduction to the middle node
                T result = Reduce(value, op, Size / 2);

                // Broadcast the result to all nodes
                Broadcast(ref result, Size / 2);
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
        ///   <c>Allreduce</c> is a collective algorithm that combines the values stored by each process into a 
        ///   single value available to all processes. The values are combined in a user-defined way, specified via 
        ///   a delegate. When provided with arrays, <c>Allreduce</c> combines the ith value of each of the arrays 
        ///   passed to each process. Thus, <c>Allreduce</c> on arrays is the same as calling 
        ///   <see cref="Allreduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;)"/> for each <c>inValues[i]</c>.
        /// 
        ///   An <c>Allreduce</c> is equivalent to a <see cref="Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int, ref T[])"/> 
        ///   followed by a <see cref="Broadcast&lt;T&gt;(ref T[], int)"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">The local values that will be combined with the values provided by other processes.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <returns>
        ///   The values that result from combining all of the values in <paramref name="values"/>
        ///   element-by-element.
        /// </returns>
        public T[] Allreduce<T>(T[] values, ReductionOperation<T> op)
        {
            T[] result = null;
            Allreduce(values, op, ref result);
            return result;
        }

        /// <summary>
        ///   <c>Allreduce</c> is a collective algorithm that combines the values stored by each process into a 
        ///   single value available to all processes. The values are combined in a user-defined way, specified via 
        ///   a delegate. When provided with arrays, <c>Allreduce</c> combines the ith value of each of the arrays 
        ///   passed to each process. Thus, <c>Allreduce</c> on arrays is the same as calling 
        ///   <see cref="Allreduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;)"/> for each <c>inValues[i]</c>.
        /// 
        ///   An <c>Allreduce</c> is equivalent to a <see cref="Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int, ref T[])"/> 
        ///   followed by a <see cref="Broadcast&lt;T&gt;(ref T[], int)"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The local values that will be combined with the values provided by other processes.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <param name="outValues">
        ///   The values that result from combining all of the values in <paramref name="inValues"/>
        ///   element-by-element.
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
                // Perform a reduction to the middle node
                Reduce(inValues, op, Size / 2, ref outValues);

                // Broadcast the result to all nodes
                Broadcast(ref outValues, Size / 2);
            }
            else
            {
                using (Operation<T> mpiOp = new Operation<T>(op))
                {
                    GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                    GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                    int errorCode = Unsafe.MPI_Allreduce(inHandle.AddrOfPinnedObject(), 
                                                         outHandle.AddrOfPinnedObject(),
                                                         inValues.Length, datatype, mpiOp.Op, comm);
                    inHandle.Free();
                    outHandle.Free();
                    
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        /// Collective operation in which every process sends data to every other process. <c>Alltoall</c>
        /// differs from <see cref="Allgather&lt;T&gt;(T)"/> in that a given process can send different
        /// data to all of the other processes, rather than contributing the same piece of data to all
        /// processes. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   The array of values that will be sent to each process. The ith value in this array
        ///   will be sent to the process with rank i.
        /// </param>
        /// <returns>
        ///   An array of values received from all of the other processes. The jth value in this
        ///   array will be the value sent to the calling process from the process with rank j.
        /// </returns>
        public T[] Alltoall<T>(T[] values)
        {
            T[] result = null;
            Alltoall(values, ref result);
            return result;
        }

        /// <summary>
        /// Collective operation in which every process sends data to every other process. <c>Alltoall</c>
        /// differs from <see cref="Allgather&lt;T&gt;(T)"/> in that a given process can send different
        /// data to all of the other processes, rather than contributing the same piece of data to all
        /// processes. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The array of values that will be sent to each process. The ith value in this array
        ///   will be sent to the process with rank i.
        /// </param>
        /// <param name="outValues">
        ///   The array of values received from all of the other processes. The jth value in this
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
                if (Size == 1)
                {
                    inValues.CopyTo(outValues, 0);
                    return;
                }

                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                if (SplitLargeObjects)
                    Serialization.Alltoall(this, inValues, outValues);
                else            
                    Alltoall_serialized(inValues, ref outValues);
            }
            else
            {
                GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                int errorCode = Unsafe.MPI_Alltoall(inHandle.AddrOfPinnedObject(), 1, datatype,
                                                    outHandle.AddrOfPinnedObject(), 1, datatype, comm);
                inHandle.Free();
                outHandle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        protected void Alltoall_serialized<T>(T[] inValues, ref T[] outValues)
        {
            int[] sendCounts = new int[Size];
            int[] sendOffsets = new int[Size];

            using (UnmanagedMemoryStream sendStream = new UnmanagedMemoryStream())
            {
                // Serialize all of the outgoing data to the outgoing stream
                for (int dest = 0; dest < Size; ++dest)
                {
                    sendOffsets[dest] = Convert.ToInt32(sendStream.Length);
                    if (dest != Rank)
                        Serialize(sendStream, inValues[dest]);
                    sendCounts[dest] = checked(Convert.ToInt32(sendStream.Length) - sendOffsets[dest]);
                }

                // Use all-to-all on integers to tell every process how much data
                // it will be receiving.
                int[] recvCounts = Alltoall(sendCounts);

                // Compute the offsets at which each of the streams will be received
                int[] recvOffsets = new int[Size];
                recvOffsets[0] = 0;
                for (int i = 1; i < Size; ++i) checked
                {
                    recvOffsets[i] = recvOffsets[i - 1] + recvCounts[i - 1];
                }

                // Total length of the receive buffer
                int recvLength = checked(recvOffsets[Size - 1] + recvCounts[Size - 1]);

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
                    for (int source = 0; source < Size; ++source)
                    {
                        if (source == Rank)
                            // We never transmitted this object, so we don't need to de-serialize it.
                            outValues[source] = inValues[source];
                        else
                        {
                            // Seek to the proper location in the stream and de-serialize
                            recvStream.Position = recvOffsets[source];
                            outValues[source] = Deserialize<T>(recvStream);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Collective operation in which every process sends data to every other process. <c>Alltoall</c>
        /// differs from <see cref="Allgather&lt;T&gt;(T)"/> in that a given process can send different
        /// data to all of the other processes, rather than contributing the same piece of data to all
        /// processes. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The array of values that will be sent to each process. sendCounts[i] worth of data will
        ///   be sent to process i.
        /// </param>
        /// <param name="count">
        ///   The number of items to be sent and received by each process.
        /// </param>
        public T[] AlltoallFlattened<T>(T[] inValues, int count)
        {
            T[] returnValues = null;
            AlltoallFlattened(inValues, count, ref returnValues);
            return returnValues;
        }

        /// <summary>
        /// Collective operation in which every process sends data to every other process. <c>Alltoall</c>
        /// differs from <see cref="Allgather&lt;T&gt;(T)"/> in that a given process can send different
        /// data to all of the other processes, rather than contributing the same piece of data to all
        /// processes. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The array of values that will be sent to each process. sendCounts[i] worth of data will
        ///   be sent to process i.
        /// </param>
        /// <param name="count">
        ///   The number of items to be sent and received by each process.
        /// </param>
        /// <param name="outValues">
        ///   The array of values received from all of the other processes.
        /// </param>
        public void AlltoallFlattened<T>(T[] inValues, int count, ref T[] outValues)
        {
            int[] sendCounts = new int[Size];
            int[] recvCounts = new int[Size];
            for (int i = 0; i < Size; i++)
            {
                sendCounts[i] = count;
                recvCounts[i] = count;
            }
            AlltoallFlattened(inValues, sendCounts, recvCounts, ref outValues);
        }

        /// <summary>
        /// Collective operation in which every process sends data to every other process. <c>Alltoall</c>
        /// differs from <see cref="Allgather&lt;T&gt;(T)"/> in that a given process can send different
        /// data to all of the other processes, rather than contributing the same piece of data to all
        /// processes. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The array of values that will be sent to each process. sendCounts[i] worth of data will
        ///   be sent to process i.
        /// </param>
        /// <param name="sendCounts">
        ///   The numbers of items to be sent to each process.
        /// </param>
        /// <param name="recvCounts">
        ///   The numbers of items to be received by each process.
        /// </param>
        public T[] AlltoallFlattened<T>(T[] inValues, int[] sendCounts, int[] recvCounts)
        {
            T[] returnValues = null;
            AlltoallFlattened(inValues, sendCounts, recvCounts, ref returnValues);
            return returnValues;
        }

        /// <summary>
        /// Collective operation in which every process sends data to every other process. <c>Alltoall</c>
        /// differs from <see cref="Allgather&lt;T&gt;(T)"/> in that a given process can send different
        /// data to all of the other processes, rather than contributing the same piece of data to all
        /// processes. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   An array holding all values that will be sent. The first sendCounts[0] items are sent to
        ///   process 0, the next sendCounts[1] items are sent to process 1, and so on.
        ///   The length of the array must be at least the sum of sendCounts.
        /// </param>
        /// <param name="sendCounts">
        ///   The number of items to be sent to each process.  Must be non-negative and have length this.Size.
        /// </param>
        /// <param name="recvCounts">
        ///   The number of items to be received by each process.  Must be non-negative and have length this.Size.
        /// </param>
        /// <param name="outValues">
        ///   An array that receives the concatenated values from all of the other processes.
        ///   If null or the length of the array is less than the sum of recvCounts, it will be assigned to a new array.
        /// </param>
        public void AlltoallFlattened<T>(T[] inValues, int[] sendCounts, int[] recvCounts, ref T[] outValues)
        {
            if (sendCounts.Length != Size)
                throw new ArgumentException($"sendCounts.Length ({sendCounts.Length}) != Communicator.Size ({Size})");
            if (recvCounts.Length != Size)
                throw new ArgumentException($"recvCounts.Length ({recvCounts.Length}) != Communicator.Size ({Size})");
            if (Size == 1)
            {
                outValues = inValues;
                return;
            }

            SpanTimer.Enter("AlltoallFlattened");
            int totalCounts = 0;
            for (int i = 0; i < recvCounts.Length; i++) checked
            {
                totalCounts += recvCounts[i];
            }
            // Make sure the outgoing array is the right size
            if (outValues == null || outValues.Length < totalCounts)
                outValues = new T[totalCounts];

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                if (SplitLargeObjects)
                    Serialization.AlltoallFlattened(this, inValues, sendCounts, recvCounts, outValues);
                else
                    AlltoallFlattened_serialized(inValues, sendCounts, recvCounts, outValues);
            }
            else
            {
                int[] sendDispls = new int[sendCounts.Length];
                int[] recvDispls = new int[recvCounts.Length];
                sendDispls[0] = 0;
                recvDispls[0] = 0;
                for (int i = 1; i < sendDispls.Length; i++) checked
                {
                    sendDispls[i] = sendDispls[i - 1] + sendCounts[i - 1];
                    recvDispls[i] = recvDispls[i - 1] + recvCounts[i - 1];
                }
                int lastIndex = sendCounts.Length - 1;
                int totalSendCount = checked(sendDispls[lastIndex] + sendCounts[lastIndex]);
                if (totalSendCount > inValues.Length)
                {
                    throw new ArgumentException($"Sum of sendCounts ({totalSendCount}) > inValues.Length ({inValues.Length})");
                }

                GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                int errorCode = Unsafe.MPI_Alltoallv(inHandle.AddrOfPinnedObject(), sendCounts, sendDispls, datatype,
                                                    outHandle.AddrOfPinnedObject(), recvCounts, recvDispls, datatype, comm);
                inHandle.Free();
                outHandle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            SpanTimer.Leave("AlltoallFlattened");
        }

        protected void AlltoallFlattened_serialized<T>(T[] inValues, int[] sendCounts, int[] recvCounts, T[] outValues)
        {
            using (UnmanagedMemoryStream sendStream = new UnmanagedMemoryStream())
            {
                int selfLocation = 0;
                int inLocation = 0;
                // Serialize all of the outgoing data to the outgoing stream
                int[] sendOffsets = new int[Size];
                int[] sendCountsSerialized = new int[Size];
                for (int dest = 0; dest < Size; ++dest) checked
                {
                    sendOffsets[dest] = Convert.ToInt32(sendStream.Length);
                    if (dest == Rank)
                        selfLocation = inLocation;
                    else if (sendCounts[dest] > 0)
                    {
                        var temp = new T[sendCounts[dest]];
                        Array.Copy(inValues, inLocation, temp, 0, sendCounts[dest]);
                        Serialize(sendStream, temp);
                    }
                    inLocation += sendCounts[dest];
                    sendCountsSerialized[dest] = Convert.ToInt32(sendStream.Length) - sendOffsets[dest];
                }

                // Use all-to-all on integers to tell every process how much data
                // it will be receiving.
                int[] recvCountsSerialized = Alltoall(sendCountsSerialized);

                // Compute the offsets at which each of the streams will be received
                int[] recvOffsets = new int[Size];
                recvOffsets[0] = 0;
                for (int i = 1; i < Size; ++i) checked
                {
                    recvOffsets[i] = recvOffsets[i - 1] + recvCountsSerialized[i - 1];
                }

                // Total length of the receive buffer
                int recvLength = checked(recvOffsets[Size - 1] + recvCountsSerialized[Size - 1]);

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
                    int outLocation = 0;
                    for (int source = 0; source < Size; ++source) checked
                    {
                        if (source == Rank)
                        {
                            // We never transmitted this object, so we don't need to de-serialize it.
                            if (sendCounts[source] != recvCounts[source])
                                throw new ArgumentException($"sendCounts[Rank] ({sendCounts[Rank]}) != recvCounts[Rank] ({recvCounts[Rank]})");
                            Array.Copy(inValues, selfLocation, outValues, outLocation, recvCounts[source]);
                        }
                        else if (recvCounts[source] > 0)
                        {
                            // Seek to the proper location in the stream and de-serialize
                            recvStream.Position = recvOffsets[source];
                            var temp = Deserialize<T[]>(recvStream);
                            if (temp.Length != recvCounts[source])
                                throw new Exception($"Deserialized array has length {temp.Length}; expected {recvCounts[source]}");
                            Array.Copy(temp, 0, outValues, outLocation, temp.Length);
                        }
                        outLocation += recvCounts[source];
                    }
                }
            }
        }

        /// <summary>
        /// Broadcast a value from the <paramref name="root"/> process to all other processes.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">
        ///   The value to be broadcast. At the <paramref name="root"/> process, this value is
        ///   read (but not written); at all other processes, this value will be replaced with
        ///   the value at the root.
        /// </param>
        /// <param name="root">
        ///   The rank of the process that is broadcasting the value out to
        ///   all of the non-<paramref name="root"/> processes.
        /// </param>
        public void Broadcast<T>(ref T value, int root)
        {
            Broadcast_impl<T>((Rank == root), ref value, root);
        }

        /// <summary>
        /// Broadcast an array from the <paramref name="root"/> process to all other processes.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   The array of values to be broadcast. At the <paramref name="root"/> process, this value is
        ///   read (but not written); at all other processes, this value will be replaced with
        ///   the value at the root. Note that the receiving processes must already have allocated
        ///   enough space in the array to receive data from the root, prior to calling <c>Broadcast</c>.
        /// </param>
        /// <param name="root">
        ///   The rank of the process that is broadcasting the values out to
        ///   all of the non-<paramref name="root"/> processes.
        /// </param>
        public void Broadcast<T>(ref T[] values, int root)
        {
            Broadcast_impl<T>((Rank == root), ref values, root);
        }
        
        /// <summary>
        ///   <c>ExclusiveScan</c> is a collective algorithm that performs partial reductions on data provided by
        ///   each process in the communicator. <c>ExclusiveScan</c> combines the values stored by each process into 
        ///   partial results delivered to each process. The values are combined 
        ///   in a user-defined way, specified via a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> 
        ///   are the values provided by the N processes in the communicator, the resulting value for the process with 
        ///   rank P > 0 will be <c>value1 op value2 op ... op valueP</c>. The rank 0 process receives a default value.
        /// 
        ///   <c>ExclusiveScan</c> is sometimes called an "exclusive" scan, because the result returned to each process
        ///   does not include the contribution of that process. For an "inclusive" scan (that does include the contribution
        ///   of the calling process in its result), use <see cref="Scan&lt;T&gt;(T, MPI.ReductionOperation&lt;T&gt;)"/>.
        ///   Note that the results provided by <c>ExclusiveScan</c> are the same as those provided by <c>Scan</c>, except that
        ///   with <c>ExclusiveScan</c>, the process with rank i will receive the result that rank i-1 would have received from
        ///   <c>Scan</c>, i.e., <c>ExclusiveScan</c> gives the same results but shifted to the right.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">The value contributed by the calling process.</param>
        /// <param name="op">
        ///   Operation used to combine two values from different processes.
        ///   This operation must be associative.
        /// </param>
        /// <returns>
        ///   If the rank of the calling process is not zero, the value <c>value1 op value2 op ... op valueP</c>, where P 
        ///   is the rank of the calling process. Otherwise, returns a default value.
        /// </returns>
        public T ExclusiveScan<T>(T value, ReductionOperation<T> op)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // Gather all of the values to rank 0
                T[] values = Gather(value, 0);
                if (Rank == 0)
                {
                    // This is the root: compute each intermediate result
                    T intermediate = values[0];
                    for (int p = 1; p < Size; ++p)
                    {
                        T prior = intermediate;
                        intermediate = op(intermediate, values[p]);
                        values[p] = prior;
                    }
                    values[0] = default(T);
                }

                return Scatter(values, 0);
            }
            else
            {
                T result = default(T);
                using (Operation<T> mpiOp = new Operation<T>(op))
                {
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Exscan(Memory.LoadAddress(ref value), Memory.LoadAddressOfOut(out result),
                                          1, datatype, mpiOp.Op, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
                return result;
            }
        }

        /// <summary>
        ///   <c>ExclusiveScan</c> is a collective algorithm that performs partial reductions on data provided by
        ///   each process in the communicator. <c>ExclusiveScan</c> combines the arrays stored by each process into 
        ///   partial results delivered to each process. The values are combined in a user-defined way, specified via 
        ///   a delegate that will be applied elementwise to the values in the arrays. If <c>array1</c>, <c>array2</c>, ..., <c>arrayN</c> 
        ///   are the values provided by the N processes in the communicator, the resulting value for the process with 
        ///   rank P > 0 will be <c>array1 op array2 op ... op arrayP</c>. The rank 0 process receives a <c>null</c> array.
        /// 
        ///   <c>ExclusiveScan</c> is sometimes called an "exclusive" scan, because the result returned to each process
        ///   does not include the contribution of that process. For an "inclusive" scan (that does include the contribution
        ///   of the calling process in its result), use <see cref="Scan&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;)"/>.
        ///   Note that the results provided by <c>ExclusiveScan</c> are the same as those provided by <c>Scan</c>, except that
        ///   with <c>ExclusiveScan</c>, the process with rank i will receive the result that rank i-1 would have received from
        ///   <c>Scan</c>, i.e., <c>ExclusiveScan</c> gives the same results but shifted to the right.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   The values contributed by the calling process. The lengths of the arrays provided by all of the
        ///   processes must be the same.
        /// </param>
        /// <param name="op">
        ///   Operation used to combine two values from different processes.
        ///   This operation must be associative.
        /// </param>
        /// <returns>
        ///   If the rank of the calling process is not zero, the value <c>array1 op array2 op ... op arrayP</c>, where P 
        ///   is the rank of the calling process. Otherwise, returns a <c>null</c> array.
        /// </returns>
        public T[] ExclusiveScan<T>(T[] values, ReductionOperation<T> op)
        {
            T[] result = null;
            ExclusiveScan(values, op, ref result);
            return result;
        }

        /// <summary>
        ///   <c>ExclusiveScan</c> is a collective algorithm that performs partial reductions on data provided by
        ///   each process in the communicator. <c>ExclusiveScan</c> combines the arrays stored by each process into 
        ///   partial results delivered to each process. The values are combined in a user-defined way, specified via 
        ///   a delegate that will be applied elementwise to the values in the arrays. If <c>array1</c>, <c>array2</c>, ..., <c>arrayN</c> 
        ///   are the values provided by the N processes in the communicator, the resulting value for the process with 
        ///   rank P > 0 will be <c>array1 op array2 op ... op arrayP</c>. The rank 0 process receives a <c>null</c> array.
        /// 
        ///   <c>ExclusiveScan</c> is sometimes called an "exclusive" scan, because the result returned to each process
        ///   does not include the contribution of that process. For an "inclusive" scan (that does include the contribution
        ///   of the calling process in its result), use <see cref="Scan&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;)"/>.
        ///   Note that the results provided by <c>ExclusiveScan</c> are the same as those provided by <c>Scan</c>, except that
        ///   with <c>ExclusiveScan</c>, the process with rank i will receive the result that rank i-1 would have received from
        ///   <c>Scan</c>, i.e., <c>ExclusiveScan</c> gives the same results but shifted to the right.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The values contributed by the calling process. The lengths of the arrays provided by all of the
        ///   processes must be the same.
        /// </param>
        /// <param name="op">
        ///   Operation used to combine two values from different processes.
        ///   This operation must be associative.
        /// </param>
        /// <param name="outValues">
        ///   If the rank of the calling process is not zero, this will receive the value <c>array1 op array2 op ... op arrayP</c>, where P 
        ///   is the rank of the calling process. Otherwise, this value will be replaced with <c>null</c>.
        /// </param>
        public void ExclusiveScan<T>(T[] inValues, ReductionOperation<T> op, ref T[] outValues)
        {
            if (outValues == null || outValues.Length != inValues.Length)
                outValues = new T[inValues.Length];

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // Perform a reduction to the middle node
                T[][] allValues = Gather(inValues, 0);
                if (Rank == 0)
                {
                    // Intermediate results: shares storage with outValues, which will eventually be overwritten
                    T[] intermediate = outValues;

                    // Initialize the intermediate result with our (the 0th) results
                    for (int i = 0; i < inValues.Length; ++i)
                        intermediate[i] = inValues[i];

                    // Compute each intermediate result and send it to the
                    // appropriate process
                    T[] prior = new T[intermediate.Length];
                    for (int p = 1; p < Size; ++p)
                    {
                        Array.Copy(intermediate, prior, intermediate.Length);

                        for (int i = 0; i < inValues.Length; ++i)
                            intermediate[i] = op(intermediate[i], allValues[p][i]);

                        Array.Copy(prior, allValues[p], prior.Length);
                    }

                    allValues[0] = null;
                }

                // Scatter all of the results
                outValues = Scatter(allValues, 0);
            }
            else
            {
                using (Operation<T> mpiOp = new Operation<T>(op))
                {
                    GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                    GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                    int errorCode = Unsafe.MPI_Exscan(inHandle.AddrOfPinnedObject(),
                                                      outHandle.AddrOfPinnedObject(),
                                                      inValues.Length, datatype, mpiOp.Op, comm);
                    outHandle.Free();
                    inHandle.Free();
                    
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }

                if (Rank == 0)
                    outValues = null;
            }
        }

        /// <summary>
        ///   Gather the values from each process into an array of values at the 
        ///   <paramref name="root"/> process. On the root process, the pth element of the result
        ///   will be equal to the <paramref name="value"/> parameter of the process
        ///   with rank <c>p</c> when this routine returns.
        /// </summary>
        /// <example>
        /// This example demonstrates the use of <c>Gather</c> to gather the ranks of 
        /// all processes together at root 0.
        /// <code>
        /// using System;
        /// using MPI;
        /// 
        /// class Gather
        /// {
        ///     static int Main(string[] args)
        ///     {
        ///         using (MPI.Environment env = new MPI.Environment(ref args))
        ///         {
        ///             Intracommunicator world = Communicator.world;
        ///             if (world.Rank == 0)
        ///             {
        ///                 int[] ranks = world.Gather(world.Rank, 0);
        ///                 System.Console.Write("Ranks of all of the processes:");
        ///                 for (int i = 0; i &lt; ranks.Length; ++i)
        ///                     System.Console.Write(" " + i);
        ///                 System.Console.WriteLine();
        ///             }
        ///             else
        ///             {
        ///                 world.Gather(world.Rank, 0);
        ///             }
        ///         }
        ///     }
        /// }
        /// </code>
        /// </example>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">The value contributed by this process.</param>
        /// <param name="root">
        ///   The rank of the process that will receive values for all of the
        ///   processes in the communicator.
        /// </param>
        /// <returns>
        ///   At the root, an array containing the <paramref name="value"/>s supplied by each of the processes
        ///   in the communicator. All other processes receive <c>null</c>.
        /// </returns>
        public T[] Gather<T>(T value, int root)
        {
            T[] result = null;
            Gather(value, root, ref result);
            return result;
        }

        /// <summary>
        ///   Gather the values from each process into an array of values at the 
        ///   <paramref name="root"/> process. On the root process, <c>outValues[p]</c>
        ///   will be equal to the <paramref name="inValue"/> parameter of the process
        ///   with rank <c>p</c> when this routine returns. Use this variant of the routine 
        ///   when you want to pre-allocate storage for the <paramref name="outValues"/> array.
        /// </summary>
        /// <example>
        /// This example demonstrates the use of <c>Gather</c> to gather the ranks of 
        /// all processes together at root 0.
        /// <code>
        /// using System;
        /// using MPI;
        /// 
        /// class Gather
        /// {
        ///     static int Main(string[] args)
        ///     {
        ///         using (MPI.Environment env = new MPI.Environment(ref args))
        ///         {
        ///             Intracommunicator world = Communicator.world;
        ///             if (world.Rank == 0)
        ///             {
        ///                 int[] ranks = new int[world.Size];
        ///                 world.Gather(world.Rank, 0, ref ranks);
        ///                 System.Console.Write("Ranks of all of the processes:");
        ///                 for (int i = 0; i &lt; ranks.Length; ++i)
        ///                     System.Console.Write(" " + i);
        ///                 System.Console.WriteLine();
        ///             }
        ///             else
        ///             {
        ///                 world.Gather(world.Rank, 0);
        ///             }
        ///         }
        ///     }
        /// }
        /// </code>
        /// </example>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValue">The value contributed by this process.</param>
        /// <param name="root">
        ///   The rank of the process that will receive values for all of the
        ///   processes in the communicator.
        /// </param>
        /// <param name="outValues">
        ///   An array that will store the values contributed by each process.
        ///   This value is only significant at the <paramref name="root"/>, and
        ///   can be omitted by non-root processes.
        ///   Supply this argument when you have pre-allocated space for the resulting array.
        /// </param>
        public void Gather<T>(T inValue, int root, ref T[] outValues)
        {
            Gather_impl<T>((Rank == root), Size, inValue, root, ref outValues);
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The values to be contributed by this process.  Must have the same length in all processes.</param>
        /// <param name="root">The rank of the root node.</param>
        /// <returns>The aggregated gathered values.</returns>
        public T[] GatherFlattened<T>(T[] inValues, int root)
        {
            T[] result = null;
            GatherFlattened(inValues, root, ref result);
            return result;
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        ///   In this variant, the number of elements contributed need not be identical for each process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The values to be contributed by this process.</param>
        /// <param name="counts">
        ///   The number of elements contributed by each process.
        /// </param>
        /// <param name="root">The rank of the root node.</param>
        /// <returns>The aggregated gathered values.</returns>
        public T[] GatherFlattened<T>(T[] inValues, int[] counts, int root)
        {
            T[] result = null;
            GatherFlattened(inValues, counts, root, ref result);
            return result;
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The values to be contributed by this process.  Must have the same length in all processes.</param>
        /// <param name="root">The rank of the root node.</param>
        /// <param name="outValues">
        ///   The array to write the gathered values to; use this parameter when you have preallocated
        ///   space for the array.
        /// </param>
        /// <returns>The aggregated gathered values.</returns>
        public void GatherFlattened<T>(T[] inValues, int root, ref T[] outValues)
        {
            int[] counts = new int[Size];
            for (int i = 0; i < Size; i++)
                counts[i] = inValues.Length;

            GatherFlattened<T>(inValues, counts, root, ref outValues);
        }

        /// <summary>
        ///   Similar to <see cref="Gather&lt;T&gt;(T,int)"/> but here all values are aggregated into one large array.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The values to be contributed by this process.</param>
        /// <param name="counts">
        ///   The number of elements contributed by each process.  Only relevant at the root.
        /// </param>
        /// <param name="root">The rank of the root node.</param>
        /// <param name="outValues">
        ///   The array to write the gathered values to; use this parameter when you have preallocated
        ///   space for the array.  Only relevant at the root.
        /// </param>
        /// <returns>The aggregated gathered values.</returns>
        public void GatherFlattened<T>(T[] inValues, int[] counts, int root, ref T[] outValues)
        {
            GatherFlattened_impl<T>((Rank == root), Size, inValues, counts, root, ref outValues);
        }

        /// <summary>
        ///   <c>Reduce</c> is a collective algorithm that combines the values stored by each process into a 
        ///   single value available at the designated <paramref name="root"/> process. The values are combined 
        ///   in a user-defined way, specified via a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> 
        ///   are the values provided by the N processes in the communicator, the result will be the value 
        ///   <c>value1 op value2 op ... op valueN</c>. This result is only
        ///   available to the <paramref name="root"/> process. If all processes need the result of the reduction,
        ///   use <see cref="Intracommunicator.Allreduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;)"/>.
        /// </summary>
        /// <example>
        ///   This example computes the sum of the ranks of all of the processes using 
        ///   <see cref="Reduce&lt;T&gt;(T, MPI.ReductionOperation&lt;T&gt;, int)"/>.
        ///   <code>
        /// using System;
        /// using MPI;
        /// 
        /// class Reduce
        /// {
        ///   static int Main(string[] args)
        ///   {
        ///     using (MPI.Environment env = new MPI.Environment(ref args))
        ///     {
        ///       Intracommunicator world = Communicator.world;
        ///
        ///       int root = 0;
        ///       if (world.Rank == root)
        ///       {
        ///         int sum;
        ///         world.Reduce(world.Rank, out sum, Operation&lt;int&gt;.Add, root);
        ///         System.Console.WriteLine("Sum of ranks = " + sum);
        ///       }
        ///       else 
        ///       {
        ///         world.Reduce(world.Rank, Operation&lt;int&gt;.Add, root);
        ///       }
        ///     }
        ///   }
        /// };
        ///   </code>
        /// </example>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">The local value that will be combined with the values provided by other processes.</param>
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
            return Reduce_impl<T>((Rank == root), Size, value, op, root);
        }

        /// <summary>
        ///   <c>Reduce</c> is a collective algorithm that combines the values stored by each process into a 
        ///   single value available at the designated <paramref name="root"/> process. This particular variant of
        ///   <see cref="Reduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;, int)"/> applies to each of the elements
        ///   of the provided arrays. Each process must provide arrays of the same length, and the values at each
        ///   array index are combined 
        ///   in a user-defined way, specified via a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> 
        ///   are the ith values provided by the N processes in the communicator, the ith result will be the value 
        ///   <c>value1 op value2 op ... op valueN</c>. The resulting array is only
        ///   available to the <paramref name="root"/> process. If all processes need the result of the reduction,
        ///   use <see cref="Intracommunicator.Allreduce&lt;T&gt;(T[], ReductionOperation&lt;T&gt;, ref T[])"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The local values that will be combined with the values provided by other processes.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <param name="root">
        ///   The rank of the process that is the root of the reduction operation, which will receive the result
        ///   of the reduction operation in its <paramref name="outValues"/> argument.
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

            if (Rank != root)
            {
                if (datatype == Unsafe.MPI_DATATYPE_NULL)
                {
                    // Gather values at the root
                    Gather(inValues, root);
                }
                else
                {
                    // Use the low-level MPI reduction operation from a non-root
                    using (Operation<T> mpiOp = new Operation<T>(op))
                    {
                        GCHandle handle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                        int errorCode = Unsafe.MPI_Reduce(Marshal.UnsafeAddrOfPinnedArrayElement(inValues, 0), new IntPtr(),
                                              inValues.Length, datatype, mpiOp.Op, root, comm);
                        handle.Free();

                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
            }
            else
            {
                // Reduction at the root

                // Make sure the resulting array is long enough
                if (outValues == null || outValues.Length != inValues.Length)
                    outValues = new T[inValues.Length];

                if (datatype == Unsafe.MPI_DATATYPE_NULL)
                {
                    // Gather into a temporary array
                    T[][] values = new T[Size][];
                    Gather(inValues, root, ref values);

                    // Perform reduction locally
                    for (int i = 0; i < inValues.Length; ++i)
                    {
                        outValues[i] = values[0][i];
                        for (int p = 1; p < Size; ++p)
                            outValues[i] = op(outValues[i], values[p][i]);
                    }
                }
                else
                {
                    // Use the low-level MPI reduction operation from the root
                    using (Operation<T> mpiOp = new Operation<T>(op))
                    {
                        GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                        GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                        int errorCode = Unsafe.MPI_Reduce(inHandle.AddrOfPinnedObject(),
                                                          outHandle.AddrOfPinnedObject(),
                                                          inValues.Length, datatype, mpiOp.Op, root, comm);
                        inHandle.Free();
                        outHandle.Free();

                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
            }

        
        }

        /// <summary>
        ///   <c>Reduce</c> is a collective algorithm that combines the values stored by each process into a 
        ///   single value available at the designated <paramref name="root"/> process. This particular variant of
        ///   <see cref="Reduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;, int)"/> applies to each of the elements
        ///   of the provided arrays. Each process must provide arrays of the same length, and the values at each
        ///   array index are combined 
        ///   in a user-defined way, specified via a delegate. If <c>value1</c>, <c>value2</c>, ..., <c>valueN</c> 
        ///   are the ith values provided by the N processes in the communicator, the ith result will be the value 
        ///   <c>value1 op value2 op ... op valueN</c>. The resulting array is only
        ///   available to the <paramref name="root"/> process. If all processes need the result of the reduction,
        ///   use <see cref="Intracommunicator.Allreduce&lt;T&gt;(T[], ReductionOperation&lt;T&gt;, ref T[])"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">The local values that will be combined with the values provided by other processes.</param>
        /// <param name="op">
        ///   The operation used to combine two values. This operation must be associative.
        /// </param>
        /// <param name="root">
        ///   The rank of the process that is the root of the reduction operation, to which this routine will
        ///   return the result of the reduction operation. 
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
        /// A collective operation that first performs a reduction on the given <paramref name="values"/> 
        /// (see <see cref="Intracommunicator.Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int)"/> and then scatters
        /// the results by sending some elements to each process. The reduction will be performed on 
        /// the entire array of <paramref name="values"/> (like the array form of 
        /// <see cref="Intracommunicator.Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int)"/>). Then, the array will
        /// be scattered, with process i receiving <paramref name="counts"/>[i] elements. The process
        /// with rank 0 will receive the first <c>counts[0]</c> elements, the process with rank 1 will 
        /// receive the next <c>counts[1]</c> elements, and so on.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   An array of values that will be reduced. The length of this array must equal to 
        ///   sum of the counts in <paramref name="counts"/>.
        /// </param>
        /// <param name="op">
        ///   The operation used to combine the elements in <paramref name="values"/>.
        ///   This operation must be associative.
        /// </param>
        /// <param name="counts">
        ///   An array whose ith element states the number of elements from the reduced result 
        ///   that will be returned to the process with rank i. 
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
        /// (see <see cref="Intracommunicator.Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int, ref T[])"/> and then scatters
        /// the results by sending some elements to each process. The reduction will be performed on 
        /// the entire array of <paramref name="inValues"/> (like the array form of 
        /// <see cref="Intracommunicator.Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int, ref T[])"/>). Then, the array will
        /// be scattered, with process i receiving <paramref name="counts"/>[i] elements. The process
        /// with rank 0 will receive the first <c>counts[0]</c> elements, the process with rank 1 will 
        /// receive the next <c>counts[1]</c> elements, and so on.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   An array of values that will be reduced. The length of this array must equal to 
        ///   sum of the counts in <paramref name="counts"/>.
        /// </param>
        /// <param name="op">
        ///   The operation used to combine the elements in <paramref name="inValues"/>.
        ///   This operation must be associative.
        /// </param>
        /// <param name="counts">
        ///   An array whose ith element states the number of elements from the reduced result 
        ///   that will be returned to the process with rank i. 
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

            // Make sure inValues has the right length
            int sum = 0;
            for (int i = 0; i < Size; ++i) checked
            {
                sum += counts[i];
            }
            if (inValues.Length != sum)
                throw new ArgumentException("inValues array must have enough values for all of the receivers", "inValues");

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;

            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                if (Size == 1)
                {
                    for (int i = 0; i < inValues.Length; ++i)
                        outValues[i] = inValues[i];
                    return;
                }

                // Reduce the entire array to a root
                T[] results = Reduce(inValues, op, 0);

                if (Rank == 0)
                {
                    // This is the root process
                    using (UnmanagedMemoryStream sendStream = new UnmanagedMemoryStream())
                    {
                        // Serialize all of the outgoing data for the scatter
                        int[] resultCounts = new int[Size];
                        int[] resultOffsets = new int[Size];
                        resultCounts[0] = 0;
                        resultOffsets[0] = 0;
                        int position = counts[0];
                        for (int dest = 1; dest < Size; ++dest)
                        {
                            // Serialize the outgoing elements for the process with rank "dest"
                            resultOffsets[dest] = Convert.ToInt32(sendStream.Length);
                            for (int i = 0; i < counts[dest]; ++i)
                            {
                                Serialize(sendStream, results[position]);
                                ++position;
                            }
                            resultCounts[dest] = checked(Convert.ToInt32(sendStream.Length) - resultOffsets[dest]);
                        }

                        // Scatter the counts to tell each process how many bytes to expect
                        Scatter(true, resultCounts, 0);

                        // Scatter the reduced, serialized data to all of the other processes
                        unsafe
                        {
                            int errorCode = Unsafe.MPI_Scatterv(sendStream.Buffer, resultCounts, resultOffsets, Unsafe.MPI_BYTE,
                                                                new IntPtr(), 0, Unsafe.MPI_BYTE, 0, comm);
                            if (errorCode != Unsafe.MPI_SUCCESS)
                                throw Environment.TranslateErrorIntoException(errorCode);
                        }

                        // Copy our own results to the resulting array
                        for (int i = 0; i < counts[0]; ++i)
                            outValues[i] = results[i];
                    }
                }
                else
                {
                    // The number of bytes we should expect from the root
                    int receiveCount = Scatter<int>(0);

                    using (UnmanagedMemoryStream receiveStream = new UnmanagedMemoryStream(receiveCount))
                    {
                        // Receive the serialized form of our part of the result
                        unsafe
                        {
                            int errorCode = Unsafe.MPI_Scatterv(new IntPtr(), null, null, Unsafe.MPI_BYTE,
                                                                receiveStream.Buffer, receiveCount, Unsafe.MPI_BYTE, 0, comm);
                            if (errorCode != Unsafe.MPI_SUCCESS)
                                throw Environment.TranslateErrorIntoException(errorCode);
                        }

                        // Deserialize the incoming stream
                        for (int i = 0; i < counts[Rank]; ++i)
                        {
                            // Seek to the appropriate position in the stream, since we cannot trust the deserializer to stop at the right place.
                            //receiveStream.Position = ???
                            throw new NotImplementedException();
                            outValues[i] = Deserialize<T>(receiveStream);
                        }
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
                    int errorCode = Unsafe.MPI_Reduce_scatter(inHandle.AddrOfPinnedObject(),
                                                              outHandle.AddrOfPinnedObject(),
                                                              counts, datatype, mpiOp.Op, comm);
                    inHandle.Free();
                    outHandle.Free();

                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        ///   <c>Scan</c> is a collective algorithm that performs partial reductions on data provided by
        ///   each process in the communicator. Scan combines the values stored by each process into 
        ///   partial results delivered to each process. The values are combined 
        ///   in a user-defined way, specified via a delegate. If <c>value(0)</c>, <c>value(1)</c>, ..., <c>value(N-1)</c> 
        ///   are the values provided by the N processes in the communicator, the resulting value for the process with 
        ///   rank P will be <c>value(0) op value(1) op ... op value(P)</c>. The processor with rank <c>N-1</c> will
        ///   receive the same result as if one had performed a <see cref="Reduce&lt;T&gt;(T, MPI.ReductionOperation&lt;T&gt;, int)"/> 
        ///   operation with root <c>N-1</c>.
        /// 
        ///   <c>Scan</c> is sometimes called an "inclusive" scan, because the result returned to each process
        ///   includes the contribution of that process. For an "exclusive" scan (that does not include the contribution
        ///   of the calling process in its result), use <see cref="ExclusiveScan&lt;T&gt;(T, MPI.ReductionOperation&lt;T&gt;)"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">The value contributed by the calling process.</param>
        /// <param name="op">Operation used to combine two values from different processes.</param>
        /// <returns>The value <c>value(0) op value(1) op ... op value(Rank)</c></returns>
        public T Scan<T>(T value, ReductionOperation<T> op)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // Gather all of the values to the middle node
                T[] values = Gather(value, 0);
                if (Rank == 0)
                {
                    // This is the root: compute each intermediate result into values
                    for (int p = 1; p < Size; ++p)
                        values[p] = op(values[p - 1], values[p]);
                }

                // Scatter the resulting values
                return Scatter(values, 0);
            }
            else
            {
                T result;
                using (Operation<T> mpiOp = new Operation<T>(op))
                {
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Scan(Memory.LoadAddress(ref value), Memory.LoadAddressOfOut(out result),
                                                        1, datatype, mpiOp.Op, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
                return result;
            }
        }

        /// <summary>
        ///   <c>Scan</c> is a collective algorithm that performs partial reductions on data provided by
        ///   each process in the communicator. Scan combines the arrays stored by each process into 
        ///   partial results delivered to each process. The arrays are combined in a user-defined way, 
        ///   specified via a delegate that will be applied elementwise to the values in each arrays. 
        ///   If <c>array(0)</c>, <c>array(1)</c>, ..., <c>array(N-1)</c> are the arrays provided by the 
        ///   N processes in the communicator, the resulting array for the process with 
        ///   rank P will be <c>array(0) op array(1) op ... op array(P)</c>. The processor with rank <c>N-1</c> will
        ///   receive the same result as if one had performed a 
        ///   <see cref="Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int, ref T[])"/> 
        ///   operation with root <c>N-1</c>.
        /// 
        ///   <c>Scan</c> is sometimes called an "inclusive" scan, because the result returned to each process
        ///   includes the contribution of that process. For an "exclusive" scan (that does not include the contribution
        ///   of the calling process in its result), use <see cref="ExclusiveScan&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;)"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   The array contributed by the calling process. The arrays provided by each process must
        ///    have the same length.
        /// </param>
        /// <param name="op">Operation used to combine two values from different processes.</param>
        /// <returns>The array <c>array(0) op array(1) op ... op array(Rank)</c></returns>
        public T[] Scan<T>(T[] values, ReductionOperation<T> op)
        {
            T[] result = null;
            Scan(values, op, ref result);
            return result;
        }

        /// <summary>
        ///   <c>Scan</c> is a collective algorithm that performs partial reductions on data provided by
        ///   each process in the communicator. Scan combines the arrays stored by each process into 
        ///   partial results delivered to each process. The arrays are combined in a user-defined way, 
        ///   specified via a delegate that will be applied elementwise to the values in each arrays. 
        ///   If <c>array(0)</c>, <c>array(1)</c>, ..., <c>array(N-1)</c> are the arrays provided by the 
        ///   N processes in the communicator, the resulting array for the process with 
        ///   rank P will be <c>array(0) op array(1) op ... op array(P)</c>. The processor with rank <c>N-1</c> will
        ///   receive the same result as if one had performed a 
        ///   <see cref="Reduce&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, int, ref T[])"/> 
        ///   operation with root <c>N-1</c>.
        /// 
        ///   <c>Scan</c> is sometimes called an "inclusive" scan, because the result returned to each process
        ///   includes the contribution of that process. For an "exclusive" scan (that does not include the contribution
        ///   of the calling process in its result), use <see cref="ExclusiveScan&lt;T&gt;(T[], MPI.ReductionOperation&lt;T&gt;, ref T[])"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">
        ///   The array contributed by the calling process. The arrays provided by each process must
        ///    have the same length.
        /// </param>
        /// <param name="op">Operation used to combine two values from different processes.</param>
        /// <param name="outValues">The array <c>array(0) op array(1) op ... op array(Rank)</c></param>
        public void Scan<T>(T[] inValues, ReductionOperation<T> op, ref T[] outValues)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;

            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // Perform a reduction to the middle node
                T[][] allValues = Gather(inValues, 0);
                if (Rank == 0)
                {
                    // Compute each intermediate result in allValues
                    for (int p = 1; p < Size; ++p)
                    {
                        for (int i = 0; i < inValues.Length; ++i)
                            allValues[p][i] = op(allValues[p-1][i], allValues[p][i]);
                    }
                }

                // Scatter the results
                outValues = Scatter(allValues, 0);
            }
            else
            {
                if (outValues == null || outValues.Length != inValues.Length)
                    outValues = new T[inValues.Length];

                using (Operation<T> mpiOp = new Operation<T>(op))
                {
                    GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                    GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                    int errorCode = Unsafe.MPI_Scan(inHandle.AddrOfPinnedObject(),
                                                    outHandle.AddrOfPinnedObject(),
                                                    inValues.Length, datatype, mpiOp.Op, comm);
                    outHandle.Free();
                    inHandle.Free();

                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        /// Scatters an array of values by sending the ith value of the array to processor i. 
        /// This variant of <c>Scatter</c> can only be called by the root process. Other processes
        /// should call the non-root variant of <see cref="Scatter&lt;T&gt;(int)"/>.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   An array of values of length <see cref="Communicator.Size"/>, which is only significant at the root. 
        ///   The ith value of this array (at the root process) will be sent to the ith processor.
        /// </param>
        /// <returns>
        ///   The ith value of the <paramref name="values"/> array provided by the root process, where i is
        ///   the rank of the calling process.
        /// </returns>
        public T Scatter<T>(T[] values)
        {
            return Scatter(true, values, Rank);
        }

         /// <summary>
         ///   Scatters an array of values by sending the ith value of the array to processor i. 
         ///   This variant of <c>Scatter</c> can only be called by a non-root process. The root process
         ///   should either call the root-only variant of <see cref="Scatter&lt;T&gt;(T[])"/> or the
         ///   general <see cref="Scatter&lt;T&gt;(T[], int)"/>.
         /// </summary>
         /// <typeparam name="T">Any serializable type.</typeparam>
         /// <returns>
         /// </returns>
        public T Scatter<T>(int root)
        {
            if (Rank == root)
            {
                throw new ArgumentException("MPI.Communicator.Scatter: this variant of Scatter can only be called by non-root processes");
            }

            return Scatter<T>(false, null, root);
        }

        /// <summary>
        /// Scatters an array of values by sending the ith value of the array to processor i. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   An array of values of length <see cref="Communicator.Size"/>, which is only significant at the root. 
        ///   The ith value of this array (at the root process) will be sent to the ith processor.
        /// </param>
        /// <param name="root">
        ///   Rank of the "root" process, which will supply the array of values to be scattered.
        /// </param>
        /// <returns>
        ///   The ith value of the <paramref name="values"/> array provided by the root process, where i is
        ///   the rank of the calling process.
        /// </returns>
        public T Scatter<T>(T[] values, int root)
        {
            if (Rank == root && (values == null || values.Length != Size))
            {
                throw new ArgumentException("MPI.Communicator.Scatter: values must contain one value for each process.");
            }

            return Scatter<T>((Rank == root), values, root);

        }

        /// <summary>
        /// Scatters an array of values by sending the ith value of the array to processor i.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="isRoot">Whether this process is root (== root for Intracommunicators, or 
        ///   ==Intercommunicator.Root for Intercommunicators).</param>
        /// <param name="values">
        ///   An array of values of length <see cref="Communicator.Size"/>, which is only significant at the root. 
        ///   The ith value of this array (at the root process) will be sent to the ith processor.
        /// </param>
        /// <param name="root">
        ///   Rank of the "root" process, which will supply the array of values to be scattered.
        /// </param>
        /// <returns>
        ///   The ith value of the <paramref name="values"/> array provided by the root process, where i is
        ///   the rank of the calling process.
        /// </returns>
        protected T Scatter<T>(bool isRoot, T[] values, int root)
        {
            if (isRoot && (values == null || values.Length != Size))
            {
                throw new ArgumentException("MPI.Communicator.Scatter: values must contain one value for each process.");
            }

            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the values for transmission.
                SpanTimer.Enter("Scatter");
                var result = Scatter_serialized(isRoot, values, root);
                SpanTimer.Leave("Scatter");
                return result;
            }
            else
            {
                T result;
                int errorCode;
                if (isRoot)
                {
                    unsafe
                    {
                        GCHandle handle = GCHandle.Alloc(values, GCHandleType.Pinned);
                        errorCode = Unsafe.MPI_Scatter(handle.AddrOfPinnedObject(), 1, datatype,
                                                           Memory.LoadAddressOfOut(out result), 1, datatype, root, comm);
                        handle.Free();
                    }
                }
                else
                {
                    unsafe
                    {
                        errorCode = Unsafe.MPI_Scatter(new IntPtr(0), 0, datatype, Memory.LoadAddressOfOut(out result), 1, datatype, root, comm);
                    }
                }
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                return result;
            }
        }

        protected T Scatter_serialized<T>(bool isRoot, T[] values, int root)
        {
            if (Size == 1)
                return values[0];
            else if (SplitLargeObjects)
            {
                return Serialization.ScatterLarge(this, isRoot, values, root);
            }
            else if (isRoot)
            {
                using (UnmanagedMemoryStream stream = new UnmanagedMemoryStream())
                {
                    int[] counts = new int[Size];
                    int[] offsets = new int[Size];

                    for (int dest = 0; dest < counts.Length; ++dest)
                    {
                        // Serialize this value to the stream
                        offsets[dest] = Convert.ToInt32(stream.Length);
                        if (dest != root)
                        {
                            Serialize(stream, values[dest]);
                        }
                        counts[dest] = checked(Convert.ToInt32(stream.Length) - offsets[dest]);
                    }

                    // Scatter the byte counts
                    Scatter<int>(counts);

                    // Scatter the data
                    unsafe
                    {
                        int errorCode = MPI.Unsafe.MPI_Scatterv(stream.Buffer, counts, offsets, Unsafe.MPI_BYTE,
                                                                new IntPtr(), 0, Unsafe.MPI_BYTE, root, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }

                // Extract our own value at the root
                return values[Rank];
            }
            else
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
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes, where multiple items are sent to each process.
        ///   (If the number of items to be sent is different, see 
        ///   <see cref="ScatterFromFlattened&lt;T&gt;(T[], int[], int, ref T[])"/>
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The array to be scattered. Only significant at the root.</param>
        /// <param name="count">
        ///   The number of items to be received by each process. If T is a value type (primitive or
        ///   structure) count must be the same at each process (not just at the root). If T must 
        ///   be serialized, count is ignored at processes other than the root.
        /// </param>
        /// <param name="root">The rank of the root process.</param>      
        public T[] ScatterFromFlattened<T>(T[] inValues, int count, int root)
        {
            T[] result = null;
            ScatterFromFlattened(inValues, count, root, ref result);
            return result;
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes, where the number of data items sent to 
        ///   each process may differ.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The array to be scattered. Only significant at the root.</param>
        /// <param name="counts">
        ///   The number of items to be received by each process. If T must 
        ///   be serialized (i.e. is not a value type), counts is ignored at processes other than the root.
        /// </param>
        /// <param name="root">The rank of the root process.</param>       
        public T[] ScatterFromFlattened<T>(T[] inValues, int[] counts, int root)
        {
            T[] result = null;
            ScatterFromFlattened(inValues, counts, root, ref result);
            return result;
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes, where multiple items are sent to each process.
        ///   (If the number of items to be sent is different, see 
        ///   <see cref="ScatterFromFlattened&lt;T&gt;(T[], int[], int, ref T[])"/>
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The array to be scattered. Only significant at the root.</param>
        /// <param name="count">
        ///   The number of items to be received by each process. If T is a value type (primitive or
        ///   structure) count must be the same at each process (not just at the root). If T must 
        ///   be serialized, count is ignored at processes other than the root.
        /// </param>
        /// <param name="root">The rank of the root process.</param>
        /// <param name="outValues">The array to write to at the receiving process. Does not have to be preallocated.</param>
        public void ScatterFromFlattened<T>(T[] inValues, int count, int root, ref T[] outValues)
        {
            int[] counts = new int[Size];
            for (int i = 0; i < Size; i++)
                counts[i] = count;
            ScatterFromFlattened<T>(inValues, counts, root, ref outValues);
        }

        /// <summary>
        ///   Scatter a one dimensional array to all processes, where the number of data items sent to 
        ///   each process may differ.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The array to be scattered. Only significant at the root.</param>
        /// <param name="counts">
        ///   The number of items to be received by each process. If T must 
        ///   be serialized (i.e. is not a value type), counts is ignored at processes other than the root.
        /// </param>
        /// <param name="root">The rank of the root process.</param>
        /// <param name="outValues">The array to write to at the receiving process. Does not have to be preallocated.</param>
        public void ScatterFromFlattened<T>(T[] inValues, int[] counts, int root, ref T[] outValues)
        {
            if (counts.Length != Size)
                throw new ArgumentException($"counts.Length ({counts.Length}) != Communicator.Size ({Size})");
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                if (Rank == root)
                {
                    T[][] tempIn = new T[Size][];
                    int inLocation = 0;
                    for (int i = 0; i < Size; i++) checked
                    {
                        tempIn[i] = new T[counts[i]];
                        Array.Copy(inValues, inLocation, tempIn[i], 0, counts[i]);
                        inLocation += counts[i];
                    }
                    outValues = Scatter<T[]>(tempIn);

                }
                else
                    outValues = Scatter<T[]>(root);
            }
            else
            {
                if (outValues == null || outValues.Length != counts[Rank])
                    outValues = new T[counts[Rank]];

                if (Rank == root)
                {
                    int[] displs = new int[counts.Length];
                    displs[0] = 0;
                    for (int i = 1; i < counts.Length; i++) checked
                    {
                        displs[i] = displs[i - 1] + counts[i - 1];
                    }
                    int lastIndex = counts.Length - 1;
                    int totalCount = checked(displs[lastIndex] + counts[lastIndex]);
                    if (totalCount > inValues.Length)
                    {
                        throw new ArgumentException($"Sum of counts ({totalCount}) > inValues.Length ({inValues.Length})");
                    }

                    // Pin the array while we are scattering it.
                    GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                    GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                    int errorCode = Unsafe.MPI_Scatterv(inHandle.AddrOfPinnedObject(), counts, displs, datatype,
                                          outHandle.AddrOfPinnedObject(), counts[Rank], datatype, root, comm);
                    inHandle.Free();
                    outHandle.Free();

                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                else
                {
                    // Pin the array while we are scattering it.
                    GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                    int errorCode = Unsafe.MPI_Scatterv(new IntPtr(0), counts, new int[0], datatype,
                                                        outHandle.AddrOfPinnedObject(), counts[Rank], datatype, root, comm);
                    outHandle.Free();

                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }
        #endregion

        #region Process creation
#if PROCESS_CREATION_PRESENT
        
        /// <summary>
        ///   Spawns child MPI processes, providing an <see cref="Intercommunicator"/> for communicating with them.
        /// </summary>
        /// <param name="command">The name of the program to execute.</param>
        /// <param name="argv">A list of arguments to pass to the program.</param>
        /// <param name="maxprocs">The number of processes to be created.</param>
        /// <param name="root">The process from which to execute the child programs.</param>
        public Intercommunicator Spawn(string command, string[] argv, int maxprocs, int root)
        {
            MPI_Comm intercomm;
            unsafe {
                byte[] byte_command = System.Text.Encoding.ASCII.GetBytes(command);
                
                // Copy args into C-style argc/argv
                ASCIIEncoding ascii = new ASCIIEncoding();
                byte** my_argv = stackalloc byte*[argv.Length];
                for (int argidx = 0; argidx < argv.Length; ++argidx)
                {
                    // Copy argument into a byte array (C-style characters)
                    char[] arg = argv[argidx].ToCharArray();
                    fixed (char* argp = arg)
                    {
                        int length = ascii.GetByteCount(arg);
                        byte* c_arg = stackalloc byte[length];
                        if (length > 0)
                        {
                            ascii.GetBytes(argp, arg.Length, c_arg, length);
                        }
                        my_argv[argidx] = c_arg;
                    }
                }
                if (argv == null || argv.Length == 0)
                {
                    //my_argv = Unsafe.MPI_ARGV_NULL;
                    my_argv = (byte**)0;
                }

                fixed (byte* byte_command_p = byte_command)
                {
                        //Unsafe.MPI_Comm_spawn(byte_command_p, my_argv, maxprocs, Unsafe.MPI_INFO_NULL, root, comm, out intercomm, out array_of_errorcodes);
                    Unsafe.MPI_Comm_spawn(byte_command_p, my_argv, maxprocs, Unsafe.MPI_INFO_NULL, root, comm, out intercomm, (int*)0);
                }
            }
            //System.Console.WriteLine("Almost there...");
            //if (world.Rank == 0)
            //    System.Diagnostics.Debugger.Break();
            Intercommunicator retval = Intercommunicator.Adopt(intercomm);
            //System.Console.WriteLine("There!");
            return retval;
        }

#endif

        #endregion
    }
}
