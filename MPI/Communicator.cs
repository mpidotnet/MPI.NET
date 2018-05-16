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
using System.Runtime.InteropServices;

namespace MPI
{
    using System.Collections.Concurrent;
    using System.IO;
    using System.Threading.Tasks;
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
    ///   A reduction operation that combines two values to produce a third value.
    /// </summary>
    ///   
    /// <remarks>
    ///   Reduction operations are used by various collective operations such as 
    ///   <see cref="Intracommunicator.Allreduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;)"/>.
    ///   Note that any operation used as a reduction operation must be associative, 
    ///   e.g., op(x, op(y, z)) == op(op(x, y), z). These operations need not be 
    ///   commutative.
    /// </remarks>
    public delegate T ReductionOperation<T>(T x, T y);

    /// <summary>
    ///   Provides communication among a set of MPI processes.
    /// </summary>
    /// 
    /// <remarks>
    ///   The communicator class abstracts a set of communicating processes in MPI.
    ///   Communicators are the most fundamental types in MPI, because they are the basis
    ///   of all inter-process communication. Each communicator provides a separate communication
    ///   space for a set of processes, so that the messages transmitted with one communicator 
    ///   will not collide with messages transmitted with another communicator. As such, different
    ///   libraries and different tasks can all use MPI without colliding, so long as they are using
    ///   different communicators. There are two important kinds of communicators: intracommunicators 
    ///   and intercommunicators.
    /// 
    ///   <para><see cref="Intracommunicator"/>s are the most commonly used form of communicator. 
    ///   Each intracommunicator contains a set of processes, each of which is identified by its
    ///   "rank" within the communicator. The ranks are numbered 0 through <see cref="Size"/>-1. 
    ///   Any process in the communicator can <see cref="Send&lt;T&gt;(T, int, int)"/> a message 
    ///   to another process
    ///   within the communicator or <see cref="Receive&lt;T&gt;(int, int)"/> a message from any other process in 
    ///   the communicator. Intracommunicators also support a variety of collective operations
    ///   that involve all of the processes in the communicator. Most MPI communication occurs
    ///   within intracommunicators, with very few MPI programs requiring intercommunicators.</para>
    /// 
    ///   <para><see cref="Intercommunicator"/>s differ from intracommunicators in that intercommunicators
    ///   contain two disjoint groups of processes, call them A and B. Any process in group A can send
    ///   a message to or receive a message from any process in group B, and vice-versa. However, there
    ///   is no way to use an intercommunicator to send messages among the processes within a group.
    ///   Intercommunicators are often useful in large MPI applications that tie together many, smaller
    ///   modules. Typically, each module will have its own intracommunicators and the modules will 
    ///   interact with each other via intercommunicators.</para>
    /// </remarks>
    public abstract class Communicator : IDisposable, ICloneable
    {
        /// <summary>
        /// Communicators can only be constructed from other communicators or adopted
        /// from low-level communicators.
        /// </summary>
        internal Communicator()
        {
            comm = Unsafe.MPI_COMM_NULL;
        }

    #region Communicator management
    /// <summary>
    ///   Adopts a low-level MPI communicator that was created with any of the low-level MPI facilities.
    ///   The resulting <c>Communicator</c> object will manage the lifetime of the low-level MPI communicator,
    ///   and will free the communicator when it is disposed or finalized.
    /// </summary>
    /// <remarks>
    ///   This constructor should only be used in rare cases where the program 
    ///   is creating MPI communicators through the low-level MPI interface.
    /// </remarks>
    public static Communicator Adopt(MPI_Comm comm)
        {
            if (comm == Unsafe.MPI_COMM_NULL)
                return null;

            int isIntercommunicator;
            Communicator result;
            unsafe
            {
                int errorCode = Unsafe.MPI_Comm_test_inter(comm, out isIntercommunicator);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            if (isIntercommunicator != 0)
                result = new Intercommunicator();
            else
            {
                int status;
                int errorCode = Unsafe.MPI_Topo_test(comm, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

                if (status == Unsafe.MPI_CART)
                    result = new CartesianCommunicator();
                else if (status == Unsafe.MPI_GRAPH)
                    result = new GraphCommunicator();
                else
                    result = new Intracommunicator();
            }
            result.comm = comm;

            result.AttachToComm();

            return result;
        }

        /// <summary>
        /// Attaches this communicator to the low-level MPI communicator in <c>comm</c>,
        /// setting up the shadow communicator and error handlers as appopriate.
        /// </summary>
        internal virtual void AttachToComm()
        {
            // We need MPI errors to return error codes rather than failing immediately
            Unsafe.MPI_Errhandler_set(comm, Unsafe.MPI_ERRORS_RETURN);

            // Set up the communicator's attributes
            Attributes = new AttributeSet(comm);
        }

        /// <summary>
        /// Finalizer that frees the MPI communicator.
        /// </summary>
        ~Communicator()
        {
            // Free any non-predefined communicators
            if (comm != Unsafe.MPI_COMM_SELF && comm != Unsafe.MPI_COMM_WORLD && comm != Unsafe.MPI_COMM_NULL)
            {
                if (!Environment.Finalized)
                {
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Comm_free(ref comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
                comm = Unsafe.MPI_COMM_NULL;
            }
        }

        /// <summary>
        /// Free the MPI communicator.
        /// </summary>
        public virtual void Dispose()
        {
            Serialization.Dispose();

            // Free any non-predefined communicators
            if (comm != Unsafe.MPI_COMM_SELF && comm != Unsafe.MPI_COMM_WORLD && comm != Unsafe.MPI_COMM_NULL)
            {
                unsafe
                {
                  int errorCode = Unsafe.MPI_Comm_free(ref comm);
                  if (errorCode != Unsafe.MPI_SUCCESS)
                      throw Environment.TranslateErrorIntoException(errorCode);
                }
                comm = Unsafe.MPI_COMM_NULL;
            }

            // We don't need to finalize this object
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///   Creates a new communicator containing all of the processes in the given group. 
        ///   The resulting communicator may be <c>null</c>, if the calling process is not in this group.
        /// </summary>
        /// <param name="group">
        ///   The group of processes that will be contained in the resulting communicator.
        ///   This must be a subgroup of <see cref="Group"/>.
        /// </param>
        public Communicator Create(Group group)
        {
            MPI_Comm newComm;
            unsafe
            {
                int errorCode = Unsafe.MPI_Comm_create(comm, group.group, out newComm);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            return Adopt(newComm);
        }

        /// <summary>
        ///   Clones the communicator, creating a new (but distinct) communicator with the
        ///   same processes. The attributes associated with the communicator are copied
        ///   to the new communicator; which attributes are copied (and how) is dependent
        ///   on how the <see cref="Attribute"/>s were created.
        /// </summary>
        public object Clone()
        {
            // Duplicate the current communicator
            MPI_Comm newComm;
            unsafe
            {
                int errorCode = Unsafe.MPI_Comm_dup(comm, out newComm);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            // Adopt the communicator into MPI.NET
            Communicator result = Adopt(newComm);

            // Copy object attributes from this communicator to the clone
            result.Attributes.CopyAttributesFrom(Attributes);

            return result;
        }

        /// <summary>
        ///   Splits a communicator into several different communicators, each of which is identified
        ///   by a particular color value. 
        /// </summary>
        /// <remarks>
        ///   This is a collective operation, where each process may provide a different 
        ///   <paramref name="color"/> and <paramref name="key"/>. Each distinct color value will 
        ///   correspond to a new communicator, which contains all processes that provided that 
        ///   specific color. The ranks of the processes in the resulting communicators will be 
        ///   determined by the key value, with the lower ranks in the communicator assigned to
        ///   the processes providing the smaller key values; ties are broken based on the ranks of
        ///   the processes in the original communicator.
        /// </remarks>
        /// <param name="color">
        ///    A non-negative color value, designating which new communicator the calling process
        ///    will belong two. All processes that provide the same color will be a part of the 
        ///    same resulting communicator.
        /// </param>
        /// <param name="key">
        ///   A value that determines how processes with the same color are ranked in the resulting
        ///   communicator. Smaller keys correspond to lower ranks, with ties broken by the ranks of
        ///   the processes in the original communicator.
        /// </param>
        /// <returns>
        ///   The communicator that the calling process is in, corresponding to its color value.
        /// </returns>
        public Communicator Split(int color, int key) 
        {
            if (color < 0)
                throw new ArgumentException("color value to Communicator.Split must be non-negative");

            // Split the communicator
            MPI_Comm newComm;
            unsafe
            {
                int errorCode = Unsafe.MPI_Comm_split(comm, color, key, out newComm);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            // Return the new communicator
            return Adopt(newComm);
        }

        /// <summary>
        /// Terminates all processes in this communicator. In most systems, this terminates all processes.
        /// </summary>
        /// <param name="errorcode">An error code that will be returned to the invoking environment.</param>
        public void Abort(int errorcode)
        {
            Unsafe.MPI_Abort(comm, errorcode);
        }
        #endregion

        #region Predefined communicators
        /// <summary>
        ///   The "world" communicator contains all of the processes that were
        ///   originally created by MPI.
        /// </summary>
        /// <remarks>
        ///   Use the "world" communicator to determine
        ///   how many processes the user started for your MPI job, and as the 
        ///   basis for creating the other communicators in your program. Many
        ///   simple programs will use only the "world" communicator.
        /// </remarks>
        public static Intracommunicator world = null; 

        /// <summary>
        ///   The "self" communicator is a simple communicator that contains only
        ///   the currently-executing process. 
        /// </summary>
        /// <remarks>
        ///   Each process will have a different "self" communicator, which only 
        ///   permits communication with itself.
        /// </remarks>
        public static Intracommunicator self = null;
        #endregion

        #region Accessors
        /// <summary>
        ///   Returns the number of processes within this communicator. 
        /// </summary>
        /// <remarks>
        ///   All of the processes within this communicator will have the same 
        ///   value for Size.
        /// </remarks>
        public int Size
        {
            get
            {
                int size;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Comm_size(comm, out size);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                return size;
            }
        }

        /// <summary>
        ///   Returns the rank of the currently executing process within this
        ///   communicator. 
        /// </summary>
        /// <remarks>
        ///   The rank is a number between 0 and Size-1 that is used 
        ///   to identify this process, e.g., to send messages to it. The value of
        ///   Rank will differ from one process to the next.
        /// </remarks>
        public int Rank
        {
            get
            {
                int rank;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Comm_rank(comm, out rank);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                return rank;
            }
        }

        /// <summary>
        ///   Compare two MPI communicators.
        /// </summary>
        /// <list>
        ///   <listheader>
        ///     <term>Value</term>
        ///     <description>Description</description>
        ///   </listheader>
        /// <item>
        ///   <term><see cref="Comparison.Identical"/></term>
        ///   <description>The two <c>Communicator</c> objects represent the same communicator.</description>
        /// </item>
        /// <item>
        ///   <term><see cref="Comparison.Congruent"/></term>
        ///   <description>
        ///     The two <c>Communicator</c> objects contain the same processes with the same ranks,
        ///     but represent different communicators.
        ///   </description>
        /// </item>
        /// <item>
        ///   <term><see cref="Comparison.Similar"/></term>
        ///   <description>
        ///     The two <c>Communicator</c> objects contain the same processes, but with different ranks.
        ///   </description>
        /// </item>
        /// <item>
        ///   <term><see cref="Comparison.Unequal"/></term>
        ///   <descsription>The two <c>Communicator</c> objects are different.</descsription>
        /// </item>
        /// </list>
        public Comparison Compare(Communicator other)
        {
            int result;
            unsafe
            {
                int errorCode = Unsafe.MPI_Comm_compare(this.comm, other.comm, out result);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            return Unsafe.ComparisonFromInt(result);
        }

        /// <summary>
        /// Retrieve the group containing all of the processes in this communicator.
        /// </summary>
        public MPI.Group Group
        {
            get
            {
                MPI_Group group;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Comm_group(comm, out group);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                return MPI.Group.Adopt(group);
            }
        }

        /// <summary>
        /// If true, serialized objects are split into multiple messages of size SerializationBufferSize.
        /// All processes communicating with this Communicator must have the same value.
        /// </summary>
        public bool SplitLargeObjects = true;
        /// <summary>
        /// Properties that control serialization behavior.
        /// </summary>
        public Serialization Serialization { get; } = new Serialization();
        #endregion

        internal void Serialize<T>(Stream stream, T value)
        {
            SpanTimer.Enter("Serialize");
            Serialization.Serializer.Serialize(stream, value);
            SpanTimer.Leave("Serialize");
        }

        internal T Deserialize<T>(Stream stream)
        {
            SpanTimer.Enter("Deserialize");
            var result = Serialization.Serializer.Deserialize<T>(stream);
            SpanTimer.Leave("Deserialize");
            return result;
        }

        #region Point-to-point communication
        /// <summary>
        ///   Send a message to a particular processor.
        /// </summary>
        /// <remarks>
        ///   The basic <c>Send</c> operation will block until this message data has been transferred from <c>value</c>. 
        ///   This might mean that the <c>Send</c> operation will return immediately, before the receiver has actually received
        ///   the data. However, it is also possible that <c>Send</c> won't return until it matches a <see cref="Receive&lt;T&gt;(int, int)"/>
        ///   operation. Thus, the <paramref name="dest"/> parameter should not be equal to <see cref="Rank"/>, because a send-to-self
        ///   operation might never complete. 
        /// </remarks>
        /// <typeparam name="T">The type of the value to be sent. This type must be serializable.</typeparam>
        /// <param name="value">The value that will be transmitted with the message.</param>
        /// <param name="dest">
        ///   The rank of the process that will receive this message. This must be a value in 
        ///   [<c>0</c>, <c>Size-1</c>), but it should not be equal to <see cref="Rank"/>.
        /// </param>
        /// <param name="tag">A message "tag" that identifies this particular kind of message. The receive must use the same message tag to receive this message.</param>
        public void Send<T>(T value, int dest, int tag)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                if (SplitLargeObjects)
                {
                    Serialization.SendLarge(this, value, dest, tag);
                }
                else
                {
                    using (UnmanagedMemoryStream stream = new UnmanagedMemoryStream())
                    {
                        // Serialize the data to a stream
                        Serialize(stream, value);

                        // Send a message containing the size of the serialized data
                        int length = Convert.ToInt32(stream.Length);
                        Send(length, dest, tag);

                        int errorCode = Unsafe.MPI_SUCCESS;
                        if (length > 0)
                        {
                            // Send a message containing the actual serialized data
                            unsafe
                            {
                                errorCode = Unsafe.MPI_Send(stream.Buffer, length, Unsafe.MPI_BYTE, dest, tag, comm);
                            }
                        }

                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
            }
            else
            {
                // We have an MPI datatype for this value type, so transmit it using that MPI datatype.
                unsafe
                {
                    IntPtr ptr = Memory.LoadAddress(ref value);
                    int errorCode = Unsafe.MPI_Send(ptr, 1, datatype, dest, tag, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        ///   Simultaneously send and receive a value from another process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValue">The value to be sent.</param>
        /// <param name="dest">The rank of the process the data will be sent to and received from.</param>
        /// <param name="tag">A message "tag" that identifies this particular kind of message.</param>
        /// <param name="outValue">The value to be received.</param>
        public void SendReceive<T>(T inValue, int dest, int tag, out T outValue)
        {
            CompletedStatus status;
            SendReceive(inValue, dest, tag, dest, tag, out outValue, out status);
        }

        /// <summary>
        ///   Simultaneously send and receive a value from another process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValue">The value to be sent.</param>
        /// <param name="dest">The rank of the process the data will be sent to.</param>
        /// <param name="sendTag">A message "tag" that identifies the particular kind of message being sent.</param>
        /// <param name="source">The rank of the process the data will received from.</param>
        /// <param name="recvTag">A message "tag" that identifies the particular kind of message being received.</param>
        /// <param name="outValue">The value to be received.</param>
        public void SendReceive<T>(T inValue, int dest, int sendTag, int source, int recvTag, out T outValue)
        {
            CompletedStatus status;
            SendReceive(inValue, dest, sendTag, source, recvTag, out outValue, out status);
        }

        /// <summary>
        ///   Simultaneously send and receive a value from another process (which need not be the same).
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValue">The value to be sent.</param>
        /// <param name="dest">The rank of the process the data will be sent to.</param>
        /// <param name="sendTag">A message "tag" that identifies the particular kind of message being sent.</param>
        /// <param name="source">The rank of the process the data will received from.</param>
        /// <param name="recvTag">A message "tag" that identifies the particular kind of message being received.</param>
        /// <param name="outValue">The value to be received.</param>
        /// <param name="status">Receives information about the completed receive operation.</param>
        public void SendReceive<T>(T inValue, int dest, int sendTag, int source, int recvTag, out T outValue, out CompletedStatus status)
        {
            Unsafe.MPI_Status mpiStatus;
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                using (UnmanagedMemoryStream inStream = new UnmanagedMemoryStream())
                {
                    // Serialize the data to a stream
                    Serialize(inStream, inValue);

                    // Send a message containing the size of the serialized data and its
                    // tag on the shadow communicator
                    int inLength = Convert.ToInt32(inStream.Length);
                    int outLength;
                    SendReceive(inLength, dest, sendTag, source, recvTag, out outLength, out status);
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Sendrecv(new IntPtr(&inLength), 1, FastDatatypeCache<int>.datatype, dest, sendTag,
                                                            new IntPtr(&outLength), 1, FastDatatypeCache<int>.datatype, source, recvTag, comm, out mpiStatus);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }

                    using (UnmanagedMemoryStream outStream = new UnmanagedMemoryStream(outLength))
                    {
                        Unsafe.MPI_Status junkStatus;

                        int errorCode = Unsafe.MPI_SUCCESS;
                        if (inLength > 0 || outLength > 0)
                        {
                            // Send a message containing the actual serialized data
                            unsafe
                            {
                                errorCode = Unsafe.MPI_Sendrecv(inStream.Buffer, inLength, Unsafe.MPI_BYTE, dest, sendTag,
                                                                outStream.Buffer, outLength, Unsafe.MPI_BYTE, mpiStatus.MPI_SOURCE, recvTag, comm, out junkStatus);
                            }
                        }

                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                        
                        outValue = Deserialize<T>(outStream);
                    }
                }
            }
            else
            {
                // We have an MPI datatype for this value type, so transmit it using that MPI datatype.
                unsafe
                {
                    IntPtr inPtr = Memory.LoadAddress(ref inValue);
                    IntPtr outPtr = Memory.LoadAddressOfOut(out outValue);
                    int errorCode;
                    errorCode = Unsafe.MPI_Sendrecv(inPtr, 1, datatype, dest, sendTag,
                                                                outPtr, 1, datatype, source, recvTag, comm, out mpiStatus);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
            status = new CompletedStatus(mpiStatus, 1);
        }

        /// <summary>
        ///   Simultaneously send and receive an array of values from another process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The values to be sent.</param>
        /// <param name="dest">The rank of the process the data will be sent to and received from.</param>
        /// <param name="tag">A message "tag" that identifies the particular kind of message being sent and received.</param>
        /// <param name="outValues">An array in which to store the values to be received. The array must be large enough to contain the received data.</param>
        public void SendReceive<T>(T[] inValues, int dest, int tag, ref T[] outValues)
        {
            CompletedStatus status;
            SendReceive(inValues, dest, tag, dest, tag, ref outValues, out status);
        }

        /// <summary>
        ///   Simultaneously send and receive an array of values from another process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The values to be sent.</param>
        /// <param name="dest">The rank of the process the data will be sent to.</param>
        /// <param name="sendTag">A message "tag" that identifies the particular kind of message being sent.</param>
        /// <param name="source">The rank of the process the data will received from.</param>
        /// <param name="recvTag">A message "tag" that identifies the particular kind of message being received.</param>
        /// <param name="outValues">An array in which to store the values to be received. The array must be large enough to contain the received data.</param>
        public void SendReceive<T>(T[] inValues, int dest, int sendTag, int source, int recvTag, ref T[] outValues)
        {
            CompletedStatus status;
            SendReceive(inValues, dest, sendTag, source, recvTag, ref outValues, out status);
        }

        /// <summary>
        ///   Simultaneously send and receive an array of values from another process.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="inValues">The values to be sent.</param>
        /// <param name="dest">The rank of the process the data will be sent to.</param>
        /// <param name="sendTag">A message "tag" that identifies the particular kind of message being sent.</param>
        /// <param name="source">The rank of the process the data will received from.</param>
        /// <param name="recvTag">A message "tag" that identifies the particular kind of message being received.</param>
        /// <param name="outValues">An array in which to store the values to be received. The array must be large enough to contain the received data.</param>
        /// <param name="status">Receives information about the completed receive operation.</param>
        public void SendReceive<T>(T[] inValues, int dest, int sendTag, int source, int recvTag, ref T[] outValues, out CompletedStatus status)
        {
            Unsafe.MPI_Status mpiStatus;
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {

                object valuesObj;
                SendReceive((object)inValues, dest, sendTag, source, recvTag, out valuesObj, out status);
                outValues = (T[])valuesObj;
                status = new CompletedStatus(status.status, outValues.Length);
            }
            else
            {
                int count = 0;
                // We have an MPI datatype for this value type, so transmit it using that MPI datatype.
                GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                int errorCode = Unsafe.MPI_Sendrecv(inHandle.AddrOfPinnedObject(), inValues.Length, datatype, dest, sendTag,
                                                    outHandle.AddrOfPinnedObject(), outValues.Length, datatype, source, recvTag, comm, out mpiStatus);
                inHandle.Free();
                outHandle.Free();
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                errorCode = Unsafe.MPI_Get_count(ref mpiStatus, datatype, out count);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

                status = new CompletedStatus(mpiStatus, count);
            }            
        }


        /// <summary>
        ///   Receive a message from another process. The <c>Receive</c> operation is a blocking operation that will 
        ///   not complete until it has completely received a message. 
        /// </summary>
        /// <typeparam name="T">The type of the value that will be received. This type must be serializable.</typeparam>
        /// <param name="source">
        ///   The process that sent (or that will send) the message. This must be a value in 
        ///   [<c>0</c>, <c>Size-1</c>), but it should not be equal to <see cref="Rank"/>.
        ///   Alternatively, this might have the special value <see cref="anySource"/>, if the message can be
        ///   received from any other process in this communicator.
        /// </param>
        /// <param name="tag">
        ///   A message "tag" that identifies this particular kind of message. Only messages sent with this tag
        ///   will be received by this call. The special value <see cref="anyTag"/> permits messages sent with
        ///   any tag value to be received.
        /// </param>
        /// <returns>
        /// The value received.
        /// </returns>
        public T Receive<T>(int source, int tag)
        {
            T result;
            Receive(source, tag, out result);
            return result;
        }

        /// <summary>
        ///   Receive a message from another process. The <c>Receive</c> operation is a blocking operation that will 
        ///   not complete until it has completely received a message. 
        /// </summary>
        /// <typeparam name="T">The type of the value that will be received. This type must be serializable.</typeparam>
        /// <param name="source">
        ///   The process that sent (or that will send) the message. This must be a value in 
        ///   [<c>0</c>, <c>Size-1</c>), but it should not be equal to <see cref="Rank"/>.
        ///   Alternatively, this might have the special value <see cref="anySource"/>, if the message can be
        ///   received from any other process in this communicator.
        /// </param>
        /// <param name="tag">
        ///   A message "tag" that identifies this particular kind of message. Only messages sent with this tag
        ///   will be received by this call. The special value <see cref="anyTag"/> permits messages sent with
        ///   any tag value to be received.
        /// </param>
        /// <param name="value">
        ///   When <c>Receive</c> completes, this parameter will contain the value
        ///   transmitted as part of the message.
        /// </param>
        public void Receive<T>(int source, int tag, out T value)
        {
            CompletedStatus status;
            Receive(source, tag, out value, out status);
        }

        /// <summary>
        ///   Receive a message from another process. The <c>Receive</c> operation is a blocking operation that will 
        ///   not complete until it has completely received a message. 
        /// </summary>
        /// <typeparam name="T">The type of the value that will be received. This type must be serializable.</typeparam>
        /// <param name="source">
        ///   The process that sent (or that will send) the message. This must be a value in 
        ///   [<c>0</c>, <c>Size-1</c>), but it should not be equal to <see cref="Rank"/>.
        ///   Alternatively, this might have the special value <see cref="anySource"/>, if the message can be
        ///   received from any other process in this communicator.
        /// </param>
        /// <param name="tag">
        ///   A message "tag" that identifies this particular kind of message. Only messages sent with this tag
        ///   will be received by this call. The special value <see cref="anyTag"/> permits messages sent with
        ///   any tag value to be received.
        /// </param>
        /// <param name="value">
        ///   When <c>Receive</c> completes, this parameter will contain the value
        ///   transmitted as part of the message.
        /// </param>
        /// <param name="status">
        /// Receives information about the completed receive operation.
        /// </param>
        public void Receive<T>(int source, int tag, out T value, out CompletedStatus status)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // Since there is no MPI datatype for this type, we will need to receive
                // serialized data.
                if (SplitLargeObjects)
                {
                    Serialization.ReceiveLarge(this, source, tag, out value, out status);
                }
                else
                {
                    // Receive a message containing the size of the serialized data
                    int length;
                    Receive(source, tag, out length, out status);

                    using (UnmanagedMemoryStream stream = new UnmanagedMemoryStream(length))
                    {
                        Unsafe.MPI_Status junkStatus;

                        // Receive the second message containing the serialized data (if any)
                        if (length > 0)
                        {
                            unsafe
                            {
                                int errorCode = Unsafe.MPI_Recv(stream.Buffer, length, Unsafe.MPI_BYTE,
                                                                source, tag, comm, out junkStatus);
                                if (errorCode != Unsafe.MPI_SUCCESS)
                                    throw Environment.TranslateErrorIntoException(errorCode);
                            }
                        }
                        value = Deserialize<T>(stream);
                    }
                }
            }
            else
            {
                // We have an MPI datatype for this value type, so transmit it using that MPI datatype.
                Unsafe.MPI_Status mpiStatus;
                unsafe
                {
                    IntPtr ptr = Memory.LoadAddressOfOut(out value);
                    int errorCode = Unsafe.MPI_Recv(ptr, 1, datatype, source, tag, comm, out mpiStatus);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                status = new CompletedStatus(mpiStatus, 1);
            }
        }

        /// <summary>
        ///   Send a message to a particular processor.
        /// </summary>
        /// <remarks>
        ///   The basic <c>Send</c> operation will block until this message data has been transferred from <c>values</c>. 
        ///   This might mean that the <c>Send</c> operation will return immediately, before the receiver has actually received
        ///   the data. However, it is also possible that <c>Send</c> won't return until it matches a <see cref="Receive&lt;T&gt;(int, int, ref T[])"/>
        ///   operation. Thus, the <paramref name="dest"/> parameter should not be equal to <see cref="Rank"/>, because a send-to-self
        ///   operation might never complete. 
        /// </remarks>
        /// <typeparam name="T">The type of the value to be sent. This type must be serializable.</typeparam>
        /// <param name="values">The values that will be transmitted with the message.</param>
        /// <param name="dest">
        ///   The rank of the process that will receive this message. This must be a value in 
        ///   [<c>0</c>, <c>Size-1</c>), but it should not be equal to <see cref="Rank"/>.
        /// </param>
        /// <param name="tag">A message "tag" that identifies this particular kind of message. The receive must use the same message tag to receive this message.</param>
        public void Send<T>(T[] values, int dest, int tag)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                Send((object)values, dest, tag);
            }
            else
            {
                // We have an MPI datatype for this value type, so transmit it using that MPI datatype.
                // Pin the array so we can make sure it won't move during the send.
                GCHandle handle = GCHandle.Alloc(values, GCHandleType.Pinned);
                int errorCode = Unsafe.MPI_Send(handle.AddrOfPinnedObject(), values.Length, datatype, dest, tag, comm);
                handle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        /// <summary>
        ///   Receive a message containing an array of values from another process.
        ///   
        ///   <para><b>Important</b>: if the type <c>T</c> is a value type (e.g., a primitive type or a struct of primitive types), then 
        ///   the <paramref name="values"/> array must be pre-allocated with enough storage to store all of the
        ///   values that will be received. If the message contains more data than can be received into the
        ///   array, this method will throw a <see cref="MessageTruncatedException"/> or, in some cases, crash.
        ///   Providing an array that is longer than the received message is allowable; to determine the actual
        ///   amount of data received, use <see cref="Receive&lt;T&gt;(int, int, ref T[], out CompletedStatus)"/>.
        ///   Note that this restriction does not apply when <c>T</c> is a reference type that is serialized.</para>
        ///   
        ///   <para>The <c>Receive</c> operation is a blocking operation, that will not complete until it has
        ///   completely received a message. </para>
        /// </summary>
        /// <typeparam name="T">The type of the value that will be received. This type must be serializable.</typeparam>
        /// <param name="source">
        ///   The process that sent (or that will send) the message. This must be a value in 
        ///   [<c>0</c>, <c>Size-1</c>), but it should not be equal to <see cref="Rank"/>.
        ///   Alternatively, this might have the special value <see cref="anySource"/>, if the message can be
        ///   received from any other process in this communicator.
        /// </param>
        /// <param name="tag">
        ///   A message "tag" that identifies this particular kind of message. Only messages sent with this tag
        ///   will be received by this call. The special value <see cref="anyTag"/> permits messages sent with
        ///   any tag value to be received.
        /// </param>
        /// <param name="values">
        ///   When <c>Receive</c> completes, this parameter will contain the values
        ///   transmitted as part of the message. The <paramref name="values"/> array
        ///   must be large enough to contain all of the values that will be received.
        /// </param>
        public void Receive<T>(int source, int tag, ref T[] values)
        {
            CompletedStatus status;
            Receive(source, tag, ref values, out status);
        }

        /// <summary>
        ///   Receive an array from another process.
        ///   
        ///   <para><b>Important</b>: if the type <c>T</c> is a value type (e.g., a primitive type or a struct of primitive types), then 
        ///   the <paramref name="values"/> array must be pre-allocated with enough storage to store all of the
        ///   values that will be received. If the message contains more data than can be received into the
        ///   array, this method will throw a <see cref="MessageTruncatedException"/> or, in some cases, crash.
        ///   Providing an array that is longer than the received message is allowable; to determine the actual
        ///   amount of data received, retrieve the message length from the <paramref name="status"/> result.
        ///   Note that this restriction does not apply when <c>T</c> is a reference type that is serialized.</para>
        ///   
        ///   <para>The <c>Receive</c> operation is a blocking operation, that will not complete until it has
        ///   completely received a message.</para>
        /// </summary>
        /// <typeparam name="T">The type of the value that will be received. This type must be serializable.</typeparam>
        /// <param name="source">
        ///   The process that sent (or that will send) the message. This must be a value in 
        ///   [<c>0</c>, <c>Size-1</c>), but it should not be equal to <see cref="Rank"/>.
        ///   Alternatively, this might have the special value <see cref="anySource"/>, if the message can be
        ///   received from any other process in this communicator.
        /// </param>
        /// <param name="tag">
        ///   A message "tag" that identifies this particular kind of message. Only messages sent with this tag
        ///   will be received by this call. The special value <see cref="anyTag"/> permits messages sent with
        ///   any tag value to be received.
        /// </param>
        /// <param name="values">
        ///   When <c>Receive</c> completes, this parameter will contain the values
        ///   transmitted as part of the message. The <paramref name="values"/> array
        ///   must be large enough to contain all of the values that will be received.
        /// </param>
        /// <param name="status">Contains information about the received message.</param>
        public void Receive<T>(int source, int tag, ref T[] values, out CompletedStatus status)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                Receive(source, tag, out values, out status);
                status = new CompletedStatus(status.status, values.Length);
            }
            else
            {
                // We have an MPI datatype for this value type, so transmit it using that MPI datatype.
                Unsafe.MPI_Status mpiStatus;

                // Pin the array so we can make sure it won't move during the receive.
                GCHandle handle = GCHandle.Alloc(values, GCHandleType.Pinned);
                int result = Unsafe.MPI_Recv(handle.AddrOfPinnedObject(), values.Length, datatype, source, tag, comm, out mpiStatus);
                handle.Free();
                if (result != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(result);
                int count = 0;
                result = Unsafe.MPI_Get_count(ref mpiStatus, datatype, out count);
                if (result != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(result);
                status = new CompletedStatus(mpiStatus, count);
            }
        }

        /// <summary>
        /// Non-blocking send of a single value. This routine will initiate communication and
        /// then return immediately with a <see cref="Request"/> object that can be used to
        /// query the status of the communication. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="value">The value that will be transmitted.</param>
        /// <param name="dest">The rank of the destination process.</param>
        /// <param name="tag">The tag used to identify this message.</param>
        /// <returns>
        ///   A new request object that can be used to manipulate this non-blocking
        ///   communication.
        /// </returns>
        public Request ImmediateSend<T>(T value, int dest, int tag)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                // Serialize the data to a stream
                MemoryStream stream = new MemoryStream();
                Serialize(stream, value);
                return new SerializedSendRequest(this, dest, tag, stream.GetBuffer(), Convert.ToInt32(stream.Length), 1);
            }
            else
            {
                // We have an MPI datatype for this value type, so transmit it using that MPI datatype.
                // However, we need this value to go onto the heap, so box it first, pin that object, 
                // and send the pinned value.
                object valueObj = value;
                GCHandle handle = GCHandle.Alloc(valueObj, GCHandleType.Pinned);
                MPI_Request request;
                int errorCode = Unsafe.MPI_Isend(handle.AddrOfPinnedObject(), 1, datatype, dest, tag, comm, out request);
                if (errorCode != Unsafe.MPI_SUCCESS)
                {
                    handle.Free();
                    throw Environment.TranslateErrorIntoException(errorCode);
                }

                return new ValueTypeSendRequest(request, 1, handle);
            }
        }

        /// <summary>
        /// Non-blocking send of an array of data. This routine will initiate communication and
        /// then return immediately with a <see cref="Request"/> object that can be used to
        /// query the status of the communication. 
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="values">
        ///   The array of values that will be transmitted. If <c>T</c> is a value type, 
        ///   do not change the values in this array before the resulting request has been 
        ///   completed, because the implementation may send these values at any time.
        /// </param>
        /// <param name="dest">The rank of the destination process.</param>
        /// <param name="tag">The tag used to identify this message.</param>
        /// <returns>
        ///   A new request object that can be used to manipulate this non-blocking
        ///   communication.
        /// </returns>
        public Request ImmediateSend<T>(T[] values, int dest, int tag)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the values for transmission.
                // Serialize the data to a stream
                MemoryStream stream = new MemoryStream();
                Serialize(stream, values);
                return new SerializedSendRequest(this, dest, tag, stream.GetBuffer(), Convert.ToInt32(stream.Length), values.Length);
            }
            else
            {
                // We have an MPI datatype for this value type, so transmit it using that MPI datatype.
                GCHandle handle = GCHandle.Alloc(values, GCHandleType.Pinned);
                MPI_Request request;
                int errorCode = Unsafe.MPI_Isend(handle.AddrOfPinnedObject(), 
                                                 values.Length, datatype, dest, tag, comm, out request);
                if (errorCode != Unsafe.MPI_SUCCESS)
                {
                    handle.Free();
                    throw Environment.TranslateErrorIntoException(errorCode);
                }

                return new ValueTypeSendRequest(request, values.Length, handle);
            }
        }

        /// <summary>
        /// Non-blocking receive of a single value. This routine will initiate a request to receive
        /// data and then return immediately. The data may be received in the background. To test for
        /// or force the completion of the communication, then access the received data, use the 
        /// returned <see cref="ReceiveRequest"/> object.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="source">
        ///   Rank of the source process to receive data from. Alternatively, use <see cref="anySource"/> to
        ///   receive a message from any other process.
        /// </param>
        /// <param name="tag">
        ///   The tag that identifies the message to be received. Alternatively, use <see cref="anyTag"/>
        ///   to receive a message with any tag.
        /// </param>
        /// <returns>
        ///   A request object that allows one to test or force the completion of this receive request,
        ///   and retrieve the resulting value.
        /// </returns>
        public ReceiveRequest ImmediateReceive<T>(int source, int tag)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                return new SerializedReceiveRequest<T>(this, source, tag);
            }
            else
                return new ValueReceiveRequest<T>(comm, source, tag);
        }

        /// <summary>
        /// Non-blocking receive of a single value. This routine will initiate a request to receive
        /// data and then return immediately. The data may be received in the background. To test for
        /// or force the completion of the communication, then access the received data, use the 
        /// returned <see cref="ReceiveRequest"/> object.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="source">
        ///   Rank of the source process to receive data from. Alternatively, use <see cref="anySource"/> to
        ///   receive a message from any other process.
        /// </param>
        /// <param name="tag">
        ///   The tag that identifies the message to be received. Alternatively, use <see cref="anyTag"/>
        ///   to receive a message with any tag.
        /// </param>
        /// <param name="action">
        ///   A delegate to be invoked with the received value, if any.
        /// </param>
        /// <returns>
        ///   A request object that allows one to test or force the completion of this receive request,
        ///   and retrieve the resulting value.
        /// </returns>
        public ReceiveRequest ImmediateReceive<T>(int source, int tag, Action<T> action)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                return new SerializedReceiveRequest<T>(this, source, tag, action);
            }
            else
                return new ValueReceiveRequest<T>(comm, source, tag, action);
        }

        /// <summary>
        /// Non-blocking receive of an array of values. This routine will initiate a request to receive
        /// data and then return immediately. The data may be received in the background. To test for
        /// or force the completion of the communication, then access the received data, use the 
        /// returned <see cref="ReceiveRequest"/> object.
        /// 
        ///   <para><b>Important</b>: if the type <c>T</c> is a value type (e.g., a primitive type or a struct of primitive types), then 
        ///   the <paramref name="values"/> array must be pre-allocated with enough storage to store all of the
        ///   values that will be received. If the message contains more data than can be received into the
        ///   array, this method will throw a <see cref="MessageTruncatedException"/> or, in some cases, crash.
        ///   Providing an array that is longer than the received message is allowable; to determine the actual
        ///   amount of data received, retrieve the message length from the <see cref="CompletedStatus"/>
        ///   structure provided when the request is completed.</para>
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="source">
        ///   Rank of the source process to receive data from. Alternatively, use <see cref="anySource"/> to
        ///   receive a message from any other process.
        /// </param>
        /// <param name="tag">
        ///   The tag that identifies the message to be received. Alternatively, use <see cref="anyTag"/>
        ///   to receive a message with any tag.
        /// </param>
        /// <param name="values">
        ///   An array into which the values will be received. This array must be large enough to
        ///   accommodate all of the data sent by the source process.
        /// </param>
        /// <returns>
        ///   A request object that allows one to test or force the completion of this receive request,
        ///   and retrieve the resulting value. The object retrieved from the request object will be
        ///   <paramref name="values"/>.
        /// </returns>
        public ReceiveRequest ImmediateReceive<T>(int source, int tag, T[] values)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                return new SerializedArrayReceiveRequest<T>(this, source, tag, values);
            }
            else
                return new ValueArrayReceiveRequest<T>(comm, source, tag, values);
        }

        /// <summary>
        /// Wait for a message from the given source and with the specified tag to become
        /// available, but don't try to receive the message. This routine will wait indefinitely
        /// for a message meeting the given criteria to arrive, so it should only be invoked
        /// when you know a message is coming. If you just want to check whether a message is
        /// available use <see cref="ImmediateProbe"/>.
        /// </summary>
        /// <param name="source">
        ///   The process that sent (or that will send) the message. This must be a value in 
        ///   [<c>0</c>, <c>Size-1</c>), or the special value <see cref="anySource"/>. If it is
        ///   <see cref="anySource"/>, then we can match a message sent by any other process in
        ///   this communicator.
        /// </param>
        /// <param name="tag">
        ///   A message "tag" that identifies this particular kind of message. Only messages sent with this tag
        ///   will be matched by this call. The special value <see cref="anyTag"/> permits messages sent with
        ///   any tag value to be received.
        /// </param>
        /// <returns>A <see cref="Status"/> object containing information about the message.</returns>
        public Status Probe(int source, int tag)
        {
            Unsafe.MPI_Status status;
            unsafe
            {
                int errorCode = Unsafe.MPI_Probe(source, tag, comm, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            return new Status(status);
        }

        /// <summary>
        /// Determine whether a message from the given source and with the specified tag is
        /// available, but don't try to receive the message. This routine will return 
        /// immediately, regardless of whether a message is available, so it is useful for
        /// polling to determine whether any messages need to be handled at this time. If
        /// your program can't do any work until a message arrives (and the message is 
        /// guaranteed to arrive, eventually), use <see cref="Probe"/> instead.
        /// </summary>
        /// <param name="source">
        ///   The process that sent (or that will send) the message. This must be a value in 
        ///   [<c>0</c>, <c>Size-1</c>), or the special value <see cref="anySource"/>. If it is
        ///   <see cref="anySource"/>, then we can match a message sent by any other process in
        ///   this communicator.
        /// </param>
        /// <param name="tag">
        ///   A message "tag" that identifies this particular kind of message. Only messages sent with this tag
        ///   will be matched by this call. The special value <see cref="anyTag"/> permits messages sent with
        ///   any tag value to be received.
        /// </param>
        /// <returns>
        ///   If a message is available, a <see cref="Status"/> object containing information about that message.
        ///   Otherwise, returns <c>null</c>.
        /// </returns>
        public Status ImmediateProbe(int source, int tag)
        {
            Unsafe.MPI_Status status;
            int flag;
            unsafe
            {
                int errorCode = Unsafe.MPI_Iprobe(source, tag, comm, out flag, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            if (flag != 0)
                return new Status(status);
            else
                return null;
        }
        #endregion

        #region Collective communication
        /// <summary>
        ///   Wait until all processes in the communicator have reached the same barrier.
        /// </summary>
        /// <remarks>
        ///   This collective operation should be called by all processes within the communicator.
        ///   Each process will wait within the barrier until all processes have entered the barrier,
        ///   at which point all of the processes will be released. Use barriers when all of the 
        ///   processes need to synchronize at some particular point in your program flow.
        /// </remarks>
        /// <example>
        ///   Barriers are often used after each process has completed some large, independent
        ///   chunk of work:
        /// <code>
        ///   public void Superstep(Communicator comm) 
        ///   {
        ///     // Perform a large chunk of work  locally
        ///     DoLocalWork();
        /// 
        ///     // Synchronize with everyone else
        ///     comm.Barrier();
        /// 
        ///     // Okay, move on to the next piece of work.
        ///   }
        /// </code>
        /// </example>
        public void Barrier()
        {
            SpanTimer.Enter("Barrier");
            int errorCode = Unsafe.MPI_Barrier(comm);
            if (errorCode != Unsafe.MPI_SUCCESS)
                throw Environment.TranslateErrorIntoException(errorCode);
            SpanTimer.Leave("Barrier");
        }

        /// <summary>
        /// Broadcast a value from the <paramref name="root"/> process to all other processes.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="isRoot">
        ///   Whether this is the root process or not.
        /// </param>
        /// <param name="value">
        ///   The value to be broadcast. At the <paramref name="root"/> process, this value is
        ///   read (but not written); at all other processes, this value will be replaced with
        ///   the value at the root.
        /// </param>
        /// <param name="root">
        ///   The rank of the process that is broadcasting the value out to
        ///   all of the non-<paramref name="root"/> processes.
        /// </param>
        internal void Broadcast_impl<T>(bool isRoot, ref T value, int root)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                Broadcast_impl_serialized(isRoot, ref value, root);
            }
            else
            {
                // Broadcast the data directly
                unsafe
                {
                    int errorCode = Unsafe.MPI_Bcast(Memory.LoadAddress(ref value), 1, datatype, root, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        /// Broadcast an array from the <paramref name="root"/> process to all other processes.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="isRoot">
        ///   Whether this is the root process.
        /// </param>
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
        internal void Broadcast_impl<T>(bool isRoot, ref T[] values, int root)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                Broadcast_impl_serialized(isRoot, ref values, root);
            }
            else
            {
                GCHandle handle = GCHandle.Alloc(values, GCHandleType.Pinned);
                int errorCode = Unsafe.MPI_Bcast(handle.AddrOfPinnedObject(), values.Length, datatype, root, comm);
                handle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        private void Broadcast_impl_serialized<T>(bool isRoot, ref T value, int root)
        {
            SpanTimer.Enter("Broadcast");
            using (UnmanagedMemoryStream stream = new UnmanagedMemoryStream())
            {
                if (isRoot)
                {
                    // Serialize the data to a stream
                    Serialize(stream, value);
                }

                // Broadcast the size of the serialized data
                int length = Convert.ToInt32(stream.Length);
                unsafe
                {
                    int errorCode = Unsafe.MPI_Bcast(new IntPtr(&length), 1, Unsafe.MPI_INT, root, comm);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }

                if (!isRoot)
                {
                    // Allocate memory for the receive buffer
                    stream.SetLength(length);
                }

                // Broadcast the serialized data
                if (length > 0)
                {
                    // Send a message containing the actual serialized data
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Bcast(stream.Buffer, length, Unsafe.MPI_BYTE, root, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }

                if (!isRoot)
                {
                    // Deserialize the data from the stream
                    value = Deserialize<T>(stream);
                }
            }
            SpanTimer.Leave("Broadcast");
        }

        /// <summary>
        /// Gather the values from each process into an array of values at the 
        /// <paramref name="root"/> process. This version of Gather does not distinguish
        /// between Intra- and Intercommunicators, and should be used as a base for Gather
        /// in derived communicators.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="isRoot">Whether this process is root (== root for Intracommunicators, or 
        ///   ==Intercommunicator.Root for Intercommunicators).</param>
        /// <param name="size">
        ///   The number of processes from which the root will collect information.
        /// </param>
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
        internal void Gather_impl<T>(bool isRoot, int size, T inValue, int root, ref T[] outValues)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (datatype == Unsafe.MPI_DATATYPE_NULL)
            {
                SpanTimer.Enter("Gather");
                // There is no associated MPI datatype for this type, so we will
                // need to serialize the value for transmission.
                if (SplitLargeObjects)
                    Serialization.GatherLarge(this, isRoot, size, inValue, root, ref outValues);
                else
                    Gather_impl_serialized(isRoot, size, inValue, root, ref outValues);
                SpanTimer.Leave("Gather");
                return;
            }
            if (!isRoot)
            {
                int errorCode = Unsafe.MPI_Gather(Memory.LoadAddress(ref inValue), 1, datatype, new IntPtr(), 0, datatype, root, comm);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            else
            {
                if (outValues == null || outValues.Length != size)
                    outValues = new T[size];

                // Pin the array while we are gathering into it.
                GCHandle handle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                int errorCode = Unsafe.MPI_Gather(Memory.LoadAddress(ref inValue), 1, datatype,
                                      handle.AddrOfPinnedObject(), 1, datatype, root, comm);
                handle.Free();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        private void Gather_impl_serialized<T>(bool isRoot, int size, T inValue, int root, ref T[] outValues)
        {
            if (!isRoot)
            {
                using (UnmanagedMemoryStream stream = new UnmanagedMemoryStream())
                {
                    // Serialize the data to a stream
                    Serialize(stream, inValue);

                    // Root gathers lengths
                    int[] temp = null;
                    Gather_impl<int>(isRoot, size, Convert.ToInt32(stream.Length), root, ref temp);

                    // Root gathers bytes
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Gatherv(stream.Buffer, Convert.ToInt32(stream.Length), Unsafe.MPI_BYTE,
                                                         new IntPtr(), null, null, Unsafe.MPI_BYTE, root, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
            }
            else
            {
                if (outValues == null || outValues.Length != size)
                    outValues = new T[size];

                int[] counts = null;
                Gather_impl<int>(isRoot, size, 0, root, ref counts);
                int[] offsets = new int[size];

                // Compute offsets and total bytes
                int totalBytes = 0;
                for (int i = 0; i < counts.Length; ++i) checked
                {
                    offsets[i] = totalBytes;
                    totalBytes += counts[i];
                }

                using (UnmanagedMemoryStream stream = new UnmanagedMemoryStream(totalBytes))
                {
                    // Gather all of the serialized data
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Gatherv(new IntPtr(), 0, Unsafe.MPI_BYTE,
                                                           stream.Buffer, counts, offsets, Unsafe.MPI_BYTE, root, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }

                    // De-serialize the data
                    for (int source = 0; source < size; ++source)
                    {
                        if (source == root)
                            // Just copy the locally-provided value
                            outValues[source] = inValue;
                        else
                        {
                            // De-serialize from the stream
                            // To be safe, we set the position since some deserializers will read beyond the end of the serialized object.
                            stream.Position = offsets[source];
                            outValues[source] = Deserialize<T>(stream);
                        }
                    }
                }
            }
        }

        /// <summary>
        ///   This is an internal implementation of the GatherFlattened methods, designed to be called by derived classes.
        /// </summary>
        /// <typeparam name="T">Any serializable type.</typeparam>
        /// <param name="isRoot">Whether this process is the root process.</param>
        /// <param name="size">The number of processes to gather from.</param>
        /// <param name="inValues">The values contributed by this process, if any.</param>
        /// <param name="counts">The number of elements to be gathered from each process. This parameter is only significant at the root process.</param>
        /// <param name="root">The rank of the root process.</param>
        /// <param name="outValues">An array to store the gathered values in. If null or shorter than the total count, a new array will be allocated.</param>
        internal void GatherFlattened_impl<T>(bool isRoot, int size, T[] inValues, int[] counts, int root, ref T[] outValues)
        { 
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (!isRoot)
            {
                if (datatype == Unsafe.MPI_DATATYPE_NULL)
                {
                    T[][] tempOut = null;
                    Gather_impl<T[]>(isRoot, size, inValues, root, ref tempOut);
                }
                else
                {
                    // Pin the array while we are gathering out of it.
                    GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                    int errorCode = Unsafe.MPI_Gatherv(inHandle.AddrOfPinnedObject(), inValues.Length, datatype, new IntPtr(), null, null, datatype, root, comm);
                    inHandle.Free();

                    if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
            else
            {
                if (counts.Length != Size)
                    throw new ArgumentException($"counts.Length ({counts.Length}) != Communicator.Size ({Size})");
                int totalRecvSize = 0;
                // we must use checked addition in case the total count exceeds 2 billion.
                for (int i = 0; i < counts.Length; i++) checked
                {
                    totalRecvSize += counts[i];
                }

                if (outValues == null || outValues.Length < totalRecvSize)
                    outValues = new T[totalRecvSize];

                if (datatype == Unsafe.MPI_DATATYPE_NULL)
                {
                    T[][] tempOut = new T[counts.Length][];

                    Gather_impl<T[]>(isRoot, size, inValues, root, ref tempOut);

                    int cumulativeCount = 0;
                    for (int source = 0; source < counts.Length; ++source) checked
                    {
                        tempOut[source].CopyTo(outValues, cumulativeCount);
                        cumulativeCount += counts[source];
                    }
                }
                else
                {
                    int[] displs = new int[counts.Length];
                    displs[0] = 0;
                    for (int i = 1; i < counts.Length; i++)
                        displs[i] = checked(displs[i - 1] + counts[i - 1]);

                    // Pin the array while we are gathering into it.
                    GCHandle inHandle = GCHandle.Alloc(inValues, GCHandleType.Pinned);
                    GCHandle outHandle = GCHandle.Alloc(outValues, GCHandleType.Pinned);
                    int errorCode = Unsafe.MPI_Gatherv(inHandle.AddrOfPinnedObject(), inValues.Length, datatype,
                                          outHandle.AddrOfPinnedObject(), counts, displs, datatype, root, comm);
                    inHandle.Free();
                    outHandle.Free();

                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
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
        ///   <see cref="Intracommunicator.Reduce&lt;T&gt;(T, MPI.ReductionOperation&lt;T&gt;, int)"/>.
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
        ///       Communicator world = Communicator.world;
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
        /// <param name="isRoot">
        ///   Whether this process is the root process.
        /// </param>
        /// <param name="size">
        ///   The number of items that will be collected.
        /// </param>
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
        internal T Reduce_impl<T>(bool isRoot, int size, T value, ReductionOperation<T> op, int root)
        {
            MPI_Datatype datatype = FastDatatypeCache<T>.datatype;
            if (!isRoot)
            {
                if (datatype == Unsafe.MPI_DATATYPE_NULL)
                {
                    // Gather values at the root
                    T[] temp = null;
                    Gather_impl(isRoot, size, value, root, ref temp);
                }
                else
                {
                    // Use the low-level MPI reduction operation from a non-root
                    using (Operation<T> mpiOp = new Operation<T>(op))
                    {
                        unsafe
                        {
                            int errorCode = Unsafe.MPI_Reduce(Memory.LoadAddress(ref value), new IntPtr(),
                                                              1, datatype, mpiOp.Op, root, comm);
                            if (errorCode != Unsafe.MPI_SUCCESS)
                                throw Environment.TranslateErrorIntoException(errorCode);
                        }
                    }
                }

                return default(T);
            }
            else
            {
                T result;
                if (datatype == Unsafe.MPI_DATATYPE_NULL)
                {
                    // Gather into a temporary array
                    T[] values = null;
                    Gather_impl(isRoot, size, value, root, ref values);

                    SpanTimer.Enter("Reduce");
                    // Perform reduction locally
                    result = values[0];
                    for (int p = 1; p < size; ++p)
                        result = op(result, values[p]);
                    SpanTimer.Leave("Reduce");
                }
                else
                {
                    // Use the low-level MPI reduction operation from the root
                    using (Operation<T> mpiOp = new Operation<T>(op))
                    {
                        int errorCode = Unsafe.MPI_Reduce(Memory.LoadAddress(ref value), Memory.LoadAddressOfOut(out result),
                                                          1, datatype, mpiOp.Op, root, comm);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }

                return result;
            }
        }
        #endregion

        /// <summary>
        /// The set of attributes attached to this communicator.
        /// </summary>
        public AttributeSet Attributes;

        /// <summary>
        /// The low-level MPI communicator handle.
        /// </summary>
        internal MPI_Comm comm;

        /// <summary>
        ///   Special value for the <c>source</c> argument to <c>Receive</c> that
        ///   indicates that the message can be received from any process in the communicator.
        /// </summary>
        public static int anySource = Unsafe.MPI_ANY_SOURCE;

        /// <summary>
        ///   Special value for the <c>tag</c> argument to <c>Receive</c> that
        ///   indices that the message with any tag can be received by the receive operation.
        /// </summary>
        public static int anyTag = Unsafe.MPI_ANY_TAG;

        #region Process Creation and Management
#if PROCESS_CREATION_PRESENT
        /// <summary>
        ///   Provides the parent communicator. Otherwise, returns null.
        /// </summary>
        public Intercommunicator Parent
        {
            get
            {
                MPI_Comm comm;
                Unsafe.MPI_Comm_get_parent(out comm);
                if (comm != Unsafe.MPI_COMM_NULL)
                {
                    return Intercommunicator.Adopt(comm);
                    //return null;
                }
                else
                    return null;
            }
        }
#endif
        #endregion

        #region Protected members
        
        /// <summary>
        ///   Convert an array of integers to an array of booleans. Used for some low-level calls 
        ///   that return an array of logical ints to express boolean values.
        /// </summary>
        /// <param name="arr">
        ///   An array to convert.
        /// </param>
        /// <returns>
        ///   An array of bools corresponding to the logical values in the input array.
        /// </returns>
        protected static bool[] IntToBool(int[] arr)
        {
            bool[] retarr = new bool[arr.Length];
            int i;
            for (i = 0; i < arr.Length; i++)
                retarr[i] = Convert.ToBoolean(arr[i]);
            return retarr;
        }

        /// <summary>
        ///   Convert an array of booleans to an array of integers. Used for some low-level calls 
        ///   that require an array of logical ints to express boolean values.
        /// </summary>
        /// <param name="arr">
        ///   An array to convert.
        /// </param>
        /// <returns>
        ///   An array of ints with values of 0 and/or 1 corresponding to the values in the input array.
        /// </returns>
        protected static int[] BoolToInt(bool[] arr)
        {
            int[] retarr = new int[arr.Length];
            int i;
            for (i = 0; i < arr.Length; i++)
                retarr[i] = Convert.ToInt32(arr[i]);
            return retarr;
        }
        #endregion
    }
}
