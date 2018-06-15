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
using System.IO;

namespace MPI
{
    using System.Threading;
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
    /// A non-blocking communication request.
    /// </summary>
    /// <remarks>
    /// Each request object refers to a single
    /// communication operation, such as non-blocking send 
    /// (see <see cref="Communicator.ImmediateSend&lt;T&gt;(T, int, int)"/>)
    /// or receive. Non-blocking operations may progress in the background, and can complete
    /// without any user intervention. However, it is crucial that outstanding communication
    /// requests be completed with a successful call to <see cref="Wait"/> or <see cref="Test"/>
    /// before the request object is lost.
    /// </remarks>
    public abstract class Request
    {
        /// <summary>
        /// Wait until this non-blocking operation has completed.
        /// </summary>
        /// <returns>
        ///   Information about the completed communication operation.
        /// </returns>
        public abstract CompletedStatus Wait();

        /// <summary>
        /// Determine whether this non-blocking operation has completed.
        /// </summary>
        /// <returns>
        /// If the non-blocking operation has completed, returns information
        /// about the completed communication operation. Otherwise, returns
        /// <c>null</c> to indicate that the operation has not completed.
        /// </returns>
        public abstract CompletedStatus Test();

        /// <summary>
        /// Cancel this communication request.
        /// </summary>
        public abstract void Cancel();
    }

    /// <summary>
    /// A non-blocking receive request. 
    /// </summary>
    /// <remarks>
    /// This class allows one to test a receive
    /// request for completion, wait for completion of a request, cancel a request,
    /// or extract the value received by this communication request.
    /// </remarks>
    public abstract class ReceiveRequest : Request
    {
        /// <summary>
        /// Retrieve the value received via this communication. The value
        /// will only be available when the communication has completed.
        /// </summary>
        /// <returns>The value received by this communication.</returns>
        public abstract object GetValue();
    };

    /// <summary>
    /// A request that corresponds to a non-blocking send of either a 
    /// single value or an array of values, where the type of the value
    /// (or values) is a value type with an associated MPI datatype.
    /// Underneath, this request is a single <c>MPI_Request</c> object
    /// coupled with a handle to pinned memory and some additional 
    /// information about the send itself.
    /// </summary>
    class ValueTypeSendRequest : Request
    {
        /// <summary>
        /// Initialize a single request for a non-blocking send that has already been initiated.
        /// </summary>
        /// <param name="request">The request object.</param>
        /// <param name="count">The number of elements transmitted in this communication.</param>
        /// <param name="handle">A handle pointing to pinned memory that will be unpinned after
        /// this operation completes.</param>
        internal ValueTypeSendRequest(MPI_Request request, int count, GCHandle handle)
        {
            this.request = request;
            this.count = count;
            this.handle = handle;
            this.cachedStatus = null;
        }

        public override CompletedStatus Wait()
        {
            if (cachedStatus != null)
                return cachedStatus;

            Unsafe.MPI_Status status;
            unsafe
            {
                // Wait until the request completes.
                // On normal completion, this will set request to null.
                int errorCode = Unsafe.MPI_Wait(ref request, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            Cleanup();

            cachedStatus = new CompletedStatus(status, count);
            return cachedStatus;
        }

        public override CompletedStatus Test()
        {
            if (cachedStatus != null)
                return cachedStatus;

            Unsafe.MPI_Status status;
            int flag;
            unsafe
            {
                // Wait until the request completes
                int errorCode = Unsafe.MPI_Test(ref request, out flag, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

                if (flag == 0)
                    // This request has not completed
                    return null;
            }

            Cleanup();

            cachedStatus = new CompletedStatus(status, count);
            return cachedStatus;
        }

        public override void Cancel()
        {
            if (cachedStatus == null)
            {
                Unsafe.MPI_Status status;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Cancel(ref request);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);

                    int flag;
                    errorCode = Unsafe.MPI_Test(ref request, out flag, out status);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);

                    if (flag != 0)
                        cachedStatus = new CompletedStatus(status, 0);
                }
            }
            Cleanup();
        }

        ~ValueTypeSendRequest()
        {
            if (request != Unsafe.MPI_REQUEST_NULL)
            {
                // We are in trouble. The user no longer has any references to 
                // this object, but the communication has not completed. Our
                // handle has the memory associated with the communication pinned.
                // If we unpin the memory, the communication could fail.
                // If we leave the memory pinned, it will never be unpinned and
                // will probably leak.
                //
                // We choose to write a nasty error message to the console and
                // permit the leak.
                System.Console.Error.WriteLine("ERROR: Invalid use of MPI.Request object; your application may leak memory.");
                System.Console.Error.WriteLine("To fix this problem, you must complete non-blocking communications explicitly.");
            }
        }

        /// <summary>
        /// Cleanup any resources that we're currently holding. This typically
        /// involves unpinning the memory associated with this request.
        /// </summary>
        protected void Cleanup()
        {
            if (request != Unsafe.MPI_REQUEST_NULL)
            {
                throw new Exception("Called Cleanup when request is not null");
            }

            // Unpin the memory associated with this transaction
            handle.Free();

            // Suppress finalization; there will be nothing to do.
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// If we've already completed this operation, this caches the status information
        /// so we can return it again later.
        /// </summary>
        protected CompletedStatus cachedStatus;

        /// <summary>
        /// The underlying MPI request object that provides a handle to this
        /// non-blocking send.
        /// </summary>
        protected MPI_Request request;

        /// <summary>
        /// The number of elements sent by this operation. 
        /// </summary>
        protected int count;

        /// <summary>
        /// Handle to the pinned memory used in this request.
        /// </summary>
        protected GCHandle handle;
    }

    /// <summary>
    /// A structure storing two requests. Use this to keep the two requests in memory
    /// next to each other, so that we can complete or test them with a single MPI call.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    struct TwoRequests
    {
      public MPI_Request body;
      public MPI_Request header;
    }

    /// <summary>
    /// A request for a serialized send, which typically consists of two <c>MPI_Request</c> objects.
    /// This class manages pinning and unpinning the data for both send requests, and testing/completing
    /// both requests in the same operation.
    /// </summary>
    class SerializedSendRequest : Request
    {
        /// <summary>
        /// Initiate an MPI non-blocking send operation managed by this Request object.
        /// </summary>
        /// <param name="comm">The communicator over which the initial message will be sent.</param>
        /// <param name="dest">The destination rank for this message.</param>
        /// <param name="tag">The message tag.</param>
        /// <param name="buffer">The bytes that should be transmitted.  Must not be modified by the caller.</param>
        /// <param name="byteCount">The number of bytes from the beginning of buffer that should be transmitted.</param>
        /// <param name="count">The number of serialized objects stored in <paramref name="buffer"/>.</param>
        internal SerializedSendRequest(Communicator comm, int dest, int tag, byte[] buffer, int byteCount, int count)
        {
            this.count = count;
            this.cachedStatus = null;

            // Create the message header containing the size of the serialized data
            this.headerObj = byteCount;

            // Pin down this object and initiate the send of the length
            headerHandle = GCHandle.Alloc(headerObj, GCHandleType.Pinned);
            int errorCode;
            unsafe
            {
                errorCode = Unsafe.MPI_Isend(headerHandle.AddrOfPinnedObject(), 1, FastDatatypeCache<int>.datatype,
                                                 dest, tag, comm.comm, out requests.header);
            }

            if (errorCode != Unsafe.MPI_SUCCESS)
            {
                headerHandle.Free();
                throw Environment.TranslateErrorIntoException(errorCode);
            }

            if (byteCount > 0)
            {
                this.bufferHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                // Initiate a send of the serialized data
                unsafe
                {
                    errorCode = Unsafe.MPI_Isend(bufferHandle.AddrOfPinnedObject(), byteCount,
                                                 Unsafe.MPI_BYTE, dest, tag, comm.comm, out requests.body);
                }

                if (errorCode != Unsafe.MPI_SUCCESS)
                {
                    Unsafe.MPI_Cancel(ref requests.header);
                    headerHandle.Free();
                    bufferHandle.Free();
                    throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
            else
            {
                requests.body = Unsafe.MPI_REQUEST_NULL;
            }
        }

        public override CompletedStatus Wait()
        {
            if (cachedStatus != null)
                return cachedStatus;

            Unsafe.MPI_Status status;
            unsafe
            {
                if (requests.header == Unsafe.MPI_REQUEST_NULL)
                {
                    // Wait until the request completes
                    int errorCode = Unsafe.MPI_Wait(ref requests.body, out status);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                else if (requests.body == Unsafe.MPI_REQUEST_NULL)
                {
                    // Wait until the request completes
                    int errorCode = Unsafe.MPI_Wait(ref requests.header, out status);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                else
                {
                    // Wait until both requests complete
                    Unsafe.MPI_Status[] statuses = new Unsafe.MPI_Status[2];
                    fixed (MPI_Request* requestsPtr = &requests.body)
                    {
                        int errorCode = Unsafe.MPI_Waitall(2, &requestsPtr[0], statuses);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                        status = statuses[0];
                    }
                }
            }

            Cleanup();

            cachedStatus = new CompletedStatus(status, this.count);
            return cachedStatus;
        }

        public override CompletedStatus Test()
        {
            if (cachedStatus != null)
                return cachedStatus;

            Unsafe.MPI_Status status;
            unsafe
            {
                int flag;
                if (requests.header == Unsafe.MPI_REQUEST_NULL)
                {
                    // Test whether the request has completed
                    int errorCode = Unsafe.MPI_Test(ref requests.body, out flag, out status);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                else if (requests.body == Unsafe.MPI_REQUEST_NULL)
                {
                    // Test whether the request has completed
                    int errorCode = Unsafe.MPI_Test(ref requests.header, out flag, out status);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                else
                {
                    fixed (MPI_Request* requestPtr = &requests.body)
                    {
                        // Test whether both requests completed
                        Unsafe.MPI_Status[] statuses = new Unsafe.MPI_Status[2];
                        {
                            int errorCode = Unsafe.MPI_Testall(2, &requestPtr[0], out flag, statuses);
                            if (errorCode != Unsafe.MPI_SUCCESS)
                                throw Environment.TranslateErrorIntoException(errorCode);
                            status = statuses[0];
                        }
                    }
                }

                if (flag == 0)
                {
                    // The communications have not yet completed. We're done here
                    return null;
                }
            }

            Cleanup();

            cachedStatus = new CompletedStatus(status, this.count);
            return cachedStatus;
        }

        public override void Cancel()
        {
            if (cachedStatus != null)
                return;

            int errorCode1 = Unsafe.MPI_SUCCESS;
            int errorCode2 = Unsafe.MPI_SUCCESS;
            Unsafe.MPI_Status status = new Unsafe.MPI_Status();
            int flag = 0;
            unsafe
            {
                // Cancel both MPI requests
                if (requests.body != Unsafe.MPI_REQUEST_NULL)
                {
                    errorCode1 = Unsafe.MPI_Cancel(ref requests.body);

                    if (errorCode1 == Unsafe.MPI_SUCCESS)
                    {
                        errorCode1 = Unsafe.MPI_Test(ref requests.body, out flag, out status);
                    }
                }
                if (requests.header != Unsafe.MPI_REQUEST_NULL)
                {
                    errorCode2 = Unsafe.MPI_Cancel(ref requests.header);

                    if (errorCode2 == Unsafe.MPI_SUCCESS)
                    {
                        int myFlag = 0;
                        errorCode2 = Unsafe.MPI_Test(ref requests.body, out myFlag, out status);
                        if (myFlag != 0 && flag == 0)
                            flag = myFlag;
                    }
                }
            }

            Cleanup();

            if (errorCode1 != Unsafe.MPI_SUCCESS)
                throw Environment.TranslateErrorIntoException(errorCode1);
            if (errorCode2 != Unsafe.MPI_SUCCESS)
                throw Environment.TranslateErrorIntoException(errorCode2);
            if (flag != 0)
                cachedStatus = new CompletedStatus(status, 0);
        }

        /// <summary>
        /// Clean up whatever resources we are holding.
        /// </summary>
        protected void Cleanup()
        {
            if (requests.body != Unsafe.MPI_REQUEST_NULL)
            {
                throw new Exception("Called Cleanup when requests.body is not null");
            }
            if (requests.header != Unsafe.MPI_REQUEST_NULL)
            {
                throw new Exception("Called Cleanup when requests.header is not null");
            }

            headerHandle.Free();
            if (bufferHandle != default(GCHandle))
            {
                bufferHandle.Free();
            }

            // Suppress finalization; there will be nothing to do.
            GC.SuppressFinalize(this);
        }

        ~SerializedSendRequest()
        {
            if (requests.body != Unsafe.MPI_REQUEST_NULL
                || requests.header != Unsafe.MPI_REQUEST_NULL)
            {
                // We are in trouble. The user no longer has any references to 
                // this object, but the communication has not completed. If the
                // stream gets finalized, it will de-allocate memory and probably
                // fail. 
                System.Console.Error.WriteLine("ERROR: Invalid use of MPI.Request object; your application may crash.");
                System.Console.Error.WriteLine("To fix this problem, you must complete non-blocking communications explicitly.");
            }
        }

        /// <summary>
        /// If we've already completed this operation, this caches the status information
        /// so we can return it again later.
        /// </summary>
        protected CompletedStatus cachedStatus;

        /// <summary>
        /// The number of elements transmitted by this operation. 
        /// </summary>
        protected int count;

        /// <summary> 
        /// Header sent in the first message corresponding to the serialized send. 
        /// </summary> 
        protected object headerObj; 
 
        /// <summary> 
        /// Handle that pins down the header object, so that it does not move. 
        /// </summary> 
        protected GCHandle headerHandle;

        /// <summary>
        /// Handle that pins down the buffer object, so that it does not move. 
        /// </summary>
        protected GCHandle bufferHandle;

        /// <summary>
        /// The two outstanding MPI requests. The first request contains the send request
        /// for the buffer, while the second request contains the send for the length
        /// (which may be <see cref="Unsafe.MPI_REQUEST_NULL"/>).
        /// </summary>
        protected TwoRequests requests;
    }

    /// <summary>
    /// A non-blocking receive request for a single value of value type. This
    /// request contains only a single <c>MPI_Request</c> object, which will
    /// receive into its own <c>value</c> member.
    /// </summary>
    class ValueReceiveRequest<T> : ReceiveRequest
    {
        internal ValueReceiveRequest(MPI_Comm comm, int source, int tag, Action<T> action = null)
        {
            this.action = action;
            this.cachedStatus = null;
            this.value = default(T);
            handle = GCHandle.Alloc(value, GCHandleType.Pinned);
            unsafe
            {
                // Initiate the non-blocking receive into "value"
                int errorCode = Unsafe.MPI_Irecv(handle.AddrOfPinnedObject(), 1, FastDatatypeCache<T>.datatype, source, tag, comm, out request);
                if (errorCode != Unsafe.MPI_SUCCESS)
                {
                    handle.Free();
                    throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        public override object GetValue()
        {
            Wait();
            return value;
        }

        public override CompletedStatus Wait()
        {
            if (cachedStatus != null)
                return cachedStatus;

            Unsafe.MPI_Status status;
            unsafe
            {
                // On normal completion, this will set request to null.
                int errorCode = Unsafe.MPI_Wait(ref request, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            action?.Invoke((T)value);
            Cleanup();
            cachedStatus = new CompletedStatus(status, 1);
            return cachedStatus;
        }

        public override CompletedStatus Test()
        {
            if (cachedStatus != null)
                return cachedStatus;

            int flag;
            Unsafe.MPI_Status status;
            unsafe
            {
                // If completed, this will set request to null.
                int errorCode = Unsafe.MPI_Test(ref request, out flag, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            if (flag == 0)
                return null;

            action?.Invoke((T)value);
            Cleanup();
            cachedStatus = new CompletedStatus(status, 1);
            return cachedStatus;
        }

        public override void Cancel()
        {
            if (cachedStatus != null)
                return;

            unsafe
            {
                Unsafe.MPI_Status status;

                int errorCode = Unsafe.MPI_Cancel(ref request);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

                errorCode = Unsafe.MPI_Wait(ref request, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

                cachedStatus = new CompletedStatus(status, 0);
            }

            Cleanup();
        }

        /// <summary>
        /// Cleanup any resources we're still holding on to.
        /// </summary>
        protected void Cleanup()
        {
            if (request != Unsafe.MPI_REQUEST_NULL)
            {
                throw new Exception("Called Cleanup when request is not null");
            }
            handle.Free();
            action = null;

            GC.SuppressFinalize(this);
        }

        ~ValueReceiveRequest()
        {
            if (request != Unsafe.MPI_REQUEST_NULL)
            {
                // We are in trouble. The user no longer has any references to 
                // this object, but the communication has not completed. Our
                // handle has the memory associated with the communication pinned.
                // If we unpin the memory, the communication could fail.
                // If we leave the memory pinned, it will never be unpinned and
                // will probably leak.
                //
                // We choose to write a nasty error message to the console and
                // permit the leak.
                System.Console.Error.WriteLine("ERROR: Invalid use of MPI.Request object; your application may leak memory.");
                System.Console.Error.WriteLine("To fix this problem, you must complete non-blocking communications explicitly.");
            }
        }

        /// <summary>
        /// If we've already completed this operation, this caches the status information
        /// so we can return it again later.
        /// </summary>
        protected CompletedStatus cachedStatus;

        /// <summary>
        /// Handle to <c>value</c>, which will be pinned so that <c>value</c>'s
        /// address will remain the same.
        /// </summary>
        protected GCHandle handle;

        /// <summary>
        /// The MPI request associated with the non-blocking receive.
        /// </summary>
        protected MPI_Request request;

        /// <summary>
        /// The actual value we'll be receiving
        /// </summary>
        /// <remarks>
        /// Must be type "object" so that it is boxed.
        /// </remarks>
        protected object value;

        protected Action<T> action;
    }

    /// <summary>
    /// A non-blocking receive request for an array of values of value type. This
    /// request contains only a single <c>MPI_Request</c> object, which will
    /// receive into the given array.
    /// </summary>
    class ValueArrayReceiveRequest<T> : ReceiveRequest
    {
        internal ValueArrayReceiveRequest(MPI_Comm comm, int source, int tag, T[] array)
        {
            this.cachedStatus = null;
            this.array = array;
            handle = GCHandle.Alloc(array, GCHandleType.Pinned);
            // Initiate the non-blocking receive into "value"
            int errorCode = Unsafe.MPI_Irecv(handle.AddrOfPinnedObject(), array.Length,
                                             FastDatatypeCache<T>.datatype,
                                             source, tag, comm, out request);
            if (errorCode != Unsafe.MPI_SUCCESS)
            {
                handle.Free();
                throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        public override object GetValue()
        {
            Wait();
            return array;
        }

        public override CompletedStatus Wait()
        {
            if (cachedStatus != null)
                return cachedStatus;

            Unsafe.MPI_Status status;
            int count;
            int errorCode;
            unsafe
            {
                errorCode = Unsafe.MPI_Wait(ref request, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                errorCode = Unsafe.MPI_Get_count(ref status, FastDatatypeCache<T>.datatype, out count);
            }

            Cleanup();

            if (errorCode != Unsafe.MPI_SUCCESS)
                throw Environment.TranslateErrorIntoException(errorCode);

            cachedStatus = new CompletedStatus(status, count);
            return cachedStatus;
        }

        public override CompletedStatus Test()
        {
            if (cachedStatus != null)
                return cachedStatus;

            int flag;
            Unsafe.MPI_Status status;
            int count = 0;
            int errorCode;
            unsafe
            {
                errorCode = Unsafe.MPI_Test(ref request, out flag, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                if (flag != 0)
                    errorCode = Unsafe.MPI_Get_count(ref status, FastDatatypeCache<T>.datatype, out count);
            }

            if (flag == 0)
                return null;
            else
            {
                Cleanup();

                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

                cachedStatus = new CompletedStatus(status, count);
                return cachedStatus;
            }
        }

        public override void Cancel()
        {
            if (cachedStatus != null)
                return;

            unsafe
            {
                Unsafe.MPI_Status status;

                int errorCode = Unsafe.MPI_Cancel(ref request);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

                errorCode = Unsafe.MPI_Wait(ref request, out status);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

                cachedStatus = new CompletedStatus(status, 0);
            }

            Cleanup();
        }

        /// <summary>
        /// Cleanup any resources we're still holding on to.
        /// </summary>
        protected void Cleanup()
        {
            if (request != Unsafe.MPI_REQUEST_NULL)
            {
                throw new Exception("Called Cleanup when request is not null");
            }
            handle.Free();

            GC.SuppressFinalize(this);
        }

        ~ValueArrayReceiveRequest()
        {
            if (request != Unsafe.MPI_REQUEST_NULL)
            {
                // We are in trouble. The user no longer has any references to 
                // this object, but the communication has not completed. Our
                // handle has the memory associated with the communication pinned.
                // If we unpin the memory, the communication could fail.
                // If we leave the memory pinned, it will never be unpinned and
                // will probably leak.
                //
                // We choose to write a nasty error message to the console and
                // permit the leak.
                System.Console.Error.WriteLine("ERROR: Invalid use of MPI.Request object; your application may leak memory.");
                System.Console.Error.WriteLine("To fix this problem, you must complete non-blocking communications explicitly.");
            }
        }

        /// <summary>
        /// If we've already completed this operation, this caches the status information
        /// so we can return it again later.
        /// </summary>
        protected CompletedStatus cachedStatus;

        /// <summary>
        /// Handle to <c>this</c>, which will be pinned so that <c>value</c>'s
        /// address will remain the same.
        /// </summary>
        protected GCHandle handle;

        /// <summary>
        /// The MPI request associated with the non-blocking receive.
        /// </summary>
        protected MPI_Request request;

        /// <summary>
        /// The actual array we'll be receiving into.
        /// </summary>
        protected T[] array;
    }

    /// <summary>
    /// A request for a serialized receive, which uses a two-stage receive process. The first stage
    /// receives the size of the serialized data. The second stage receives the actual data and, 
    /// upon completion, deserializes that data.
    /// </summary>
    /// <typeparam name="T">Any serializable type.</typeparam>
    class SerializedReceiveRequest<T> : ReceiveRequest
    {
        internal SerializedReceiveRequest(Communicator comm, int source, int tag, Action<T> action = null)
        {
            this.comm = comm;
            this.action = action;
            stream = null;
            value = default(T);
            cachedStatus = null;
            headerObj = default(int);

            // Pin ourselves and post a request to receive the header.
            handle = GCHandle.Alloc(headerObj, GCHandleType.Pinned);
            unsafe
            {
                int errorCode = Unsafe.MPI_Irecv(handle.AddrOfPinnedObject(), 1, FastDatatypeCache<int>.datatype,
                                                 source, tag, comm.comm, out request);
                if (errorCode != Unsafe.MPI_SUCCESS)
                {
                    handle.Free();
                    throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        public override object GetValue()
        {
            Wait();
            return value;
        }

        public override CompletedStatus Wait()
        {
            return TestOrWait(true);
        }

        public override CompletedStatus Test()
        {
            return TestOrWait(false);
        }

        protected CompletedStatus TestOrWait(bool wait)
        {
            if (cachedStatus != null)
                return cachedStatus;

            Unsafe.MPI_Status status;
            int flag;

            unsafe
            {
                int errorCode;
                if (wait)
                {
                    errorCode = Unsafe.MPI_Wait(ref request, out status);
                    flag = 1;
                }
                else
                {
                    errorCode = Unsafe.MPI_Test(ref request, out flag, out status);
                }
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            if (flag == 0)
                return null;

            if (stream == null)
            {
                // We completed the receive of the header message.

                // Cleanup this receive
                handle.Free();

                int length = (int)headerObj;
                if (length == 0)
                {
                    // If the second message is empty, we're done
                    action?.Invoke(value);
                    Cleanup();
                    cachedStatus = new CompletedStatus(status, 1);
                    return cachedStatus;
                }

                // Setup the serialized receive and test again
                SetupSerializedReceive(status, length);
                return TestOrWait(wait);
            }

            // We completed the receive of the serialized data.
            status.MPI_TAG = originalTag;
            cachedStatus = new CompletedStatus(status, 1);

            // Deserialize the data
            try
            {
                value = comm.Serialization.Serializer.Deserialize<T>(stream);
            }
            finally
            {
                stream.Dispose();
                stream = null;
            }
            action?.Invoke(value);

            Cleanup();
            return cachedStatus;
        }

        public override void Cancel()
        {
            if (cachedStatus != null)
                return;

            if (request != Unsafe.MPI_REQUEST_NULL)
            {
                unsafe
                {
                    int errorCode = Unsafe.MPI_Cancel(ref request);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);

                    Unsafe.MPI_Status status;
                    errorCode = Unsafe.MPI_Wait(ref request, out status);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);

                    cachedStatus = new CompletedStatus(status, 0);
                }
                if (stream == null)
                    handle.Free();
                else
                    stream.Dispose();
                Cleanup();
            }
        }

        /// <summary>
        /// Cleanup any resources we're still holding on to.
        /// </summary>
        protected void Cleanup()
        {
            if (request != Unsafe.MPI_REQUEST_NULL)
            {
                throw new Exception("Called Cleanup when request is not null");
            }
            action = null;

            GC.SuppressFinalize(this);
        }

        ~SerializedReceiveRequest()
        {
            if (request != Unsafe.MPI_REQUEST_NULL)
            {
                // We are in trouble. The user no longer has any references to 
                // this object, nor the stream it stores, so the stream may
                // deallocate memory that the MPI library still has a reference 
                // to.
                System.Console.Error.WriteLine("ERROR: Invalid use of MPI.Request object; your application may crash.");
                System.Console.Error.WriteLine("To fix this problem, you must complete non-blocking communications explicitly.");
            }
        }

        /// <summary>
        /// Given the header data, this routine will set up the receive of the
        /// serialized data.
        /// </summary>
        /// <param name="status">
        ///   The status message returned from the completion of the first
        ///   receive (of the header data).
        /// </param>
        /// <param name="length">Number of bytes to receive.</param>
        protected void SetupSerializedReceive(Unsafe.MPI_Status status, int length)
        {
            // Save the tag
            originalTag = status.MPI_TAG;

            // Create the stream
            stream = new UnmanagedMemoryStream(length);

            unsafe
            {
                // Receive serialized data
                int errorCode = Unsafe.MPI_Irecv(stream.Buffer, length, Unsafe.MPI_BYTE,
                                                 status.MPI_SOURCE, originalTag, comm.comm, out request);
                if (errorCode != Unsafe.MPI_SUCCESS)
                {
                    stream.Dispose();
                    throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        /// If we've already completed this operation, this caches the status information
        /// so we can return it again later.
        /// </summary>
        protected CompletedStatus cachedStatus;

        /// <summary>
        /// The tag value used by the original received message. We store this value when we
        /// receive the header rather that storing the complete status object, since the
        /// status object from the second receive is just as good.
        /// </summary>
        protected int originalTag;

        /// <summary>
        /// Handle that pins down either the header object when receiving the header.
        /// </summary>
        protected GCHandle handle;

        /// <summary>
        /// Request object that corresponds to the currently-active request.
        /// </summary>
        protected MPI_Request request;

        protected Communicator comm;

        protected Action<T> action;

        /// <summary>
        /// Stream that will receive the serialized data.
        /// </summary>
        protected UnmanagedMemoryStream stream;

        /// <summary>
        /// Message header to be received in the first stage.
        /// </summary>
        protected object headerObj;

        /// <summary>
        /// The value we are receiving.
        /// </summary>
        protected T value;
    };

    /// <summary>
    /// A request for a serialized receive of an array of values. The communication behavior is 
    /// identical to (and implemented as) <see cref="SerializedReceiveRequest&lt;T&gt;"/>, but
    /// here we need to copy the values from the de-serialized array into the array provided 
    /// by the user.
    /// </summary>
    /// <typeparam name="T">Any serializable type.</typeparam>
    class SerializedArrayReceiveRequest<T> : ReceiveRequest
    {
        internal SerializedArrayReceiveRequest(Communicator comm, int source, int tag, T[] array)
        {
            request = new SerializedReceiveRequest<T[]>(comm, source, tag);
            this.array = array;
            this.cachedStatus = null;
        }

        public override object GetValue()
        {
            Wait();
            return array;
        }

        public override CompletedStatus Wait()
        {
            return CopyResults(request.Wait());
        }

        public override CompletedStatus Test()
        {
            return CopyResults(request.Test());
        }

        public override void Cancel()
        {
            request.Cancel();
        }

        /// <summary>
        /// Copies the results from the received array into the user-provided array
        /// and returns a new status object with the appropriate count.
        /// </summary>
        protected CompletedStatus CopyResults(CompletedStatus status)
        {
            if (status == null)
                return status;

            if (cachedStatus != null)
                return cachedStatus;

            T[] receivedArray = (T[])request.GetValue();
            if (receivedArray.Length > array.Length)
                throw new AccessViolationException("Non-blocking array received overran receive buffer");
            receivedArray.CopyTo(array, 0);

            cachedStatus = new CompletedStatus(status.status, receivedArray.Length);
            return cachedStatus;
        }

        /// <summary>
        /// The request that handles receipt of the actual array, since we cannot
        /// de-serialize an array into an existing array.
        /// </summary>
        SerializedReceiveRequest<T[]> request;

        /// <summary>
        /// The user-provided array, where values will be copied once the receive
        /// has completed.  Its length must be at least the length of the received array.
        /// </summary>
        protected T[] array;

        /// <summary>
        /// Cached copy of the status object returned from this request. When non-null,
        /// this communication has already completed, so we just return this object.
        /// </summary>
        protected CompletedStatus cachedStatus;
    }
}
