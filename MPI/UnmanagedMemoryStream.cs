/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This file provides a memory stream that uses unmanaged memory either from
 * MPI or from the unmanaged heap.
 */
using System;
using System.IO;
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
    /// A stream object that is backed by unmanaged memory. The behavior of <c>UnmanagedMemoryStream</c> is similar to 
    /// that of the .NET <see cref="System.IO.MemoryStream"/>, because it manages memory internally and will re-allocate 
    /// its internal buffers as necessary to accommodate additional writes (the latter is not supported by 
    /// <see cref="System.IO.UnmanagedMemoryStream"/>). The memory used by this class is allocated either via MPI's
    /// memory allocation routines (the default) or the <see cref="System.Runtime.InteropServices.Marshal"/> class's 
    /// unmanaged memory allocation routines.
    /// </summary>
    internal class UnmanagedMemoryStream : Stream
    {
        /// <summary>
        /// Create a new, empty unmanaged memory stream. The memory stream can be resized by 
        /// setting <see cref="Length"/>, calling <see cref="SetLength"/>, or writing to
        /// the stream.
        /// </summary>
        public UnmanagedMemoryStream()
        {
            unsafe
            {
                storage = new IntPtr(null);
            }
            capacity = 0;
            count = 0;
            position = 0;
        }

        /// <summary>
        /// Create a new, empty unmanaged memory stream. The memory stream can be resized by 
        /// setting <see cref="Length"/>, calling <see cref="SetLength"/>, or writing to
        /// the stream.
        /// </summary>
        /// <param name="defaultMemoryFromMPI">
        ///   Whether this stream should first try to allocate its memory from MPI. If
        ///   MPI cannot provide memory, or if this parameter is <c>false</c>, memory
        ///   will be allocated from the unmanaged heap.
        /// </param>
        public UnmanagedMemoryStream(bool defaultMemoryFromMPI)
        {
            DefaultMemoryFromMPI = defaultMemoryFromMPI;
            unsafe
            {
                storage = new IntPtr(null);
            }
            capacity = 0;
            count = 0;
            position = 0;
        }

        /// <summary>
        /// Create a new unmanaged memory stream with storage for a certain number of bytes of
        /// data. The length of the stream will be set to <paramref name="bytes"/>, but the 
        /// contents of the stream after initialization are undefined.
        /// </summary>
        /// <param name="bytes">The number of bytes in the new stream.</param>
        public UnmanagedMemoryStream(long bytes)
        {
            storageFromMPI = DefaultMemoryFromMPI;
            storage = AllocateMemory(bytes, ref storageFromMPI);
            capacity = bytes;
            count = bytes;
            position = 0;
        }

        /// <summary>
        /// Create a new unmanaged memory stream with storage for a certain number of bytes of
        /// data. The length of the stream will be set to <paramref name="bytes"/>, but the 
        /// contents of the stream after initialization are undefined.
        /// </summary>
        /// <param name="bytes">The number of bytes in the new stream.</param>
        /// <param name="defaultMemoryFromMPI">
        ///   Whether this stream should first try to allocate its memory from MPI. If
        ///   MPI cannot provide memory, or if this parameter is <c>false</c>, memory
        ///   will be allocated from the unmanaged heap.
        /// </param>
        public UnmanagedMemoryStream(long bytes, bool defaultMemoryFromMPI)
        {
            DefaultMemoryFromMPI = defaultMemoryFromMPI;
            storageFromMPI = DefaultMemoryFromMPI;
            storage = AllocateMemory(bytes, ref storageFromMPI);
            capacity = bytes;
            count = bytes;
            position = 0;
        }

        /// <summary>
        /// Whether the stream can be read from. Always <c>true</c>.
        /// </summary>
        public override bool CanRead
        {
            get { return true; }
        }

        /// <summary>
        /// Whether one can seek in the stream. Always <c>true</c>.
        /// </summary>
        public override bool CanSeek
        {
            get { return true; }
        }

        /// <summary>
        /// Whether on can write to the stream. Always <c>true</c>.
        /// </summary>
        public override bool CanWrite
        {
            get { return true; }
        }

        /// <summary>
        /// "Flush" the contents of the stream. This operation does nothing.
        /// </summary>
        public override void Flush()
        {
            // Nothing to do.
        }

        /// <summary>
        /// The length of the stream, in bytes.
        /// </summary>
        public override long Length
        {
            get { return count; }
        }

        /// <summary>
        /// Reports or sets the position in the stream. 
        /// </summary>
        public override long Position
        {
            get
            {
                return position;
            }
            set
            {
                if (value < 0 || value > count)
                    throw new ArgumentOutOfRangeException("Position value is beyond the end of the stream");
                position = value;
            }
        }

        /// <summary>
        /// Read <paramref name="count"/> bytes from the current position in the stream, and place
        /// them into the <paramref name="buffer"/> starting at the given <paramref name="offset"/>.
        /// The position will be updated to the point after the last byte read.
        /// </summary>
        /// <param name="buffer">Array that will receive the data read from the stream.</param>
        /// <param name="offset">The position in the buffer where the first byte will be written.</param>
        /// <param name="count">The maximum number of bytes to copy into the buffer.</param>
        /// <returns>The actual number of bytes read into the buffer.</returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            // Make sure we don't read past the end of the buffer
            int maxCount = Convert.ToInt32(checked(this.count - position));
            if (count > maxCount)
                count = maxCount;

            // Read from the buffer
            unsafe
            {
                Marshal.Copy(new IntPtr((byte*)storage + position), buffer, offset, count);
            }

            // Update the position
            position = checked(position + count);

            return count;
        }

        /// <summary>
        /// Seek to a specific position in the stream.
        /// </summary>
        /// <param name="offset">Offset (in bytes) from the <paramref name="origin"/>.</param>
        /// <param name="origin">Where to start seeking from.</param>
        /// <returns>The new position in the stream.</returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            long absoluteOffset = 0;
            switch (origin)
            {
                case SeekOrigin.Begin:
                    absoluteOffset = offset;
                    break;

                case SeekOrigin.Current:
                    absoluteOffset = checked(position + offset);
                    break;

                case SeekOrigin.End:
                    absoluteOffset = checked(count - offset);
                    break;
            }

            if (absoluteOffset < 0 || absoluteOffset > count)
                throw new ArgumentOutOfRangeException("offset", "Seek index is out of range for the stream");

            position = absoluteOffset;
            return position;
        }

        /// <summary>
        /// Set the length of the stream. If the new length of the stream 
        /// is larger than the old length, the contents of the stream from
        /// the old length to the new length are undefined.
        /// </summary>
        /// <param name="value">The new length.</param>
        public override void SetLength(long value)
        {
            Reserve(value);
            count = value;
            if (position > count)
                position = count;
        }

        /// <summary>
        /// Write data into unmanaged memory. If the write would continue past the
        /// end of the memory stream, the memory stream is expanded.
        /// </summary>
        /// <param name="buffer">The buffer containing the data to write.</param>
        /// <param name="offset">The position in the buffer from which data should be read.</param>
        /// <param name="count">The number of bytes to write to the stream.</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            long newPosition = checked(position + count);
            if (newPosition > capacity)
            {
                // Make sure we have enough space in the buffer
                Reserve(Math.Max(newPosition, checked(capacity * 2)));
            }

            // Write the new data into the stream
            unsafe
            {
                Marshal.Copy(buffer, offset, new IntPtr((byte*)storage + position), count);
            }

            // Update our position
            position = newPosition;
            if (this.count < position)
                this.count = position;
        }

        /// <summary>
        /// Reserve a certain amount of space in the buffer, to minimize the need for
        /// reallocations if you already know how much space will be needed.
        /// </summary>
        /// <param name="value">The number of bytes to reserve.</param>
        public void Reserve(long value)
        {
            if (value != capacity)
            {
                // Allocate new buffer
                bool newBufferFromMPI = DefaultMemoryFromMPI;
                IntPtr newBuffer = AllocateMemory(value, ref newBufferFromMPI);

                // Copy from the old buffer to the new buffer
                if (count > value)
                    count = value;
                unsafe
                {
                    byte* source = (byte*)storage;
                    byte* dest = (byte*)newBuffer;
                    for (long i = 0; i < count; ++i)
                        dest[i] = source[i];
                }

                // Free the old buffer
                FreeMemory(storage, storageFromMPI);

                // save the new buffer
                capacity = value;
                storage = newBuffer;
                storageFromMPI = newBufferFromMPI;

                if (position > count)
                    position = count;
            }
        }

        /// <summary>
        /// Deallocate any memory allocated by this stream and
        /// dispose of the object.
        /// </summary>
        /// <param name="disposing">
        ///   Whether this call was the result of calling dispose (<c>true</c>)
        ///   or occurred in response to a finalizer (<c>false</c>).
        /// </param>
        protected override void Dispose(bool disposing)
        {
            unsafe
            {
                if (new IntPtr(null) != storage)
                {
                    FreeMemory(storage, storageFromMPI);
                    storage = new IntPtr(null);
                }
            }

            // Dispose of the base stream
            base.Dispose(disposing);

            // No need to finalize this object: we've already deallocated memory
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the memory associated with the stream, if any.
        /// </summary>
        ~UnmanagedMemoryStream()
        {
            Dispose(false);
        }

        /// <summary>
        /// Allocates unmanaged memory. The memory will either return memory allocated from
        /// MPI or from the unmanaged heap, depending on <paramref name="fromMPI"/>. Memory
        /// allocated in this way should be freed via <see cref="FreeMemory"/>.
        /// </summary>
        /// <param name="bytes">The number of bytes to allocate.</param>
        /// <param name="fromMPI">
        ///   If true, this routine will first try to allocate the memory
        ///   via MPI's memory allocation routines (e.g. <see cref="Unsafe.MPI_Alloc_mem"/>),
        ///   then will fall back to C#'s unmanaged memory allocation routines.
        ///   This value will be updated to reflect where the memory actually came from.
        /// </param>
        /// <returns>A pointer to the newly-allocated memory.</returns>
        private static IntPtr AllocateMemory(long bytes, ref bool fromMPI)
        {
            if (fromMPI)
            {
                // Try allocating memory from MPI, directly.
                IntPtr ptr;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Alloc_mem((MPI_Aint)bytes, Unsafe.MPI_INFO_NULL, out ptr);
                    if (errorCode == Unsafe.MPI_SUCCESS)
                        return ptr;
                    else if (errorCode != Unsafe.MPI_ERR_NO_MEM)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }

                // MPI doesn't have any more memory; fall back to the C# facilities for allocating
                // unmanaged memory
                fromMPI = false;
            }

            // Allocate memory from the unmanaged heap
            return Marshal.AllocHGlobal(new IntPtr(bytes));
        }

        /// <summary>
        /// Frees memory allocated via <see cref="AllocateMemory"/>.
        /// </summary>
        /// <param name="ptr">The pointer returned from <see cref="AllocateMemory"/>.</param>
        /// <param name="fromMPI">Whether this memory came from MPI or from the unmaanged heap.</param>
        private static void FreeMemory(IntPtr ptr, bool fromMPI)
        {
            unsafe
            {
                if (ptr == new IntPtr(null))
                    return;
            }

            if (fromMPI)
            {
                if (!Environment.Finalized)
                {
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Free_mem(ptr);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
            }
            else
            {
                Marshal.FreeHGlobal(ptr);
            }
        }

        /// <summary>
        /// Retrieve a pointer to the unmanaged memory buffer. Since this
        /// buffer is pointing into unmanaged memory, it does not need to be
        /// pinned.
        /// </summary>
        public IntPtr Buffer
        {
            get { return storage; }
        }

        /// <summary>
        /// The amount of space in the unmanaged memory buffer. This can be
        /// larger than the length of the stream. If you know how many bytes
        /// will be written to the stream, you might want to set the capacity
        /// (either via this property or through <see cref="Reserve"/>) large
        /// enough to avoid resizing the stream multiple times.
        /// </summary>
        public long Capacity
        {
            get
            {
                return capacity;
            }

            set
            {
                Reserve(value);
            }
        }

        /// <summary>
        /// Whether memory comes from the MPI allocation routines by default.
        /// </summary>
        public bool DefaultMemoryFromMPI = true;

        /// <summary>
        /// A pointer to unmanaged storage.
        /// </summary>
        private IntPtr storage;

        /// <summary>
        /// True when the <see cref="storage"/> is storage obtained from MPI
        /// via <see cref="Unsafe.MPI_Alloc_mem"/>.
        /// </summary>
        private bool storageFromMPI;

        /// <summary>
        /// The number of bytes that <see cref="storage"/> refers to.
        /// </summary>
        private long capacity;

        /// <summary>
        /// The number of bytes in the buffer.
        /// </summary>
        private long count;

        /// <summary>
        /// Current position within the stream.
        /// </summary>
        private long position;
    }
}
