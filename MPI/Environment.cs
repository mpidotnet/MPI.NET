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
    /// Enumeration describing the level of threading support provided by the MPI implementation.
    /// The MPI environment should be initialized with the minimum threading support required
    /// for your application, because additional threading support can have a negative impact
    /// on performance.
    /// 
    /// The four options providing monotonically-increasing levels of freedom for the MPI
    /// program. Thus, a program implemented based on the <c>Threading.Single</c> semantics
    /// will work perfectly well (although perhaps less efficiently) with <c>Threading.Multiple</c>.
    /// </summary>
    public enum Threading
    {
        /// <summary>
        /// The program is single-threaded, and does not ever create additional threads
        /// (even those that don't use MPI).
        /// </summary>
        Single,
        /// <summary>
        /// The program is multi-threaded, but only one of those threads (the main thread)
        /// will make calls into the MPI library. Thus, all of the MPI calls are "funneled"
        /// into that one thread. One can determine if the currently-executing thread is
        /// the main thread via <see cref="MPI.Environment.IsMainThread"/>.
        /// </summary>
        Funneled,
        /// <summary>
        /// The program is multi-threaded, and more than one thread will make calls into 
        /// the MPI library. However, these calls into MPI are "serialized", in that
        /// no two threads will call the MPI library at the same time. This mode essentially
        /// requires the user program to have the equivalent of a global lock around all calls
        /// to MPI.
        /// </summary>
        Serialized,
        /// <summary>
        /// The program is multi-threaded, and more than one thread will make calls into
        /// the MPI library. The program is free to call the MPI library routines from
        /// any thread at any time, including having multiple threads calling MPI
        /// concurrently. Note that this level of threading is not supported by MS MPI.
        /// </summary>
        Multiple
    }

    /// <summary>Provides MPI initialization, finalization, and environmental queries.</summary>
    /// <remarks>
    ///   The <c>Environment</c> class provides initialization, finalization, and
    ///   environmental query operations for the Message Passing Interface.
    ///   Users must initialize the MPI environment before using any of the
    ///   MPI facilities by creating an instanceof the Environment class.
    /// </remarks>
    public class Environment : IDisposable
    {
        /// <summary>
        /// Initialize the MPI environment, execute action with the world communicator, and finalize the MPI environment.
        /// If any exception is thrown by action, all processes will be terminated.
        /// This is preferable to creating an Environment in a "using" block, since that may hang if an exception is thrown.
        /// </summary>
        /// <param name="action">Receives the world communicator and performs MPI actions.</param>
        /// <param name="cleanupEnvironment">If true, the MPi environment will be disposed of on completion 
        /// and any thrown exceptions will result in Abort being called</param>
        public static void Run(Action<Intracommunicator> action, bool cleanupEnvironment = true)
        {
            string[] args = null;
            Run(ref args, action, cleanupEnvironment);
        }

        /// <summary>
        /// Initialize the MPI environment, execute action with the world communicator, and finalize the MPI environment.
        /// If any exception is thrown by action, all processes will be terminated.
        /// This is preferable to creating an Environment in a "using" block, since that may hang if an exception is thrown.
        /// </summary>
        /// <param name="args">
        ///   Arguments passed to the <c>Main</c> function in your program. MPI 
        ///   may use some of these arguments for its initialization, and will remove 
        ///   them from this argument before returning.
        /// </param>
        /// <param name="action">Receives the world communicator and performs MPI actions.</param>
        /// <param name="cleanupEnvironment">If true, the MPi environment will be disposed of on completion 
        /// and any thrown exceptions will result in Abort being called</param>
        public static void Run(ref string[] args, Action<Intracommunicator> action, bool cleanupEnvironment = true)
        {
            Run(ref args, Threading.Serialized, action, cleanupEnvironment);
        }
        
        /// <summary>
        /// Initialize the MPI environment, execute action with the world communicator, and finalize the MPI environment.
        /// If any exception is thrown by action, all processes will be terminated.
        /// This is preferable to creating an Environment in a "using" block, since that may hang if an exception is thrown.
        /// </summary>
        /// <param name="args">
        ///   Arguments passed to the <c>Main</c> function in your program. MPI 
        ///   may use some of these arguments for its initialization, and will remove 
        ///   them from this argument before returning.
        /// </param>
        /// <param name="threading">
        ///   The level of threading support requested of the MPI implementation. The
        ///   implementation will attempt to provide this level of threading support.
        ///   However, the actual level of threading support provided will be published
        ///   via the <see cref="MPI.Environment.Threading"/> property.
        /// </param>
        /// <param name="action">Receives the world communicator and performs MPI actions.</param>
        /// <param name="cleanupEnvironment">If true, the MPi environment will be disposed of on completion 
        /// and any thrown exceptions will result in Abort being called</param>
        public static void Run(ref string[] args, Threading threading, Action<Intracommunicator> action, bool cleanupEnvironment = true)
        {
            var env = new Environment(ref args);

            if(cleanupEnvironment)
            {
                try
                {
                    action(Communicator.world);
                    env.Dispose();
                }
                catch (Exception exception)
                {
                    try
                    {
                        Console.Error.WriteLine(exception);
                    }
                    catch
                    {
                        // exception.ToString() can sometimes throw an exception.
                        Console.Error.WriteLine($"{exception.GetType().Name}: {exception.Message}");
                    }
                    Abort(1);
                }
            }
            else
            {
                action(Communicator.world);
            }
        }

        /// <summary>
        ///   Initializes the MPI environment. This variant of the <see cref="Environment"/> constructor 
        ///   initializes the MPI environment with the <see cref="MPI.Threading.Serialized"/> threading model.
        /// </summary>
        /// <param name="args">
        ///   Arguments passed to the <c>Main</c> function in your program. MPI 
        ///   may use some of these arguments for its initialization, and will remove 
        ///   them from this argument before returning.
        /// </param>
        /// <remarks>
        ///   This routine must be invoked before using any other MPI facilities. 
        ///   Be sure to call <c>Dispose()</c> to finalize the MPI environment before exiting!
        /// </remarks>
        /// <example>This simple program initializes MPI and writes out the rank of each processor:
        /// <code>
        /// using MPI;
        /// 
        /// public class Hello 
        /// {
        ///     static int Main(string[] args)
        ///     {
        ///         using (MPI.Environment env = new MPI.Environment(ref args))
        ///         {
        ///             System.Console.WriteLine("Hello, from process number " 
        ///                 + MPI.Communicator.world.Rank.ToString() + " of "
        ///                 + MPI.Communicator.world.Size.ToString());
        ///         }
        ///     }
        /// }
        /// </code>
        /// </example>
        public Environment(ref string[] args) : this(ref args, Threading.Serialized) { }

            /// <summary>Initializes the MPI environment.</summary>
        /// <param name="args">
        ///   Arguments passed to the <c>Main</c> function in your program. MPI 
        ///   may use some of these arguments for its initialization, and will remove 
        ///   them from this argument before returning.
        /// </param>
        /// <param name="threading">
        ///   The level of threading support requested of the MPI implementation. The
        ///   implementation will attempt to provide this level of threading support.
        ///   However, the actual level of threading support provided will be published
        ///   via the <see cref="MPI.Environment.Threading"/> property.
        /// </param>
        /// <remarks>
        ///   This routine must be invoked before using any other MPI facilities. 
        ///   Be sure to call <c>Dispose()</c> to finalize the MPI environment before exiting!
        /// </remarks>
        /// <example>This simple program initializes MPI and writes out the rank of each processor:
        /// <code>
        /// using MPI;
        /// 
        /// public class Hello 
        /// {
        ///     static int Main(string[] args)
        ///     {
        ///         using (MPI.Environment env = new MPI.Environment(ref args))
        ///         {
        ///             System.Console.WriteLine("Hello, from process number " 
        ///                 + MPI.Communicator.world.Rank.ToString() + " of "
        ///                 + MPI.Communicator.world.Size.ToString());
        ///         }
        ///     }
        /// }
        /// </code>
        /// </example>
        public Environment(ref string[] args, Threading threading)
        {
            if (Finalized)
            {
                throw new ObjectDisposedException("Constructor called when object already finalized.");
            }

            if (!Initialized)
            {
                int requiredThreadLevel = 0;
                int providedThreadLevel;

                switch (threading)
                {
                    case Threading.Single:
                        requiredThreadLevel = Unsafe.MPI_THREAD_SINGLE;
                        break;
                    case Threading.Funneled:
                        requiredThreadLevel = Unsafe.MPI_THREAD_FUNNELED;
                        break;
                    case Threading.Serialized:
                        requiredThreadLevel = Unsafe.MPI_THREAD_SERIALIZED;
                        break;
                    case Threading.Multiple:
                        requiredThreadLevel = Unsafe.MPI_THREAD_MULTIPLE;
                        break;
                }

                if (args == null)
                {
                    unsafe
                    {
                        int argc = 0;
                        byte** argv = null;
                        Unsafe.MPI_Init_thread(ref argc, ref argv, requiredThreadLevel, out providedThreadLevel);
                    }
                }
                else
                {
                    ASCIIEncoding ascii = new ASCIIEncoding();
                    unsafe
                    {
                        // Copy args into C-style argc/argv
                        int my_argc = args.Length;
                        byte** my_argv = stackalloc byte*[my_argc];
                        for (int argidx = 0; argidx < my_argc; ++argidx)
                        {
                            // Copy argument into a byte array (C-style characters)
                            char[] arg = args[argidx].ToCharArray();
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

                        // Initialize MPI
                        int mpi_argc = my_argc;
                        byte** mpi_argv = my_argv;
                        Unsafe.MPI_Init_thread(ref mpi_argc, ref mpi_argv, requiredThreadLevel, out providedThreadLevel);

                        // \todo Copy c-style argc/argv back into args
                        if (mpi_argc != my_argc || mpi_argv != my_argv)
                        {
                            args = new string[mpi_argc];
                            for (int argidx = 0; argidx < args.Length; ++argidx)
                            {
                                // Find the end of the string
                                int byteCount = 0;
                                while (mpi_argv[argidx][byteCount] != 0)
                                    ++byteCount;

                                // Determine how many Unicode characters we need
                                int charCount = ascii.GetCharCount(mpi_argv[argidx], byteCount);

                                // Convert ASCII characters into unicode characters
                                char[] chars = new char[charCount];
                                fixed (char* argp = chars)
                                {
                                    ascii.GetChars(mpi_argv[argidx], byteCount, argp, charCount);
                                }

                                // Create the resulting string
                                args[argidx] = new string(chars);
                            }
                        }
                    }
                }

                switch (providedThreadLevel)
                {
                    case Unsafe.MPI_THREAD_SINGLE:
                        Environment.providedThreadLevel = Threading.Single;
                        break;
                    case Unsafe.MPI_THREAD_FUNNELED:
                        Environment.providedThreadLevel = Threading.Funneled;
                        break;
                    case Unsafe.MPI_THREAD_SERIALIZED:
                        Environment.providedThreadLevel = Threading.Serialized;
                        break;
                    case Unsafe.MPI_THREAD_MULTIPLE:
                        Environment.providedThreadLevel = Threading.Multiple;
                        break;
                    default:
                        throw new ApplicationException("MPI.NET: Underlying MPI library returned incorrect value for thread level");
                }

                // Setup communicators
                Communicator.world = Intracommunicator.Adopt(Unsafe.MPI_COMM_WORLD);
                Communicator.self = Intracommunicator.Adopt(Unsafe.MPI_COMM_SELF);
            }
        }

        /// <summary>
        ///  Finalizes the MPI environment. Users must call this routine to shut down MPI.
        /// </summary>
        public void Dispose() 
        {
            if (!Finalized)
            {
                Unsafe.MPI_Finalize();
            }
        }

        /// <summary>
        /// Determine whether the MPI environment has been initialized.
        /// </summary>
        public static bool Initialized
        {
            get
            {
                int flag;
                int errorCode = Unsafe.MPI_Initialized(out flag);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                return flag != 0;
            }
        }

        /// <summary>
        /// Determine whether the MPI environment has been finalized.
        /// </summary>
        public static bool Finalized
        {
            get
            {
                int flag;
                int errorCode = Unsafe.MPI_Finalized(out flag);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                return flag != 0;
            }
        }

        /// <summary>
        /// The level of threading support provided by the MPI library.
        /// </summary>
        /// 
        /// <remarks>
        /// This value describes whether and how the MPI library can be
        /// used in multi-threaded programs. The threading level is
        /// requested when the MPI library is initialized in the
        /// <see cref="Environment(ref string[], MPI.Threading)"/>
        /// constructor.
        /// </remarks>
        public static MPI.Threading Threading
        {
            get
            {
                return providedThreadLevel;
            }
        }

        /// <summary>
        /// Determines whether the calling thread is the main MPI thread. Will return
        /// true for the thread that called the <see cref="MPI.Environment"/> constructor
        /// to initialize MPI. The main thread is particularly important when the threading
        /// mode is <see cref="MPI.Threading.Funneled"/>, because in that model only the
        /// main thread can invoke MPI routines.
        /// </summary>
        public static bool IsMainThread
        {
            get
            {
                int flag;
                int errorCode = Unsafe.MPI_Is_thread_main(out flag);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                return flag != 0;
            }
        }

        /// <summary>
        /// Returns the name of the currently executing processor.
        /// This name does not have any specific form, but typically
        /// identifies the computere on which the process is executing.
        /// </summary>
        public static string ProcessorName
        {
            get
            {
                unsafe
                {
                    byte[] name = new byte[Unsafe.MPI_MAX_PROCESSOR_NAME];
                    int len = Unsafe.MPI_MAX_PROCESSOR_NAME;
                    int errorCode = Unsafe.MPI_Get_processor_name(name, ref len);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                    fixed (byte* namePtr = name)
                        return Marshal.PtrToStringAnsi(new IntPtr(namePtr), len);
                }
            }
        }

        /// <summary>
        /// Returns the time, in seconds, since some arbitrary time in the past.
        /// This value is typically used for timing parallel applications.
        /// </summary>
        public static double Time
        {
            get
            {
                unsafe 
                {
                    return Unsafe.MPI_Wtime();
                }
            }
        }

        /// <summary>
        /// Returns the resolution of <see cref="Time"/>, in seconds.
        /// </summary>
        public static double TimeResolution
        {
            get
            {
                unsafe
                {
                    return Unsafe.MPI_Wtick();
                }
            }
        }

        /// <summary>
        /// Determines whether the <see cref="Time"/> value is synchronized
        /// across all MPI processes (i.e., if <see cref="Time"/> is a global 
        /// value).
        /// </summary>
        public static bool IsTimeGlobal
        {
            get
            {
                int is_global;
                int flag;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Attr_get(Unsafe.MPI_COMM_WORLD, Unsafe.MPI_WTIME_IS_GLOBAL, new IntPtr(&is_global), out flag);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw TranslateErrorIntoException(errorCode);
                }
                return is_global != 0;
            }
        }

        /// <summary>
        /// Returns the maximum allowed tag value for use with MPI's 
        /// point-to-point operations.
        /// </summary>
        public static int MaxTag
        {
            get
            {
                int flag;
                int result = 32767;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Attr_get(Unsafe.MPI_COMM_WORLD, Unsafe.MPI_TAG_UB, new IntPtr(&result), out flag);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw TranslateErrorIntoException(errorCode);
                }
                return result;
            }
        }

        /// <summary>
        /// Returns the rank of the "host" process, if any.
        /// </summary>
        public static int? HostRank
        {
            get
            {
                int rank;
                int flag;

                unsafe
                {
                    int errorCode = Unsafe.MPI_Attr_get(Unsafe.MPI_COMM_WORLD, Unsafe.MPI_HOST, new IntPtr(&rank), out flag);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw TranslateErrorIntoException(errorCode);
                }

                if (flag == 0)
                    return null;
                else if (rank == Unsafe.MPI_PROC_NULL)
                    return null;
                else
                    return rank;
            }
        }

        /// <summary>
        /// Returns the rank of the process (or processes) that can perform I/O via the
        /// normal language facilities. If no such rank exists, the result will be null; if 
        /// every process can perform I/O, this will return the value
        /// <see cref="Communicator.anySource"/>.
        /// </summary>
        public static int? IORank
        {
            get
            {
                int rank;
                int flag;

                unsafe
                {
                    int errorCode = Unsafe.MPI_Attr_get(Unsafe.MPI_COMM_WORLD, Unsafe.MPI_HOST, new IntPtr(&rank), out flag);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw TranslateErrorIntoException(errorCode);
                }

                if (flag == 0)
                    return null;
                else if (rank == Unsafe.MPI_PROC_NULL)
                    return null;
                else
                    return rank;
            }
        }
        /// <summary>
        /// Terminates all processes.
        /// </summary>
        /// <param name="errorCode">An error code that will be returned to the invoking environment.</param>
        public static void Abort(int errorCode)
        {
            Unsafe.MPI_Abort(Unsafe.MPI_COMM_WORLD, errorCode);
        }

        /// <summary>
        /// Translates an MPI error code into an appropriate exception, then throws that exception.
        /// </summary>
        /// <param name="errorCode">
        ///   The MPI error code, returned from one of the <see cref="Unsafe"/> class's <c>MPI</c> routines.
        /// </param>
        public static System.Exception TranslateErrorIntoException(int errorCode)
        {
            // Determine the class of error
            int errorClass;
            unsafe
            {
                Unsafe.MPI_Error_class(errorCode, out errorClass);
            }

            // Ask the MPI implementation for a string describing the error
            byte[] errorStringBuffer = new byte[Unsafe.MPI_MAX_ERROR_STRING];
            int errorStringLength = Unsafe.MPI_MAX_ERROR_STRING;
            string errorString;
            unsafe
            {
                Unsafe.MPI_Error_string(errorCode, errorStringBuffer, ref errorStringLength);
                fixed (byte* bufferPtr = errorStringBuffer)
                {
                    errorString = Marshal.PtrToStringAnsi(new IntPtr(bufferPtr), errorStringLength);
                }
            }

            // Map error class into the most appropriate exception
            Exception result;
            switch (errorClass)
            {
                case Unsafe.MPI_SUCCESS: return null;

                case Unsafe.MPI_ERR_BUFFER: result = new ArgumentException(errorString, "buffer"); break;
                case Unsafe.MPI_ERR_COUNT: result = new ArgumentException(errorString, "count"); break;
                case Unsafe.MPI_ERR_TYPE: result = new ArgumentException(errorString, "datatype"); break;
                case Unsafe.MPI_ERR_TAG: result = new ArgumentException(errorString, "tag"); break;
                case Unsafe.MPI_ERR_COMM: result = new ArgumentException(errorString, "comm"); break;
                case Unsafe.MPI_ERR_RANK: result = new ArgumentException(errorString, "rank"); break;
                case Unsafe.MPI_ERR_REQUEST: result = new ArgumentException(errorString, "request"); break;
                case Unsafe.MPI_ERR_ROOT: result = new ArgumentException(errorString, "root"); break;
                case Unsafe.MPI_ERR_GROUP: result = new ArgumentException(errorString, "group"); break;
                case Unsafe.MPI_ERR_OP: result = new ArgumentException(errorString, "op"); break;
                case Unsafe.MPI_ERR_TOPOLOGY: result = new ArgumentException(errorString, "comm"); break;
                case Unsafe.MPI_ERR_DIMS: result = new ArgumentException(errorString, "dims"); break;
                case Unsafe.MPI_ERR_ARG: result = new ArgumentException(errorString); break;
                case Unsafe.MPI_ERR_FILE: result = new ArgumentException(errorString, "file"); break;
                case Unsafe.MPI_ERR_WIN: result = new ArgumentException(errorString, "window"); break;
                case Unsafe.MPI_ERR_TRUNCATE: result = new MessageTruncatedException(errorString); break;
                
                default:
                    result = new System.Exception(errorString);
                    break;
            }

            // Add some custom keys that describe the error in more detail
            result.Data["MPI Error Code"] = errorCode;
            result.Data["MPI Error Class"] = errorClass;

            return result;
        }

        /// <summary>
        /// The level of threading support actually provided by MPI.
        /// </summary>
        private static MPI.Threading providedThreadLevel;
    }
}
