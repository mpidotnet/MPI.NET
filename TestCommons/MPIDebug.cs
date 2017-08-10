using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace MPI.TestCommons
{
    class MPIDebugException : System.Exception
    {
        public MPIDebugException() : base() { }
        public MPIDebugException(string message) : base(message) { }
        public MPIDebugException(string message, Exception innerException) : base(message, innerException) { }
    }

    public enum MPIDebugOption
    {
        SysDiagnostic,
        ThrowMPIDebugException
    }

    public class MPIDebug
    {
        public static MPIDebugOption Behavior = MPIDebugOption.ThrowMPIDebugException;
        public static bool Success = true;

        public static void Assert(bool condition)
        {
            Success = Success && condition;
            switch (Behavior)
            {
                case MPIDebugOption.ThrowMPIDebugException:
                    if (!condition)
                        throw new MPIDebugException("A condition was not met");
                    break;
                default:
                    Debug.Assert(condition);
                    break;
            }
        }

        public static void Assert(bool condition, string message)
        {
            Success = Success && condition;
            switch (Behavior)
            {
                case MPIDebugOption.ThrowMPIDebugException:
                    if(!condition)
                        throw new MPIDebugException(message);
                    break;
                default:
                    Debug.Assert(condition, message);
                    break;
            }
        }

        public static void Assert(bool condition, string message, string detailMessage)
        {
            Success = Success && condition;
            switch (Behavior)
            {
                case MPIDebugOption.ThrowMPIDebugException:
                    if (!condition)
                        throw new MPIDebugException(message + " - " + detailMessage);
                    break;
                default:
                    Debug.Assert(condition, message, detailMessage);
                    break;
            }
        }

        public static void Assert(bool condition, string message, string detailMessageFormat, params object[] args)
        {
            Success = Success && condition;
            switch (Behavior)
            {
                case MPIDebugOption.ThrowMPIDebugException:
                    if (!condition)
                        throw new MPIDebugException(message + " - " + String.Format(detailMessageFormat, args));
                    break;
                default:
                    Debug.Assert(condition, message, detailMessageFormat, args);
                    break;
            }
        }

        public static int Execute(Action<string[]> doTest, string[] args)
        {
            try
            {
                doTest(args);
            }
            catch (MPIDebugException e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine();
                return MPIDebug.Success ? 0 : 1;
            }
            return MPIDebug.Success ? 0 : 1;
        }
    }
}
