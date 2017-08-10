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

    [FlagsAttribute]
    public enum MPIDebugOption
    {
        None = 0,
        SysDiagnostic = 1,
        ThrowMPIDebugException = 2
    }

    public class MPIDebug
    {
        public static MPIDebugOption Behavior = MPIDebugOption.None;
        static MPIDebug()
        {
            MPIDebugOption behavior = MPIDebugOption.None;
            if (!string.IsNullOrEmpty(System.Environment.GetEnvironmentVariable("ThrowMPIDebugException")))
                behavior = behavior | MPIDebugOption.ThrowMPIDebugException;
            if (!string.IsNullOrEmpty(System.Environment.GetEnvironmentVariable("MPIDebugSysDiagnostic")))
                behavior = behavior | MPIDebugOption.SysDiagnostic;
            Behavior = behavior;
        }
        public static bool Success = true;

        public static void Assert(bool condition)
        {
            Success = Success && condition;
            if ((Behavior & MPIDebugOption.SysDiagnostic) == MPIDebugOption.SysDiagnostic)
                Debug.Assert(condition);
            if ((Behavior & MPIDebugOption.ThrowMPIDebugException) == MPIDebugOption.ThrowMPIDebugException)
                if (!condition)
                    throw new MPIDebugException("A condition was not met");
        }

        public static void Assert(bool condition, string message)
        {
            Success = Success && condition;
            if ((Behavior & MPIDebugOption.SysDiagnostic) == MPIDebugOption.SysDiagnostic)
                Debug.Assert(condition, message);
            if ((Behavior & MPIDebugOption.ThrowMPIDebugException) == MPIDebugOption.ThrowMPIDebugException)
                if (!condition)
                    throw new MPIDebugException(message);
        }

        public static void Assert(bool condition, string message, string detailMessage)
        {
            Success = Success && condition;

            if ((Behavior & MPIDebugOption.SysDiagnostic) == MPIDebugOption.SysDiagnostic)
                Debug.Assert(condition, message, detailMessage);
            if ((Behavior & MPIDebugOption.ThrowMPIDebugException) == MPIDebugOption.ThrowMPIDebugException)
                if (!condition)
                    throw new MPIDebugException(message + " - " + detailMessage);
        }

        public static void Assert(bool condition, string message, string detailMessageFormat, params object[] args)
        {
            Success = Success && condition;
            if ((Behavior & MPIDebugOption.SysDiagnostic) == MPIDebugOption.SysDiagnostic)
                Debug.Assert(condition, message, detailMessageFormat, args);
            if ((Behavior & MPIDebugOption.ThrowMPIDebugException) == MPIDebugOption.ThrowMPIDebugException)
                if (!condition)
                    throw new MPIDebugException(message + " - " + String.Format(detailMessageFormat, args));
        }

        public static int Execute(Action<string[]> doTest, string[] args)
        {
            using (new MPI.Environment(ref args))
            {
                try
                {
                    doTest(args);
                }
                catch (MPIDebugException e)
                {
                    Console.Error.WriteLine("===============================");
                    Console.Error.WriteLine("ABORTING - exception caught:");
                    Console.Error.WriteLine(e.Message);
                    Console.Error.WriteLine("ABORTING - exception caught and early fail option is on (ThrowMPIDebugException env var)");
                    Console.Error.WriteLine("===============================");
                    Console.Error.WriteLine();
                    System.Environment.ExitCode = 1;
                    if ((Behavior & MPIDebugOption.ThrowMPIDebugException) == MPIDebugOption.ThrowMPIDebugException)
                        MPI.Environment.Abort(-1);
                }
                System.Environment.ExitCode = MPIDebug.Success ? 0 : 1;
                return System.Environment.ExitCode;
            }
        }
    }
}
