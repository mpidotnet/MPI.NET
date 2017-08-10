using MPI;
using MPI.TestCommons;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace UnitTestOutcomeTest
{
    class UnitTestOutcomeTest
    {
        static int Main(string[] args)
        {
            return MPIDebug.Execute(DoTest, args);
        }

        public static void DoTest(string[] args)
        {
            Intracommunicator comm = Communicator.world;
            if (comm.Rank == 0)
            {
                Console.WriteLine("Rank 0 is alive and running on " + MPI.Environment.ProcessorName);
                for (int dest = 1; dest < comm.Size; ++dest)
                {
                    Console.Write("Pinging process with rank " + dest + "...");
                    comm.Send("Ping!", dest, 0);
                    string destHostname = comm.Receive<string>(dest, 1);
                    Console.WriteLine(" Pong!");
                    Console.WriteLine("  Rank " + dest + " is alive and running on " + destHostname);
                }
            }
            else
            {
                var brk = System.Environment.GetEnvironmentVariable("BreakUnitTestOutcomeTest");
                if (!string.IsNullOrEmpty(brk))
                {
                    int rankToBreak = int.Parse(brk);
                    if (rankToBreak == comm.Rank)
                        MPIDebug.Assert(false, "Force failure of an assertion in BreakUnitTestOutcomeTest");
                }
                comm.Receive<string>(0, 0);
                comm.Send(MPI.Environment.ProcessorName, 0, 1);
            }
        }
    }
}
