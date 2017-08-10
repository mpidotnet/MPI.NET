using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using MPI;
using MPI.TestCommons;

class SendReceiveTest
{
    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        //using (MPI.Environment env = new MPI.Environment(ref args))
        {

            Intracommunicator comm = MPI.Communicator.world;

            int recvValue_i;
            string recvValue_s;
            int[] recvValues_i;
            int[] sendValues_i;
            string[] recvValues_s;
            string[] sendValues_s;

            if (comm.Rank == 0)
                System.Console.Write("Testing SendReceive of integer...");
            for (int i = 0; i < comm.Size; i++)
            {
                comm.SendReceive(comm.Rank, i, 0, out recvValue_i);
                MPIDebug.Assert(recvValue_i == i);
            }
            if (comm.Rank == 0)
                System.Console.WriteLine(" done.");

            if (comm.Rank == 0)
                System.Console.Write("Testing SendReceive of string...");
            for (int i = 0; i < comm.Size; i++)
            {
                comm.SendReceive(comm.Rank.ToString(), i, 0, out recvValue_s);
                MPIDebug.Assert(recvValue_s == i.ToString());
            }
            if (comm.Rank == 0)
                System.Console.WriteLine(" done.");

            if (comm.Rank == 0)
                System.Console.Write("Testing SendReceive of integers...");
            sendValues_i = new int[comm.Rank];
            for (int i = 0; i < comm.Rank; i++)
                sendValues_i[i] = comm.Rank;
            for (int i = 0; i < comm.Size; i++)
            {
                recvValues_i = new int[i];
                comm.SendReceive(sendValues_i, i, 0, ref recvValues_i);
                for (int j = 0; j < i; j++)
                    MPIDebug.Assert(recvValues_i[j] == i);
            }
            if (comm.Rank == 0)
                System.Console.WriteLine(" done.");

            if (comm.Rank == 0)
                System.Console.Write("Testing SendReceive of strings...");
            sendValues_s = new string[comm.Rank];
            for (int i = 0; i < comm.Rank; i++)
                sendValues_s[i] = comm.Rank.ToString();
            for (int i = 0; i < comm.Size; i++)
            {
                recvValues_s = new string[i];
                comm.SendReceive(sendValues_s, i, 0, ref recvValues_s);
                for (int j = 0; j < i; j++)
                    MPIDebug.Assert(recvValues_s[j] == i.ToString());
            }
            if (comm.Rank == 0)
                System.Console.WriteLine(" done.");
        
        }
    }
}
