using MPI;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SpawnTest
{
    class SpawnTest
    {
        static void Main(string[] args)
        {
            using (new MPI.Environment(ref args))
            {
                string path;
                path = "..\\..\\SpawnChild\\bin\\";
#if DEBUG
                path += "Debug\\";
#else
                path += "Release\\";
#endif
                path += "SpawnChild.exe";


                Intracommunicator comm = MPI.Communicator.world;
                Intercommunicator inter_comm;

                string[] argv = new string[0];             

                int maxprocs = 4;
                int[] error_codes = new int[maxprocs];

                //inter_comm = comm.Spawn(path, argv, maxprocs, 0, out error_codes);
                inter_comm = comm.Spawn(path, argv, maxprocs, 0);

                //for (int i = 0; i < error_codes.Length; i++)
                //    System.Console.WriteLine("error[" + i + "] = " + error_codes[i]);

                inter_comm.Barrier();
                inter_comm.Dispose();

            }
        }
    }
}
