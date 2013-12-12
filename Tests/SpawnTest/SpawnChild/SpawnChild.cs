using MPI;
using System;
//using System.Threading;

namespace SpawnChild
{
    class SpawnChild
    {
        static void Main(string[] args)
        {
            using (new MPI.Environment(ref args))
            {          
                Intercommunicator parent;
                parent = MPI.Communicator.world.Parent;
                //if (parent == null) 
                //    System.Console.WriteLine(MPI.Communicator.world.Rank.ToString() + " has no parent!");
                //System.Console.WriteLine("Parent = " + parent);

                //System.Threading.Thread.Sleep(1000);

                System.Console.WriteLine("Hello, from process number "
                    + MPI.Communicator.world.Rank.ToString() + " of "
                    + MPI.Communicator.world.Size.ToString());

                parent.Barrier();
            }
        }
    }
}
