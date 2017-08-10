using System;
using System.Collections.Generic;
using System.Text;
using MPI;

using System.Diagnostics;
using MPI.TestCommons;

namespace IntercommunicatorTest
{
    static class IntercommunicatorTest
    {
        private static int CompareNumberStrings(string a, string b)
        {
            int x, y;
            x = Int32.Parse(a);
            y = Int32.Parse(b);
            return x.CompareTo(y);
        }

        static int Main(string[] args)
        {
            return MPIDebug.Execute(DoTest, args);
        }

        public static void DoTest(string[] args)
        {
                int rank = Communicator.world.Rank;
                Intracommunicator sub_comm = (Intracommunicator)Communicator.world.Split(rank % 2, rank);

                Intercommunicator inter_comm;
                if (rank % 2 == 0)
                    inter_comm = new Intercommunicator(sub_comm, 0, Communicator.world, 1, 0);
                else
                    inter_comm = new Intercommunicator(sub_comm, 0, Communicator.world, 0, 0);

                int j;
                int p;
                int size;
                int outValue_i;
                string outValue_s;
                int[] inValues_i;
                int[] outValues_i;
                string[] inValues_s;
                string[] outValues_s;
                int checkValue_i;
                int[] checkValues_i;
                string checkValue_s;
                string[] checkValues_s;
                bool success = true;
                int[] remoteSuccess = null;

                // Gather with ints
                if (rank == 0)
                {
                    outValues_i = inter_comm.Gather(inter_comm.Rank, Intercommunicator.Root);
                    inter_comm.Gather(inter_comm.Rank, 0);
                }
                else if (rank == 1)
                {
                    outValues_i = inter_comm.Gather(inter_comm.Rank, 0);
                    outValues_i = inter_comm.Gather(inter_comm.Rank, Intercommunicator.Root);
                }
                else if (rank % 2 == 1)
                {
                    outValues_i = inter_comm.Gather(inter_comm.Rank, 0);
                    outValues_i = inter_comm.Gather(inter_comm.Rank, Intercommunicator.Null);
                }
                else
                {
                    outValues_i = inter_comm.Gather(inter_comm.Rank, Intercommunicator.Null);
                    outValues_i = inter_comm.Gather(inter_comm.Rank, 0);
                }
                if (rank == 0 || rank == 1)
                {
                    Array.Sort(outValues_i);
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                        if (i != outValues_i[i])
                            success = false;
                    System.Console.WriteLine("Rank " + rank + ": Gather<int> Passed == " + success);
                    MPIDebug.Assert(success);
                }
                success = true;


                // Gather with strings
                if (rank == 0)
                {
                    outValues_s = inter_comm.Gather(inter_comm.Rank.ToString(), Intercommunicator.Root);
                    inter_comm.Gather(inter_comm.Rank.ToString(), 0);
                }
                else if (rank == 1)
                {
                    outValues_s = inter_comm.Gather(inter_comm.Rank.ToString(), 0);
                    outValues_s = inter_comm.Gather(inter_comm.Rank.ToString(), Intercommunicator.Root);
                }
                else if (rank % 2 == 1)
                {
                    outValues_s = inter_comm.Gather(inter_comm.Rank.ToString(), 0);
                    outValues_s = inter_comm.Gather(inter_comm.Rank.ToString(), Intercommunicator.Null);
                }
                else
                {
                    outValues_s = inter_comm.Gather(inter_comm.Rank.ToString(), Intercommunicator.Null);
                    outValues_s = inter_comm.Gather(inter_comm.Rank.ToString(), 0);
                }
                if (rank == 0 || rank == 1)
                {
                    Array.Sort(outValues_s, CompareNumberStrings);
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                        if (i.ToString() != outValues_s[i])
                            success = false;
                    System.Console.WriteLine("Rank " + rank + ": Gather<string> Passed == " + success);
                }
                MPIDebug.Assert(success);
                success = true;


                // Scatter with ints
                inValues_i = new int[inter_comm.RemoteSize];
                if (rank == 0 || rank == 1)
                {
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                        inValues_i[i] = i;
                }
                if (rank == 0)
                {
                    inter_comm.Scatter<int>(inValues_i);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Rank " + rank + ": Scatter<int> Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                    outValue_i = inter_comm.Scatter<int>(0);
                    inter_comm.Gather<int>((inter_comm.Rank == outValue_i ? 1 : 0), 0);
                }
                else if (rank == 1)
                {
                    outValue_i = inter_comm.Scatter<int>(0);
                    inter_comm.Gather<int>((inter_comm.Rank == outValue_i ? 1 : 0), 0);
                    inter_comm.Scatter<int>(inValues_i);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    System.Console.WriteLine();
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Rank " + rank + ": Scatter<int> Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 1)
                {
                    outValue_i = inter_comm.Scatter<int>(0);
                    inter_comm.Gather<int>((inter_comm.Rank == outValue_i ? 1 : 0), 0);
                    inter_comm.Scatter<int>();
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                }
                else
                {
                    inter_comm.Scatter<int>();
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                    outValue_i = inter_comm.Scatter<int>(0);
                    inter_comm.Gather<int>((inter_comm.Rank == outValue_i ? 1 : 0), 0);
                }

                // Scatter with strings (MPI_DATATYPE_NULL)
                inValues_s = new string[inter_comm.RemoteSize];
                if (rank == 0 || rank == 1)
                {
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                        inValues_s[i] = i.ToString();
                }
                if (rank == 0)
                {
                    inter_comm.Scatter(inValues_s);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Rank " + rank + ": Scatter<string> Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                    outValue_s = inter_comm.Scatter<string>(0);
                    inter_comm.Gather<int>((inter_comm.Rank.ToString() == outValue_s ? 1 : 0), 0);

                }
                else if (rank == 1)
                {
                    outValue_s = inter_comm.Scatter<string>(0);
                    inter_comm.Gather<int>((inter_comm.Rank.ToString() == outValue_s ? 1 : 0), 0);
                    inter_comm.Scatter(inValues_s);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Rank " + rank + ": Scatter<string> Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 1)
                {
                    outValue_s = inter_comm.Scatter<string>(0);
                    inter_comm.Gather<int>((inter_comm.Rank.ToString() == outValue_s ? 1 : 0), 0);
                    inter_comm.Scatter<string>();
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                }
                else
                {
                    inter_comm.Scatter<string>();
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                    outValue_s = inter_comm.Scatter<string>(0);
                    inter_comm.Gather<int>((inter_comm.Rank.ToString() == outValue_s ? 1 : 0), 0);
                }


                // Broadcast with int
                int data = 0;
                if (rank == 0)
                {
                    data = 5;
                    inter_comm.Broadcast(ref data, Intercommunicator.Root);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b != 1)
                            success = false;
                    System.Console.WriteLine("Rank " + rank + ": Broadcast<int>(int) Passed == " + success);
                    MPIDebug.Assert(success);
                }
                else if (rank % 2 == 1)
                {
                    inter_comm.Broadcast(ref data, 0);
                    remoteSuccess = inter_comm.Gather<int>((data == 5 ? 1 : 0), 0);
                }
                else
                {
                    inter_comm.Broadcast(ref data, Intercommunicator.Null);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Null);
                }
                // System.Console.WriteLine("Broadcast Test: Process " + rank + " has data == " + data);
                success = true;

                // Broadcast with string
                string data_s = "";
                if (rank == 0)
                {
                    data_s = "x";
                    inter_comm.Broadcast(ref data_s, Intercommunicator.Root);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b != 1)
                            success = false;
                    System.Console.WriteLine("Rank " + rank + ": Broadcast<string>(string) Passed == " + success);
                    MPIDebug.Assert(success);
                }
                else if (rank % 2 == 1)
                {
                    inter_comm.Broadcast(ref data_s, 0);
                    remoteSuccess = inter_comm.Gather<int>((data_s == "x" ? 1 : 0), 0);
                }
                else
                {
                    inter_comm.Broadcast(ref data_s, Intercommunicator.Null);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Null);
                }
                success = true;

                // Broadcast with int[]
                success = true;
                inValues_i = new int[] { 0, 1 };
                outValues_i = null;
                if (rank == 0)
                {
                    outValues_i = new int[inValues_i.Length];
                    inValues_i.CopyTo(outValues_i, 0);
                    inter_comm.Broadcast(ref outValues_i, Intercommunicator.Root);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Broadcast<int>(int[]) Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 0)
                {
                    inter_comm.Broadcast(ref outValues_i, Intercommunicator.Null);
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                }
                else
                {
                    outValues_i = new int[inValues_i.Length];
                    inter_comm.Broadcast(ref outValues_i, 0);
                    for (int i = 0; i < outValues_i.Length; i++)
                        if (inValues_i[i] != outValues_i[i])
                            success = false;
                    inter_comm.Gather<int>((success ? 1 : 0), 0);
                }

                // Broadcast with string[]
                success = true;
                inValues_s = new string[] { "0", "1" };
                outValues_s = null;
                if (rank == 0)
                {
                    outValues_s = new string[inValues_s.Length];
                    inValues_s.CopyTo(outValues_s, 0);
                    inter_comm.Broadcast(ref outValues_s, Intercommunicator.Root);
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Broadcast<string>(string[]) Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 0)
                {
                    inter_comm.Broadcast(ref outValues_s, Intercommunicator.Null);
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                }
                else
                {
                    outValues_s = new string[inValues_s.Length];
                    inter_comm.Broadcast(ref outValues_s, 0);
                    for (int i = 0; i < outValues_s.Length; i++)
                        if (inValues_s[i] != outValues_s[i])
                            success = false;
                    inter_comm.Gather<int>((success ? 1 : 0), 0);
                }

                // Barrier test
                if (rank == 0)
                    System.Console.WriteLine("Barrier Test...");
                inter_comm.Barrier();
                //System.Console.WriteLine("Process " + rank + " has exited Barrier...");
                if (rank == 0)
                    System.Console.WriteLine("Barrier Test Passed == True");

                // Allgather test with strings
                checkValues_s = new string[inter_comm.RemoteSize];
                j = 1 - rank % 2;
                for (int i = 0; i < inter_comm.RemoteSize; i++)
                {
                    checkValues_s[i] += j.ToString();
                    j += 2;
                }
                success = true;
                outValues_s = inter_comm.Allgather(rank.ToString());
                for (int i = 0; i < inter_comm.RemoteSize; i++)
                    if (outValues_s[i] != checkValues_s[i])
                        success = false;
                if (rank == 0)
                {
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Allgather<string>() Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 0)
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                else
                    inter_comm.Gather<int>((success ? 1 : 0), 0);

                // Allgather test with ints
                checkValues_i = new int[inter_comm.RemoteSize];
                j = 1 - rank % 2;
                for (int i = 0; i < inter_comm.RemoteSize; i++)
                {
                    checkValues_i[i] += j;
                    j += 2;
                }
                success = true;
                outValues_i = inter_comm.Allgather(rank);
                for (int i = 0; i < inter_comm.RemoteSize; i++)
                    if (outValues_i[i] != checkValues_i[i])
                        success = false;
                if (rank == 0)
                {
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Allgather<int>() Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 0)
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                else
                    inter_comm.Gather<int>((success ? 1 : 0), 0);

                // Alltoall with ints
                success = true;
                inValues_i = new int[inter_comm.RemoteSize];
                for (int dest = 0; dest < inter_comm.RemoteSize; ++dest)
                    inValues_i[dest] = inter_comm.Rank;
                outValues_i = inter_comm.Alltoall(inValues_i);
                for (int source = 0; source < inter_comm.RemoteSize; ++source)
                {
                    if (source != outValues_i[source])
                        success = false;
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Alltoall<int>(int[]) Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }

                // Alltoall with strings
                success = true;
                inValues_s = new string[inter_comm.RemoteSize];
                for (int dest = 0; dest < inter_comm.RemoteSize; ++dest)
                    inValues_s[dest] = inter_comm.Rank.ToString();
                outValues_s = inter_comm.Alltoall(inValues_s);
                for (int source = 0; source < inter_comm.RemoteSize; ++source)
                {
                    // if (inter_comm.Rank.ToString() != alltoalled_data[source])
                    if (source.ToString() != outValues_s[source])
                        success = false;
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Alltoall<string>(string[]) Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }


                // Reduce with int
                if (rank == 0)
                {
                    outValue_i = inter_comm.Reduce(0, Operation<int>.Add, Intercommunicator.Root);
                    //System.Console.WriteLine("Received " + y + " from Reduce()");
                    checkValue_i = inter_comm.RemoteSize * inter_comm.RemoteSize;
                    System.Console.WriteLine("Reduce<int>(int) Passed == " + (checkValue_i == outValue_i));
                    MPIDebug.Assert(checkValue_i == outValue_i);
                }
                else if (rank % 2 == 0)
                    outValue_i = inter_comm.Reduce(0, Operation<int>.Add, Intercommunicator.Null);
                else
                    outValue_i = inter_comm.Reduce(rank, Operation<int>.Add, 0);

                // Reduce with string
                checkValue_s = "";
                if (rank == 0)
                {
                    outValue_s = inter_comm.Reduce(rank.ToString(), Operation<string>.Add, Intercommunicator.Root);
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        checkValue_s += j.ToString();
                        j += 2;
                    }
                    System.Console.WriteLine("Reduce<string>(string) (Passed == " + (outValue_s == checkValue_s) + ")");
                }
                else if (rank % 2 == 1)
                    inter_comm.Reduce(rank.ToString(), Operation<string>.Add, 0);
                else
                    inter_comm.Reduce(rank.ToString(), Operation<string>.Add, Intercommunicator.Null);

                // Reduce(int[])
                success = true;
                outValues_i = null;
                inValues_i = null;
                if (rank == 0)
                {
                    outValues_i = new int[inter_comm.RemoteSize];
                    inter_comm.Reduce<int>(null, Operation<int>.Add, Intercommunicator.Root, ref outValues_i);
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        //System.Console.WriteLine(sums[i] + " " + inter_comm.RemoteSize*inter_comm.RemoteSize);
                        if (outValues_i[i] != inter_comm.RemoteSize * inter_comm.RemoteSize)
                            success = false;
                        j += 2;
                    }
                    System.Console.WriteLine("Reduce<int>(int[]) Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 1)
                {
                    inValues_i = new int[inter_comm.Size];
                    for (int i = 0; i < inter_comm.Size; i++)
                        inValues_i[i] = rank;
                    inter_comm.Reduce<int>(inValues_i, Operation<int>.Add, 0, ref outValues_i);
                }
                else
                {
                    inValues_i = new int[0];
                    inter_comm.Reduce<int>(null, Operation<int>.Add, Intercommunicator.Null, ref outValues_i);
                }


                // Reduce(string[])
                success = true;
                outValues_s = null;
                checkValue_s = "";
                if (rank == 0)
                {
                    outValues_s = new string[inter_comm.RemoteSize];
                    inter_comm.Reduce<string>(null, Operation<string>.Add, Intercommunicator.Root, ref outValues_s);
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        checkValue_s += j.ToString();
                        j += 2;
                    }
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        //System.Console.WriteLine(sums[i] + " " + inter_comm.RemoteSize*inter_comm.RemoteSize);
                        if (outValues_s[i] != checkValue_s)
                            success = false;
                        j += 2;
                    }
                    System.Console.WriteLine("Reduce<string>(string[]) Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 1)
                {
                    inValues_s = new string[inter_comm.Size];
                    for (int i = 0; i < inter_comm.Size; i++)
                        inValues_s[i] = rank.ToString();
                    inter_comm.Reduce<string>(inValues_s, Operation<string>.Add, 0, ref outValues_s);
                }
                else
                {
                    inter_comm.Reduce<string>(null, Operation<string>.Add, Intercommunicator.Null, ref outValues_s);
                }


                // Allreduce with ints
                outValue_i = inter_comm.Allreduce(rank, Operation<int>.Add);
                success = (outValue_i == (inter_comm.RemoteSize - (rank % 2)) * inter_comm.RemoteSize);
                if (rank == 0)
                {
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Allreduce<int> Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 0)
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                else
                    inter_comm.Gather<int>((success ? 1 : 0), 0);
                success = true;

                // Allreduce with strings
                outValue_s = inter_comm.Allreduce(rank.ToString(), Operation<string>.Add);
                checkValue_s = "";
                j = 1 - rank % 2;
                for (int i = 0; i < inter_comm.RemoteSize; i++)
                {
                    checkValue_s += j.ToString();
                    j += 2;
                }
                success = (outValue_s == checkValue_s);
                if (rank == 0)
                {
                    remoteSuccess = inter_comm.Gather<int>(1, Intercommunicator.Root);
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Allreduce<string> Passed == " + success);
                    MPIDebug.Assert(success);
                    success = true;
                }
                else if (rank % 2 == 0)
                    inter_comm.Gather<int>(1, Intercommunicator.Null);
                else
                    inter_comm.Gather<int>((success ? 1 : 0), 0);
                success = true;

                // Allreduce<int>(int[])
                // If we have an odd size, simply calling Allreduce won't work, since the data provided
                // by both groups has to be the same size
                int larger_group_size = (inter_comm.Size > inter_comm.RemoteSize ? inter_comm.Size : inter_comm.RemoteSize);
                outValues_i = new int[larger_group_size];
                inValues_i = new int[larger_group_size];
                for (int i = 0; i < inter_comm.Size; i++)
                    inValues_i[i] = rank;
                if (larger_group_size > inter_comm.Size)
                    inValues_i[larger_group_size - 1] = 0;
                inter_comm.Allreduce<int>(inValues_i, Operation<int>.Add, ref outValues_i);
                if (rank % 2 == 0)
                {
                    checkValue_i = inter_comm.RemoteSize * inter_comm.RemoteSize;
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        if (outValues_i[i] != checkValue_i)
                            success = false;
                        j += 2;
                    }
                    if (larger_group_size > inter_comm.RemoteSize)
                        if (outValues_i[larger_group_size - 1] != 0)
                            success = false;
                }
                else if (rank % 2 == 1)
                {
                    checkValue_i = (inter_comm.RemoteSize - 1) * inter_comm.RemoteSize;
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        if (outValues_i[i] != checkValue_i)
                            success = false;
                        j += 2;
                    }
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Allreduce<int>(int[]) Passed == " + success);
                    MPIDebug.Assert(success);
                }
                success = true;

                // Allreduce<string>(string[])
                outValues_s = new string[larger_group_size];
                inValues_s = new string[larger_group_size];
                for (int i = 0; i < larger_group_size; i++)
                    inValues_s[i] = rank.ToString();
                inter_comm.Allreduce<string>(inValues_s, Operation<string>.Add, ref outValues_s);
                checkValue_s = "";
                j = 1 - rank % 2;
                for (int i = 0; i < inter_comm.RemoteSize; i++)
                {
                    checkValue_s += j.ToString();
                    j += 2;
                }
                if (rank % 2 == 0)
                {
                    j = 1 - rank % 2;
                    for (int i = 0; i < larger_group_size; i++)
                    {
                        if (outValues_s[i] != checkValue_s)
                            success = false;
                        j += 2;
                    }
                }
                else if (rank % 2 == 1)
                {
                    j = 1 - rank % 2;
                    for (int i = 0; i < larger_group_size; i++)
                    {
                        if (outValues_s[i] != checkValue_s)
                            success = false;
                        j += 2;
                    }
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Allreduce<string>(string[]) Passed == " + success);
                    MPIDebug.Assert(success);
                }
                success = true;

                // ReduceScatter<int>
                // This test is just going to repeat the Allreduce test, for lack of a better test
                int smaller_group_size = (inter_comm.Size < inter_comm.RemoteSize ? inter_comm.Size : inter_comm.RemoteSize);
                inValues_i = new int[larger_group_size * smaller_group_size]; // same problem as with Allreduce
                outValues_i = new int[rank % 2 == 1 ? larger_group_size : smaller_group_size];
                for (int i = 0; i < larger_group_size * smaller_group_size; i++)
                    inValues_i[i] = rank;
                int[] counts = new int[rank % 2 == 0 ? larger_group_size : smaller_group_size];
                for (int i = 0; i < (rank % 2 == 0 ? larger_group_size : smaller_group_size); i++)
                    counts[i] = rank % 2 == 1 ? larger_group_size : smaller_group_size;
                inter_comm.ReduceScatter<int>(inValues_i, Operation<int>.Add, counts, ref outValues_i);
                if (rank % 2 == 0)
                {
                    checkValue_i = inter_comm.RemoteSize * inter_comm.RemoteSize;
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        if (outValues_i[i] != checkValue_i)
                            success = false;
                        j += 2;
                    }
                }
                else if (rank % 2 == 1)
                {
                    checkValue_i = (inter_comm.RemoteSize - 1) * inter_comm.RemoteSize;
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        if (outValues_i[i] != checkValue_i)
                            success = false;
                        j += 2;
                    }
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("ReduceScatter<int> Passed == " + success);
                    MPIDebug.Assert(success);
                }
                success = true;

                // ReduceScatter<string>
                // This test is just going to repeat the Allreduce test, for lack of a better test
                inValues_s = new string[larger_group_size * smaller_group_size]; // same problem as with Allreduce
                outValues_s = new string[rank % 2 == 1 ? larger_group_size : smaller_group_size];
                for (int i = 0; i < larger_group_size * smaller_group_size; i++)
                    inValues_s[i] = rank.ToString();
                counts = new int[rank % 2 == 0 ? larger_group_size : smaller_group_size];
                for (int i = 0; i < (rank % 2 == 0 ? larger_group_size : smaller_group_size); i++)
                    counts[i] = rank % 2 == 1 ? larger_group_size : smaller_group_size;
                inter_comm.ReduceScatter<string>(inValues_s, Operation<string>.Add, counts, ref outValues_s);
                checkValue_s = "";
                if (rank % 2 == 0)
                {
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        checkValue_s += j.ToString();
                        j += 2;
                    }
                }
                else
                {
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        checkValue_s += j.ToString();
                        j += 2;
                    }
                }
                if (rank % 2 == 0)
                {
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        if (outValues_s[i] != checkValue_s)
                            success = false;
                        j += 2;
                    }
                }
                else if (rank % 2 == 1)
                {
                    j = 1 - rank % 2;
                    for (int i = 0; i < inter_comm.RemoteSize; i++)
                    {
                        if (outValues_s[i] != checkValue_s)
                            success = false;
                        j += 2;
                    }
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("ReduceScatter<string> Passed == " + success);
                    MPIDebug.Assert(success);
                }
                success = true;


                // GatherFlattened with ints
                if (rank == 0)
                {
                    success = true;
                    System.Console.Write("GatherFlattened<int> Passed == ");
                    size = inter_comm.RemoteSize;
                    outValues_i = new int[(size * size + size) / 2];
                    counts = new int[size];
                    for (int i = 0; i < size; i++)
                        counts[i] = i;
                    inter_comm.GatherFlattened(counts, ref outValues_i);
                    p = 0;
                    for (int i = 0; i < size; ++i)
                    {
                        if (counts[i] > 0)
                            for (j = 0; j < i; j++)
                            {
                                MPIDebug.Assert(outValues_i[p] == i);
                                if (outValues_i[p] != i)
                                    success = false;
                            }
                            p += counts[i];
                    }
                    System.Console.WriteLine(success);
                }
                else if (rank % 2 == 1)
                {
                    counts = null;
                    inValues_i = new int[inter_comm.Rank];
                    for (int i = 0; i < inter_comm.Rank; i++)
                        inValues_i[i] = inter_comm.Rank;
                    inter_comm.GatherFlattened(inValues_i, 0);
                }
                else
                    inter_comm.GatherFlattened<int>();

                // GatherFlattened with strings
                if (rank == 0)
                {
                    success = true;
                    System.Console.Write("GatherFlattened<string> Passed == ");
                    size = inter_comm.RemoteSize;
                    outValues_s = new string[(size * size + size) / 2];
                    counts = new int[size];
                    for (int i = 0; i < size; i++)
                        counts[i] = i;                   
                    inter_comm.GatherFlattened(counts, ref outValues_s);
                    p = 0;
                    for (int i = 0; i < size; ++i)
                    {
                        if (counts[i] > 0)
                            for (j = 0; j < i; j++)
                            {
                                if (outValues_s[p] != i.ToString())
                                    success = false;
                            }
                        p += counts[i];
                    }
                    System.Console.WriteLine(success);
                }
                else if (rank % 2 == 1)
                {
                    counts = null;
                    inValues_s = new string[inter_comm.Rank];
                    for (int i = 0; i < inter_comm.Rank; i++)
                        inValues_s[i] = inter_comm.Rank.ToString();
                    inter_comm.GatherFlattened(inValues_s, 0);
                }
                else
                    inter_comm.GatherFlattened<string>();

                // ScatterFromFlattened with ints
                success = true;
                if (rank == 0)
                {
                    size = inter_comm.RemoteSize;
                    inValues_i = new int[(size * size - size) / 2];
                    counts = new int[size];
                    p = 0;
                    for (int i = 0; i < size; ++i)
                    {
                        counts[i] = i;
                        for (j = 0; j < i; j++)
                            inValues_i[p + j] = i;
                        p += i;
                    }
                    inter_comm.ScatterFromFlattened(inValues_i, counts);
                }
                else if (rank % 2 == 1)
                {
                    outValues_i = null;
                    counts = new int[inter_comm.Size];
                    for (int i = 0; i < inter_comm.Size; ++i)
                        counts[i] = i;

                    inter_comm.ScatterFromFlattened(counts, 0, ref outValues_i);
                    for (int i = 0; i < inter_comm.Rank; i++)
                    {
                        MPIDebug.Assert(outValues_i[i] == inter_comm.Rank);
                        if (outValues_i[i] != inter_comm.Rank)
                            success = false;
                    }
                }
                else
                    inter_comm.ScatterFromFlattened<int>();
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("ScatterFromFlattened<int> Passed == " + success);
                    MPIDebug.Assert(success);
                }
                success = true;

                // ScatterFromFlattened with strings
                success = true;
                if (rank == 0)
                {
                    size = inter_comm.RemoteSize;
                    inValues_s = new string[(size * size - size) / 2];
                    counts = new int[size];
                    p = 0;
                    for (int i = 0; i < size; ++i)
                    {
                        counts[i] = i;
                        for (j = 0; j < i; j++)
                            inValues_s[p + j] = i.ToString();
                        p += i;
                    }
                    inter_comm.ScatterFromFlattened(inValues_s, counts);
                }
                else if (rank % 2 == 1)
                {
                    outValues_s = null;
                    counts = new int[inter_comm.Size];
                    for (int i = 0; i < inter_comm.Size; ++i)
                        counts[i] = i;

                    inter_comm.ScatterFromFlattened(counts, 0, ref outValues_s);
                    for (int i = 0; i < inter_comm.Rank; i++)
                    {
                        MPIDebug.Assert(outValues_i[i] == inter_comm.Rank);
                        if (outValues_s[i] != inter_comm.Rank.ToString())
                            success = false;
                    }
                }
                else
                    inter_comm.ScatterFromFlattened<string>();
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("ScatterFromFlattened<string> Passed == " + success);
                    MPIDebug.Assert(success);
                }
                success = true;

                
                // AllgatherFlattened with ints
                size = inter_comm.RemoteSize;
                outValues_i = new int[(size * size - size) / 2];
                inValues_i = new int[inter_comm.Rank];
                counts = new int[size];
                for (int i = 0; i < inter_comm.Rank; i++)
                    inValues_i[i] = inter_comm.Rank;
                for (int i = 0; i < size; i++)
                    counts[i] = i;
                inter_comm.AllgatherFlattened(inValues_i, counts, ref outValues_i);
                p = 0;
                for (int i = 0; i < size; ++i)
                {
                    if (counts[i] > 0)
                        for (j = 0; j < i; j++)
                        {
                            MPIDebug.Assert(outValues_i[p] == i);
                            if (outValues_i[p] != i)
                                success = false;
                        }
                    p += counts[i];
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("AllgatherFlattened<int> Passed == " + success);
                    MPIDebug.Assert(success);
                }
                success = true;

                // AllgatherFlattened with strings
                size = inter_comm.RemoteSize;
                outValues_s = new string[(size * size - size) / 2];
                inValues_s = new string[inter_comm.Rank];
                counts = new int[size];
                for (int i = 0; i < inter_comm.Rank; i++)
                    inValues_s[i] = inter_comm.Rank.ToString();
                for (int i = 0; i < size; i++)
                    counts[i] = i;
                inter_comm.AllgatherFlattened(inValues_s, counts, ref outValues_s);
                p = 0;
                for (int i = 0; i < size; ++i)
                {
                    if (counts[i] > 0)
                        for (j = 0; j < i; j++)
                        {
                            MPIDebug.Assert(outValues_s[p] == i.ToString());
                            if (outValues_s[p] != i.ToString())
                                success = false;
                        }
                    p += counts[i];
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("AllgatherFlattened<string> Passed == " + success);
                    MPIDebug.Assert(success);
                }
                success = true;

                // AlltoallFlattened with ints
                size = inter_comm.Size;
                int rsize = inter_comm.RemoteSize;
                int irank = inter_comm.Rank;
                outValues_i = new int[(rsize * rsize - rsize) / 2];
                inValues_i = new int[irank * rsize];
                int[] sendCounts = new int[rsize];
                int[] recvCounts = new int[rsize];
                for (int i = 0; i < irank * rsize; i++)
                    inValues_i[i] = irank;
                for (int i = 0; i < rsize; i++)
                    sendCounts[i] = irank;
                for (int i = 0; i < rsize; i++)
                    recvCounts[i] = i;
                inter_comm.AlltoallFlattened(inValues_i, sendCounts, recvCounts, ref outValues_i);
                p = 0;
                for (int i = 0; i < rsize; ++i)
                {
                    if (recvCounts[i] > 0)
                        for (j = 0; j < i; j++)
                        {
                            if (outValues_i[p] != i)
                                success = false;
                            MPIDebug.Assert(outValues_i[p] == i);
                        }
                    p += recvCounts[i];
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Alltoall<int> Passed == " + success);
                }
                success = true;

                // AlltoallFlattened with strings
                outValues_s = new string[(rsize * rsize - rsize) / 2];
                inValues_s = new string[irank * rsize];
                for (int i = 0; i < irank * rsize; i++)
                    inValues_s[i] = irank.ToString();
                inter_comm.AlltoallFlattened(inValues_s, sendCounts, recvCounts, ref outValues_s);
                p = 0;
                for (int i = 0; i < rsize; ++i)
                {
                    if (recvCounts[i] > 0)
                        for (j = 0; j < i; j++)
                        {
                            if (outValues_s[p] != i.ToString())
                                success = false;
                            MPIDebug.Assert(outValues_s[p] == i.ToString());
                        }
                    p += recvCounts[i];
                }
                remoteSuccess = Communicator.world.Gather<int>((success ? 1 : 0), 0);
                if (rank == 0)
                {
                    foreach (int b in remoteSuccess)
                        if (b == 0)
                            success = false;
                    System.Console.WriteLine("Alltoall<string> Passed == " + success);
                }
                success = true;
        }
    }
}
