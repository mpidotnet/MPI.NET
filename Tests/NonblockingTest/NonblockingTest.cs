/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises MPI.NET's non-blocking point-to-point facilities.
 */
using System;
using MPI;
using System.Collections.Generic;
using MPI.TestCommons;

class NonblockingTest
{
    static void TestRequests(Intracommunicator comm, RequestList requestList)
    {
        int datum = comm.Rank;
        int expectedDatum = (comm.Rank + comm.Size - 1) % comm.Size;

        int[] intArraySendBuffer = new int[comm.Rank + 1];
        string[] strArraySendBuffer = new string[comm.Rank + 1];
        for (int i = 0; i <= comm.Rank; ++i)
        {
            intArraySendBuffer[i] = i;
            strArraySendBuffer[i] = i.ToString();
        }

        int[] intArrayRecvBuffer = new int[expectedDatum + 1];
        string[] strArrayRecvBuffer = new string[expectedDatum + 1];
        Request[] requests = new Request[8];
        requests[0] = comm.ImmediateReceive<int>(Communicator.anySource, 0);
        requests[1] = comm.ImmediateReceive<string>(Communicator.anySource, 1);
        requests[2] = comm.ImmediateReceive(Communicator.anySource, 2, intArrayRecvBuffer);
        requests[3] = comm.ImmediateReceive(Communicator.anySource, 3, strArrayRecvBuffer);
        requests[4] = comm.ImmediateSend(datum, (comm.Rank + 1) % comm.Size, 0);
        requests[5] = comm.ImmediateSend(datum.ToString(), (comm.Rank + 1) % comm.Size, 1);
        requests[6] = comm.ImmediateSend(intArraySendBuffer, (comm.Rank + 1) % comm.Size, 2);
        requests[7] = comm.ImmediateSend(strArraySendBuffer, (comm.Rank + 1) % comm.Size, 3);

        if (requestList == null)
        {
            // Complete all communications manually
            bool allDone = false;
            while (!allDone)
            {
                allDone = true;
                for (int i = 0; i < requests.Length; ++i)
                    allDone = allDone && requests[i].Test() != null;
            }
        }
        else
        {
            // Use the request list to complete all communications
            for (int i = 0; i < requests.Length; ++i)
                requestList.Add(requests[i]);
            requestList.WaitAll();
        }

        ReceiveRequest intRecv = (ReceiveRequest)requests[0];
        CompletedStatus intStatus = intRecv.Wait();
        if ((int)intRecv.GetValue() != expectedDatum
            || intStatus.Source != expectedDatum
            || intStatus.Tag != 0)
        {
            System.Console.Error.WriteLine("error in non-blocking receive of integer: got " + (int)intRecv.GetValue() + " from " 
                + intStatus.Source + " on tag " + intStatus.Tag + ", expected " + expectedDatum);
            MPI.Environment.Abort(-1);
        }

        ReceiveRequest strRecv = (ReceiveRequest)requests[1];
        CompletedStatus strStatus = strRecv.Wait();
        if ((string)strRecv.GetValue() != expectedDatum.ToString()
            || strStatus.Source != expectedDatum
            || strStatus.Tag != 1)
        {
            System.Console.Error.WriteLine("error in non-blocking receive of string: got " + strRecv.GetValue() + " from "
            + strStatus.Source + " on tag " + strStatus.Tag + ", expected " + expectedDatum);
            MPI.Environment.Abort(-1);
        }

        ReceiveRequest intArrayRecv = (ReceiveRequest)requests[2];
        CompletedStatus intArrayStatus = intArrayRecv.Wait();
        if (intArrayRecv.GetValue() != intArrayRecvBuffer
            || intArrayStatus.Source != expectedDatum
            || intArrayStatus.Tag != 2 )
        {
            System.Console.WriteLine("error: received into the wrong integer array");
            MPI.Environment.Abort(-1);
        }
        for (int i = 0; i <= expectedDatum; ++i)
        {
            if (intArrayRecvBuffer[i] != i)
            {
                System.Console.WriteLine("error: intArrayRecv[" + i + "] is " + intArrayRecvBuffer[i] + ", expected " + i);
                MPI.Environment.Abort(-1);
            }
        }

        ReceiveRequest strArrayRecv = (ReceiveRequest)requests[3];
        CompletedStatus strArrayStatus = strArrayRecv.Wait();
        if (strArrayRecv.GetValue() != strArrayRecvBuffer
            || strArrayStatus.Source != expectedDatum
            || strArrayStatus.Tag != 3)
        {
            System.Console.WriteLine("error: received into the wrong string array");
            MPI.Environment.Abort(-1);
        }
        for (int i = 0; i <= expectedDatum; ++i)
        {
            if (strArrayRecvBuffer[i] != i.ToString())
            {
                System.Console.WriteLine("error: strArrayRecv[" + i + "] is " + strArrayRecvBuffer[i] + ", expected " + i.ToString());
                MPI.Environment.Abort(-1);
            }
        }    
    }

    static void TestCancellation(Communicator comm)
    {
        int datum = comm.Rank;
        int expectedDatum = (comm.Rank + comm.Size - 1) % comm.Size;
        int[] intArrayRecvBuffer = new int[expectedDatum + 1];
        string[] strArrayRecvBuffer = new string[expectedDatum + 1];

        int[] intArraySendBuffer = new int[comm.Rank + 1];
        string[] strArraySendBuffer = new string[comm.Rank + 1];
        for (int i = 0; i <= comm.Rank; ++i)
        {
            intArraySendBuffer[i] = i;
            strArraySendBuffer[i] = i.ToString();
        }

        // Test cancellation of receive requests
        Request[] requests = new Request[4];
        requests[0] = comm.ImmediateReceive<int>(Communicator.anySource, 0);
        requests[1] = comm.ImmediateReceive<string>(Communicator.anySource, 1);
        requests[2] = comm.ImmediateReceive(Communicator.anySource, 2, intArrayRecvBuffer);
        requests[3] = comm.ImmediateReceive(Communicator.anySource, 3, strArrayRecvBuffer);

        // Cancel all of these requests.
        requests[0].Cancel();
        requests[1].Cancel();
        requests[2].Cancel();
        requests[3].Cancel();

        // Check that the requests were actually cancelled
        for (int i = 0; i < 4; ++i)
        {
            if (!requests[0].Wait().Cancelled)
            {
                System.Console.Error.WriteLine("error: cancelled receive request " 
                    + i.ToString() + " not marked as cancelled");
                comm.Abort(1);
            }
        }

        comm.Barrier();
    }

    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            if (MPI.Communicator.world.Rank == 0)
                System.Console.WriteLine("Test non-blocking communication via polling...");
            TestRequests(MPI.Communicator.world, null);
            if (MPI.Communicator.world.Rank == 0)
                System.Console.WriteLine("Test non-blocking communication via a request list...");
            TestRequests(MPI.Communicator.world, new RequestList());
            if (MPI.Communicator.world.Rank == 0)
                System.Console.WriteLine("Test cancellation of non-blocking communication...");
            TestCancellation(MPI.Communicator.world);
            if (MPI.Communicator.world.Rank == 0)
                System.Console.WriteLine("All tests passed.");
        }
    }
}

