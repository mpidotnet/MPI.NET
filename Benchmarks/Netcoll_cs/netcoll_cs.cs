/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 */


using System;
using System.Collections.Generic;
using System.Text;
using MPI;
using MPI.TestCommons;

struct Stats
{
    public double t;
    public double bps;
    public double variance;
    public int bits;
    public int repeat;
};

public struct DoubleStruct
{
    public DoubleStruct(double value) 
    { 
        this.value = value; 
    }

    public double value;

    public static DoubleStruct operator +(DoubleStruct x, DoubleStruct y)
    {
        return new DoubleStruct(x.value + y.value);
    }
};

[Serializable]
public class DoubleClass
{
    public DoubleClass(double value)
    {
        this.value = value;
    }

    public double value;

    public static DoubleClass operator +(DoubleClass x, DoubleClass y)
    {
        return new DoubleClass(x.value + y.value);
    }
};

class Netcoll_cs
{
    static Intracommunicator comm;
    static int self;
    static double latency, synctime;
    const int PERT = 3, TRIALS = 3, REPEAT = 1000;
    const double RUNTM = 0.25;
    static double stopTime = 0.1;
    static int nSamp = 250, latencyReps = 1000;
    static Stats[] bwstats;

    private delegate void Test(int bufflen, int n, int nRepeat, ref double tlast);

    static void Main(string[] args)
    {
        // By default, test with primitives types and the predefined MPI_SUM
        Test test = testPrimitiveAndPredefined;

        using (new MPI.Environment(ref args))
        {
            if (args.Length > 0 && args[0] == "/direct")
            {
                test = testDirect;
                System.Console.WriteLine("Using direct MPI interface.");
            }
            else if (args.Length > 0 && args[0] == "/user")
            {
                test = testPrimitiveAndMethod;
                Operation<double>.UseGeneratedUserOps = true;
                System.Console.WriteLine("Using primitive type (double) with user-defined sum and run-time code generation");
            }
            else if (args.Length > 0 && args[0] == "/marshaluser")
            {
                test = testPrimitiveAndMethod;
                Operation<double>.UseGeneratedUserOps = false;
                System.Console.WriteLine("Using primitive type (double) with user-defined sum and marshalling");
            }
            else if (args.Length > 0 && args[0] == "/valuetype")
            {
                test = testValueType;
                System.Console.WriteLine("Using value types with user-defined sum");
            }
            else if (args.Length > 0 && args[0] == "/reftype")
            {
                test = testReferenceType;
                System.Console.WriteLine("Using reference types with user-defined sum");
            }
            else
                System.Console.WriteLine("Using MPI.NET interface.");

            comm = MPI.Communicator.world;
            self = comm.Rank;
            System.Console.WriteLine(comm.Rank + ": " + MPI.Environment.ProcessorName);

            bwstats = new Stats[nSamp];

            testLatency();
            testSyncTime();
            comm.Broadcast(ref latency, 0);

            if (self == 0)
            {
                System.Console.WriteLine("Latency: {0:F9}", latency);
                System.Console.WriteLine("Sync Time: {0:F9}", synctime);
                System.Console.WriteLine("Now starting main loop");
            }

            int n, nq;
            int inc = 1, len;
            int start = 0, end = 1024 * 1024 * 1024;
            int bufflen = start;
            double tlast = latency;

            for (n = nq = 0, len = start; tlast < stopTime && len <= end; len += inc, nq++)
            {
                if (nq > 2 && (nq % 2 != 0)) inc *= 2;
                int ipert, pert;
                for (ipert = 0, pert = (inc > PERT + 1) ? -PERT : 0;
                     pert <= PERT;
                     ipert++, n++, pert += (inc > PERT + 1) ? PERT : PERT + 1)
                {
                    int nRepeat = bufflen == 0 ?
                                  latencyReps :
                                  (int)Math.Max((RUNTM / ((double)bufflen / (bufflen - inc + 1.0) * tlast)),
                                                TRIALS);
                    comm.Broadcast(ref nRepeat, 0);

                    bufflen = len + pert;
                    if (self == 0)
                        System.Console.Write("{0,3:D}: {1,9:D} doubles {2,7:D} times ---> ", n, bufflen, nRepeat);
                    GC.Collect();
                    test(bufflen, n, nRepeat, ref tlast);
                    if (self == 0)
                        System.Console.WriteLine("{0,9:F2} Mbps in {1:F9} sec", bwstats[n].bps, tlast);
                }
            }
        }
    }

    private static double when()
    {
        return MPI.Environment.Time;
    }

    private static void testSyncTime()
    {
        double t0;
        int i;

        t0 = when();
        t0 = when();
        t0 = when();
        t0 = when();
        t0 = when();
        t0 = when();
        for (i = 0; i < latencyReps; i++)
            Communicator.world.Barrier();
        synctime = (when() - t0) / (double)latencyReps;
    }

    private static void simpleAllreduce()
    {
        comm.Allreduce(comm.Rank, Operation<int>.Add);
    }

    private static void testLatency()
    {
        double t0;

        latencyReps = determineLatencyReps();
        if (latencyReps < 1024 && self == 0)
            System.Console.WriteLine("Using {0} reps to determine latency", latencyReps);
        Communicator.world.Barrier();
        t0 = when();
        t0 = when();
        t0 = when();
        t0 = when();
        for (int i = 0; i < latencyReps; i++)
        {
            simpleAllreduce();
        }
        latency = (when() - t0) / (2.0 * latencyReps);
    }

    private static int determineLatencyReps()
    {
        double t0, duration = 0;
	    int reps = 1, prev_reps = 0;
	    int i;

        simpleAllreduce();
        simpleAllreduce();
        simpleAllreduce();

        t0 = when();
        t0 = when();
        t0 = when();
        while ((duration < 1) || (duration < 3 && reps < 1000))
        {
            t0 = when();
            for (i = 0; i < reps - prev_reps; i++)
            {
                simpleAllreduce();
            }
            duration += when() - t0;
            prev_reps = reps;
            reps = reps * 2;
            comm.Broadcast(ref duration, 0);
        }

        return reps;
    }

    /// <summary>
    /// Run the test with a primitive type (double) and the direct, low-level interface.
    /// </summary>
    private static void testDirect(int bufflen, int n, int nRepeat, ref double tlast)
    {
        double[] sendBuffer = new double[bufflen]; // Align the data?  Some day.  Maybe.
        double[] recvBuffer = new double[bufflen];

        bwstats[n].t = 1e99;
        double t1 = 0, t2 = 0;

        for (int i = 0; i < TRIALS; i++)
        {
            Communicator.world.Barrier();
            double t0 = when();

            // Use the unsafe, direct interface to MPI via P/Invoke
            unsafe
            {
                fixed (double* sendPtr = sendBuffer, recvPtr = recvBuffer)
                {
                    for (int j = 0; j < nRepeat; j++)
                    {
                        Unsafe.MPI_Allreduce(new IntPtr(sendPtr), new IntPtr(recvPtr), bufflen, Unsafe.MPI_DOUBLE, Unsafe.MPI_SUM, Unsafe.MPI_COMM_WORLD);
                    }
                }
            }
            double t = (when() - t0) / (2.0 * nRepeat);
            t2 += t * t;
            t1 += t;
            bwstats[n].t = Math.Min(bwstats[n].t, t);
            bwstats[n].variance = t2 / TRIALS - t1 / TRIALS * t1 / TRIALS;
            tlast = bwstats[n].t;
            bwstats[n].bits = bufflen * sizeof(double) * 8;
            bwstats[n].bps = bwstats[n].bits / (bwstats[n].t * 1024 * 1024);
            bwstats[n].repeat = nRepeat;
        }
    }

    /// <summary>
    /// Run the test with a primitive type (double) and the predefined "add" operation.
    /// </summary>
    private static void testPrimitiveAndPredefined(int bufflen, int n, int nRepeat, ref double tlast)
    {
        double[] sendBuffer = new double[bufflen]; // Align the data?  Some day.  Maybe.
        double[] recvBuffer = new double[bufflen];

        bwstats[n].t = 1e99;
        double t1 = 0, t2 = 0;

        for (int i = 0; i < TRIALS; i++)
        {
            Communicator.world.Barrier();
            double t0 = when();
            for (int j = 0; j < nRepeat; j++)
            {
                comm.Allreduce(sendBuffer, Operation<double>.Add, ref recvBuffer);
            }
            double t = (when() - t0) / (2.0 * nRepeat);
            t2 += t * t;
            t1 += t;
            bwstats[n].t = Math.Min(bwstats[n].t, t);
            bwstats[n].variance = t2 / TRIALS - t1 / TRIALS * t1 / TRIALS;
            tlast = bwstats[n].t;
            bwstats[n].bits = bufflen * sizeof(double) * 8;
            bwstats[n].bps = bwstats[n].bits / (bwstats[n].t * 1024 * 1024);
            bwstats[n].repeat = nRepeat;
        }
    }

    private static double sum(double x, double y) { return x + y; }

    /// <summary>
    /// Run the test with a primitive type (double) and a static "sum" mpiDelegateMethod.
    /// </summary>
    private static void testPrimitiveAndMethod(int bufflen, int n, int nRepeat, ref double tlast)
    {
        double[] sendBuffer = new double[bufflen]; // Align the data?  Some day.  Maybe.
        double[] recvBuffer = new double[bufflen];

        bwstats[n].t = 1e99;
        double t1 = 0, t2 = 0;

        for (int i = 0; i < TRIALS; i++)
        {
            Communicator.world.Barrier();
            double t0 = when();
            for (int j = 0; j < nRepeat; j++)
            {
                comm.Allreduce(sendBuffer, sum, ref recvBuffer);
            }
            double t = (when() - t0) / (2.0 * nRepeat);
            t2 += t * t;
            t1 += t;
            bwstats[n].t = Math.Min(bwstats[n].t, t);
            bwstats[n].variance = t2 / TRIALS - t1 / TRIALS * t1 / TRIALS;
            tlast = bwstats[n].t;
            bwstats[n].bits = bufflen * sizeof(double) * 8;
            bwstats[n].bps = bwstats[n].bits / (bwstats[n].t * 1024 * 1024);
            bwstats[n].repeat = nRepeat;
        }
    }

    /// <summary>
    /// Run the test with a value type (DoubleStruct) and the MPI.NET "add" operation.
    /// </summary>
    private static void testValueType(int bufflen, int n, int nRepeat, ref double tlast)
    {
        DoubleStruct[] sendBuffer = new DoubleStruct[bufflen]; // Align the data?  Some day.  Maybe.
        DoubleStruct[] recvBuffer = new DoubleStruct[bufflen];

        bwstats[n].t = 1e99;
        double t1 = 0, t2 = 0;

        for (int i = 0; i < TRIALS; i++)
        {
            Communicator.world.Barrier();
            double t0 = when();
            for (int j = 0; j < nRepeat; j++)
            {
                comm.Allreduce(sendBuffer, Operation<DoubleStruct>.Add, ref recvBuffer);
            }
            double t = (when() - t0) / (2.0 * nRepeat);
            t2 += t * t;
            t1 += t;
            bwstats[n].t = Math.Min(bwstats[n].t, t);
            bwstats[n].variance = t2 / TRIALS - t1 / TRIALS * t1 / TRIALS;
            tlast = bwstats[n].t;
            bwstats[n].bits = bufflen * sizeof(double) * 8;
            bwstats[n].bps = bwstats[n].bits / (bwstats[n].t * 1024 * 1024);
            bwstats[n].repeat = nRepeat;
        }
    }

    /// <summary>
    /// Run the test with a reference type (DoubleClass) and the MPI.NET "add" operation.
    /// </summary>
    private static void testReferenceType(int bufflen, int n, int nRepeat, ref double tlast)
    {
        DoubleClass[] sendBuffer = new DoubleClass[bufflen]; // Align the data?  Some day.  Maybe.
        for (int i = 0; i < bufflen; ++i)
            sendBuffer[i] = new DoubleClass((double)i);
        DoubleClass[] recvBuffer = new DoubleClass[bufflen];

        bwstats[n].t = 1e99;
        double t1 = 0, t2 = 0;

        for (int i = 0; i < TRIALS; i++)
        {
            Communicator.world.Barrier();
            double t0 = when();
            for (int j = 0; j < nRepeat; j++)
            {
                comm.Allreduce(sendBuffer, Operation<DoubleClass>.Add, ref recvBuffer);
            }
            double t = (when() - t0) / (2.0 * nRepeat);
            t2 += t * t;
            t1 += t;
            bwstats[n].t = Math.Min(bwstats[n].t, t);
            bwstats[n].variance = t2 / TRIALS - t1 / TRIALS * t1 / TRIALS;
            tlast = bwstats[n].t;
            bwstats[n].bits = bufflen * sizeof(double) * 8;
            bwstats[n].bps = bwstats[n].bits / (bwstats[n].t * 1024 * 1024);
            bwstats[n].repeat = nRepeat;
        }
    }
}
