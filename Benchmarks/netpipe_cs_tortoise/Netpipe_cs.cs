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
using System.Runtime.InteropServices;

struct Data
{
    public double t;
    public double bps;
    public double variance;
    public int bits;
    public int repeat;
};

struct Hare {
    public byte contents;
};

[Serializable] class Tortoise {
    public byte contents;
};

class Netpipe_cs
{
    static Communicator comm;
    static int self, other;
    static double latency, synctime;
    const int PERT = 3, TRIALS = 3, REPEAT = 1000;
    const double RUNTM = 0.25;
    static double stopTime = 0.1;
    static int nSamp = 250, latencyReps = 1000;
    static Data[] bwdata;
    
    static void Main(string[] args)
    {
        // Whether we should use the unsafe, Direct interface to MPI.
        // When false, use the normal MPI.NET interface.
        bool useDirectInterface = false;

        using (MPI.Environment env = new MPI.Environment(ref args))
        {
            if (args.Length > 0 && args[0] == "/direct")
            {
                useDirectInterface = true;
                System.Console.WriteLine("Using direct MPI interface.");
                System.Console.WriteLine("Bzzt.  Can't do that here.  Goodbye");
                return;
            }
            else
                System.Console.WriteLine("Using MPI.NET interface.");

            comm = MPI.Communicator.world;
            if (comm.Size != 2)
            {
                if (comm.Rank == 0)
                    System.Console.WriteLine("Only two processes allowed.  Rerun with -np 2");
                return;
            }
            else
            {
                self = comm.Rank;
                other = (comm.Rank + 1) % 2;
            }

            System.Console.WriteLine(comm.Rank + ": " + MPI.Environment.ProcessorName);

            bwdata = new Data[nSamp];

            testLatency();
            testSyncTime();
            comm.Broadcast(ref latency, 0);

            if (self == 0)
            {
                System.Console.WriteLine("Latency: {0:F9}", latency);
                System.Console.WriteLine("Sync Time: {0:F9}", synctime);
                System.Console.WriteLine("Now starting main loop");
            }

            int i, j, n, nq;
            int inc = 1, len;
            int start = 0, end = 1024 * 1024 * 1024;
            int bufflen = start, bufalign = 16 * 1024;
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
                    Tortoise[] sendBuffer = new Tortoise[bufflen]; // Align the data?  Some day.  Maybe.
                    Tortoise[] recvBuffer = new Tortoise[bufflen];
                    for (i = 0; i < bufflen; i++) {
                        sendBuffer[i] = new Tortoise();
                        sendBuffer[i].contents = 0;
                    }
                    if (self == 0)
                        System.Console.Write("{0,3:D}: {1,9:D} bytes {2,7:D} times ---> ", n, bufflen, nRepeat);

                    bwdata[n].t = 1e99;
                    double t1 = 0, t2 = 0;
                    
                    for (i = 0; i < TRIALS; i++)
                    {
                        sync();
                        double t0 = when();
                        if (useDirectInterface)
                        {
                        }
                        else
                        {
                            for (j = 0; j < nRepeat; j++)
                            {
                                if (self == 0)
                                {
                                    comm.Send(sendBuffer, other, 142);
                                    comm.Receive(ref recvBuffer, other, 242);
                                }
                                else
                                {
                                    comm.Receive(ref recvBuffer, other, 142);
                                    comm.Send(sendBuffer, other, 242);
                                }
                            }
                        }
                        double t = (when() - t0) / (2.0 * nRepeat);
                        t2 += t*t;
                        t1 += t;
                        bwdata[n].t = Math.Min(bwdata[n].t, t);
                        bwdata[n].variance = t2 / TRIALS - t1 / TRIALS * t1 / TRIALS;
                        tlast = bwdata[n].t;
                        bwdata[n].bits = bufflen * sizeof(byte)*8;
                        bwdata[n].bps = bwdata[n].bits / (bwdata[n].t * 1024 * 1024);
                        bwdata[n].repeat = nRepeat;
                    }
                    if (self == 0)
                        System.Console.WriteLine("{0,9:F2} Mbps in {1:F9} sec", bwdata[n].bps, tlast);
                }
            }
        }
    }

    private static double when()
    {
        return MPI.Environment.Wtime;
    }

    private static void sync()
    {
        byte foo = (byte)99;
        if (self == 0)
        {
            comm.Send(foo, other, 41);
            comm.Receive(out foo, other, 41);
            comm.Send(foo, other, 41);
        }
        else
        {
            comm.Receive(out foo, other, 41);
            comm.Send(foo, other, 41);
            comm.Receive(out foo, other, 41);
        }
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
            sync();
        synctime = (when() - t0) / (double)latencyReps;
    }

    private static void testLatency()
    {
        double t0;
        int i, dummy = 0;

        latencyReps = determineLatencyReps();
        if (latencyReps < 1024 && self == 0)
            System.Console.WriteLine("Using {0} reps to determine latency", latencyReps);
        sync();
        t0 = when();
        t0 = when();
        t0 = when();
        t0 = when();
        for (i = 0; i < latencyReps; i++)
        {
            if (self == 0)
            {
                comm.Send(dummy, other, 44);
                comm.Receive(out dummy, other, 44);
            }
            else
            {
                comm.Receive(out dummy, other, 44);
                comm.Send(dummy, other, 44);
            }
        }
        latency = (when() - t0) / (2.0 * latencyReps);
    }

    private static int determineLatencyReps()
    {
        double t0, duration = 0;
	    int reps = 1, prev_reps = 0;
	    int i;

        sync();
        sync();
        sync();

        t0 = when();
        t0 = when();
        t0 = when();
        while ((duration < 1) || (duration < 3 && reps < 1000))
        {
            t0 = when();
            for (i = 0; i < reps - prev_reps; i++)
            {
                sync();
            }
            duration += when() - t0;
            prev_reps = reps;
            reps = reps * 2;

            if (self == 0)
                comm.Send(duration, other, 40);
            else
                comm.Receive(out duration, other, 40);
        }

        return reps;
    }

}
