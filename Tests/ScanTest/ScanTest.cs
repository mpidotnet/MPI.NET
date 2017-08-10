/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises Intracommunicator.Scan.
 */
using System;
using MPI;
using System.Diagnostics;
using MPI.TestCommons;

public struct Point
{
    public Point(int x, int y)
    {
        this.x = x;
        this.y = y;
    }

    public static Point Plus(Point p1, Point p2)
    {
        return new Point(p1.x + p2.x, p1.y + p2.y);
    }

    public int x;
    public int y;
}

class ScanTest
{
    public static int addInts(int x, int y) { return x + y; }

    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            Intracommunicator world = Communicator.world;

            world.Barrier();

            // Test addition of integers
            int partial_sum = world.Scan(world.Rank, addInts);
            int expected = (world.Rank + 1) * world.Rank / 2;
            MPIDebug.Assert(partial_sum == expected);

            if (world.Rank == world.Size - 1)
                System.Console.WriteLine("Sum of ranks = " + partial_sum);

            // Test addition of integer points
            Point point_sum = world.Scan(new Point(world.Rank, 1), Point.Plus);
            MPIDebug.Assert(point_sum.x == partial_sum && point_sum.y == world.Rank + 1);

            if (world.Rank == world.Size - 1)
                System.Console.WriteLine("Sum of points = (" + point_sum.x + ", " + point_sum.y + ")");

            // Test addition of integer arrays
            if (world.Rank == world.Size - 1)
                System.Console.Write("Testing scan of integer arrays...");
            int[] arraySum = world.Scan(new int[] { world.Rank, 1 }, Operation<int>.Add);
            MPIDebug.Assert(arraySum[0] == partial_sum && arraySum[1] == world.Rank + 1);
            if (world.Rank == world.Size - 1)
                System.Console.WriteLine(" done.");

            // Test concatenation of strings
            if (world.Rank == world.Size - 1)
                System.Console.Write("Testing scan of strings...");
            string str = world.Scan(world.Rank.ToString(), Operation<string>.Add);
            string expectedStr = "";
            for (int p = 0; p <= world.Rank; ++p)
            {
                expectedStr += p.ToString();
            }
            MPIDebug.Assert(expectedStr == str);

            if (world.Rank == world.Size - 1)
                System.Console.WriteLine(" done.");

            // Test concatenation of string arrays
            if (world.Rank == world.Size - 1)
                System.Console.Write("Testing scan of string arrays...");
            string[] strArray = world.Scan(new string[] { world.Rank.ToString(), "World" }, Operation<string>.Add);
            string[] expectedStrs = new string[2] { "", "" };
            for (int p = 0; p <= world.Rank; ++p)
            {
                expectedStrs[0] += p.ToString();
                expectedStrs[1] += "World";
            }
            MPIDebug.Assert(expectedStrs[0] == strArray[0]);
            MPIDebug.Assert(expectedStrs[1] == strArray[1]);

            if (world.Rank == world.Size - 1)
                System.Console.WriteLine(" done.");
        }
    }
}

