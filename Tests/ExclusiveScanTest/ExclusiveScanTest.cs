/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises Intracommunicator.ExclusiveScan.
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

class ExscanTest
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
            if (world.Rank == 0)
                System.Console.Write("Testing exclusive scan of strings...");
            int partial_sum = world.ExclusiveScan(world.Rank, addInts);
            int expected = world.Rank * (world.Rank - 1) / 2;
            MPIDebug.Assert(partial_sum == expected);
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

            // Test addition of integer points
            if (world.Rank == 0)
                System.Console.Write("Testing exclusive scan of strings...");
            Point point_sum = world.ExclusiveScan(new Point(world.Rank, 1), Point.Plus);
            MPIDebug.Assert(point_sum.x == partial_sum && point_sum.y == world.Rank);
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

            // Test addition of integer arrays
            if (world.Rank == 0)
                System.Console.Write("Testing exclusive scan of integer arrays...");
            int[] arraySum = world.ExclusiveScan(new int[] { world.Rank, 1 }, Operation<int>.Add);
            MPIDebug.Assert((world.Rank == 0 && arraySum == null)
                         || (world.Rank != 0 && arraySum[0] == partial_sum && arraySum[1] == world.Rank));
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

            // Test concatenation of strings
            if (world.Rank == 0)
                System.Console.Write("Testing exclusive scan of strings...");
            string str = world.ExclusiveScan(world.Rank.ToString(), Operation<string>.Add);
            string expectedStr = null;
            if (world.Rank != 0)
            {
                expectedStr = "";
                for (int p = 0; p < world.Rank; ++p)
                {
                    expectedStr += p.ToString();
                }
            }
            MPIDebug.Assert(expectedStr == str);

            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

            // Test concatenation of string arrays
            if (world.Rank == 0)
                System.Console.Write("Testing exclusive scan of string arrays...");
            string[] strArray = world.ExclusiveScan(new string[] { world.Rank.ToString(), "World" }, Operation<string>.Add);
            string[] expectedStrs = null;
            if (world.Rank != 0)
            {
                expectedStrs = new string[2] { "", "" };
                for (int p = 0; p < world.Rank; ++p)
                {
                    expectedStrs[0] += p.ToString();
                    expectedStrs[1] += "World";
                }
                MPIDebug.Assert(expectedStrs[0] == strArray[0]);
                MPIDebug.Assert(expectedStrs[1] == strArray[1]);
            }
            else
                MPIDebug.Assert(expectedStrs == null);

            if (world.Rank == 0)
                System.Console.WriteLine(" done.");
        }
    }
}