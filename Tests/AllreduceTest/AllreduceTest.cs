/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises Communicator.Allreduce.
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

    public static Point operator +(Point p1, Point p2)
    {
        return Plus(p1, p2);
    }

    public static bool operator <(Point p1, Point p2)
    {
        return p1.x < p2.x || (p1.x == p2.x && p1.y < p2.y);
    }

    public static bool operator >(Point p1, Point p2)
    {
        return p2 < p1;
    }

    public int x;
    public int y;
}

class AllreduceTest
{
    public static int addInts(int x, int y) { return x + y; }
    public static string concat(string x, string y) { return x + y; }

    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        using (MPI.Environment env = new MPI.Environment(ref args))
        {
            Intracommunicator world = Communicator.world;

            // Test addition of integers
            int sum = world.Allreduce(world.Rank, addInts);
            int expected = world.Size * (world.Size - 1) / 2;
            MPIDebug.Assert(sum == expected);
            if (world.Rank == 0)
                System.Console.WriteLine("Sum of ranks = " + sum);

            // Test addition of integers through the Operations class
            MPIDebug.Assert(world.Allreduce(world.Rank, Operation<int>.Add) == expected);

            // Test addition of integer points
            Point pointSum = world.Allreduce(new Point(world.Rank, world.Size - world.Rank), Point.Plus);
            MPIDebug.Assert(pointSum.x == sum && pointSum.y == (world.Size + 1) * world.Size / 2);
            if (world.Rank == 0)
                System.Console.WriteLine("Sum of points = (" + pointSum.x + ", " + pointSum.y + ")");

            // Compute the minimum rank
            int minRank = world.Allreduce(world.Rank, Operation<int>.Min);
            MPIDebug.Assert(minRank == 0);
            if (world.Rank == 0)
                System.Console.WriteLine("Minimum of ranks = " + minRank);

            // Compute the minimum point
            Point minPoint = world.Allreduce(new Point(world.Rank, world.Size - world.Rank), Operation<Point>.Min);
            MPIDebug.Assert(minPoint.x == 0 && minPoint.y == world.Size);
            if (world.Rank == 0)
                System.Console.WriteLine("Minimum point = (" + minPoint.x + ", " + minPoint.y + ")");

            // Compute the maximum rank
            int maxRank = world.Allreduce(world.Rank, Operation<int>.Max);
            MPIDebug.Assert(maxRank == world.Size - 1);
            if (world.Rank == 0)
                System.Console.WriteLine("Maximum of ranks = " + maxRank);

            // Compute the maximum point
            Point maxPoint = world.Allreduce(new Point(world.Rank, world.Size - world.Rank), Operation<Point>.Max);
            MPIDebug.Assert(maxPoint.x == world.Size - 1 && maxPoint.y == 1);
            if (world.Rank == 0)
                System.Console.WriteLine("Maximum point = (" + maxPoint.x + ", " + maxPoint.y + ")");

            // Test addition of integer points via the Operations class
            Point pointSum2 = world.Allreduce(new Point(world.Rank, world.Size - world.Rank), Operation<Point>.Add);
            MPIDebug.Assert(pointSum2.x == sum && pointSum2.y == (world.Size + 1) * world.Size / 2);

            // Test concatenation of strings
            string strcat = world.Allreduce(world.Rank.ToString(), concat);
            string expectedStr = "";
            for (int i = 0; i < world.Size; ++i)
                expectedStr += i;
            MPIDebug.Assert(expectedStr == strcat);
            if (world.Rank == 0)
                System.Console.WriteLine("Concatenation of rank strings = " + strcat);

            MPIDebug.Assert(world.Allreduce(world.Rank.ToString(), Operation<string>.Add) == expectedStr);

            // Test addition of integer arrays
            if (world.Rank == 0)
                System.Console.Write("Testing reduction of integer arrays...");
            int[] arraySum = null;
            world.Allreduce(new int[] { world.Rank, world.Size - world.Rank }, Operation<int>.Add, ref arraySum);
            MPIDebug.Assert(arraySum[0] == sum && arraySum[1] == (world.Size + 1) * world.Size / 2);
            if (world.Rank == 0)
                System.Console.WriteLine(" done.");

            // Test concatenation of string arrays
            if (world.Rank == 0)
                System.Console.Write("Testing reduction of string arrays...");
            string[] strArray = null;
            world.Allreduce(new string[] { world.Rank.ToString(), "World" }, Operation<string>.Add, ref strArray);

            string[] expectedStrs = new string[2] { "", "" };
            for (int p = 0; p < world.Size; ++p)
            {
                expectedStrs[0] += p.ToString();
                expectedStrs[1] += "World";
            }
            MPIDebug.Assert(expectedStrs[0] == strArray[0]);
            MPIDebug.Assert(expectedStrs[1] == strArray[1]);

            if (world.Rank == 0)
                System.Console.WriteLine(" done.");
        }
    }
}
