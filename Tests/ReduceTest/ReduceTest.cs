/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises Communicator.Reduce.
 */
using System;
using MPI;
using System.Diagnostics;

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

class ReduceTest
{
    public static int addInts(int x, int y) { return x + y; }

    static void RunTests(int root)
    {
        Intracommunicator world = Communicator.world;

        world.Barrier();

        if (world.Rank == root)
            System.Console.WriteLine("Testing from root " + root);

        // Test addition of integers
        int sum = world.Reduce(world.Rank, addInts, root);
        int expected = world.Size * (world.Size - 1) / 2;
        if (world.Rank == root)
            Debug.Assert(sum == expected);
        else
            Debug.Assert(sum == default(int));

        if (world.Rank == root)
            System.Console.WriteLine("Sum of ranks = " + sum);

        // Test addition of integer points
        if (world.Rank == root)
        {
            Point point_sum = world.Reduce(new Point(world.Rank, world.Size - world.Rank), Point.Plus, root);
            Debug.Assert(point_sum.x == sum && point_sum.y == (world.Size + 1) * world.Size / 2);
            System.Console.WriteLine("Sum of points = (" + point_sum.x + ", " + point_sum.y + ")");
        }
        else
            world.Reduce(new Point(world.Rank, world.Size - world.Rank), Point.Plus, root);

        // Test addition of integer arrays
        if (world.Rank == root)
        {
            System.Console.Write("Testing reduction of integer arrays...");
            int[] arraySum = null;
            world.Reduce(new int[] { world.Rank, world.Size - world.Rank }, Operation<int>.Add, root, ref arraySum);
            Debug.Assert(arraySum[0] == sum && arraySum[1] == (world.Size + 1) * world.Size / 2);
            System.Console.WriteLine(" done.");
        }
        else
        {
            world.Reduce(new int[] { world.Rank, world.Size - world.Rank }, Operation<int>.Add, root);
        }

        // Test concatenation of string arrays
        if (world.Rank == root)
        {
            System.Console.Write("Testing reduction of string arrays...");
            string[] strArray = null;
            world.Reduce(new string[] { world.Rank.ToString(), "World" }, Operation<string>.Add, root, ref strArray);

            string[] expectedStrs = new string[2] { "", "" };
            for (int p = 0; p < world.Size; ++p)
            {
                expectedStrs[0] += p.ToString();
                expectedStrs[1] += "World";
            }
            Debug.Assert(expectedStrs[0] == strArray[0]);
            Debug.Assert(expectedStrs[1] == strArray[1]);

            System.Console.WriteLine(" done.");
        }
        else
        {
            world.Reduce(new string[] { world.Rank.ToString(), "World" }, Operation<string>.Add, root);
        }

        // Test reduction on boolean values
        if (world.Rank == root)
        {
            System.Console.Write("Testing reduction of bools...");
            bool result = world.Reduce(true, Operation<bool>.LogicalAnd, root);
            Debug.Assert(result == true);
            System.Console.WriteLine(" done.");
        }
        else
        {
            world.Reduce(true, Operation<bool>.LogicalAnd, root);
        }

        // Test reduction on boolean arrays
        if (world.Rank == root)
        {
            System.Console.Write("Testing reduction of bool arrays...");
            bool[] boolArray = null;
            world.Reduce(new bool[] { false, world.Rank % 2 != 0, true }, Operation<bool>.LogicalOr, root, ref boolArray);
            Debug.Assert(boolArray[0] == false);
            Debug.Assert(boolArray[1] == (world.Size > 1));
            Debug.Assert(boolArray[2] == true);
            System.Console.WriteLine(" done.");
        }
        else
        {
            world.Reduce(new bool[] { false, world.Rank % 2 != 0, false }, Operation<bool>.LogicalOr, root);
        }
    }

    static void Main(string[] args)
    {
        using (MPI.Environment env = new MPI.Environment(ref args))
        {
            for (int i = 0; i < Communicator.world.Size; ++i)
            {
                RunTests(i);
            }
        }
    }
}
