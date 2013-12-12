/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This program tests the Broadcast operation on MPI.NET.
 */
using System;
using System.Text;
using MPI;
using System.Diagnostics;

class BroadcastTest
{
    private static void Test(int root)
    {
        Intracommunicator world = Communicator.world;

        // Broadcast an integer
        int intValue = default(int);
        if (world.Rank == root)
        {
            intValue = 17;
            System.Console.Write("Broadcasting integer from root " + root + "...");
        }
        world.Broadcast(ref intValue, root);
        Debug.Assert(intValue == 17);
        if (world.Rank == root)
            System.Console.WriteLine(" done.");

        // Broadcast a string
        string strValue = "";
        if (world.Rank == root)
        {
            strValue = "Hello, World!";
            System.Console.Write("Broadcasting string from root " + root + "...");
        }
        world.Broadcast(ref strValue, root);
        Debug.Assert(strValue == "Hello, World!");
        if (world.Rank == root)
            System.Console.WriteLine(" done.");

        // Broadcast an array of integers
        int[] intArray = new int[7];
        if (world.Rank == root)
        {
            intArray = new int[]{ 1, 1, 2, 3, 5, 8, 13 };
            System.Console.Write("Broadcasting integer array from root " + root + "...");
        }
        world.Broadcast(ref intArray, root);
        Debug.Assert(intArray[3] == 3);
        if (world.Rank == root)
            System.Console.WriteLine(" done.");

        // Broadcast an array of strings
        string[] strArray = new string[2];
        if (world.Rank == root)
        {
            strArray = new string[] { "Hello", "World" };
            System.Console.Write("Broadcasting string array from root " + root + "...");
        }
        world.Broadcast(ref strArray, root);
        Debug.Assert(strArray[0] == "Hello" && strArray[1] == "World");
        if (world.Rank == root)
            System.Console.WriteLine(" done.");

    }

    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            for (int i = 0; i < Communicator.world.Size; ++i)
                Test(i);
        }
    }
}
