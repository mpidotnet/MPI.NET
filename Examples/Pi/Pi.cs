/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This example shows how one can use MPI to compute an approximate value 
 * for Pi. The basic idea is very simple: consider 2x2 square circumscribed
 * about a circle of radius 1, centered on the origin. Then, we take a bunch
 * darts at random and throw them at the square. The ratio of darts that 
 * land in the circle to the number of darts thrown is equal to the ratio
 * of the area of the circle to the area of the square. Using this equivalence,
 * we can approximate pi. The more darts we throw, the better our
 * approximation of pi. So, we parallelize this program by having every
 * processor throw darts independently, and then sum up the results at the
 * end to compute pi.
 */
using System;
using MPI;

class Pi
{
    static void Main(string[] args)
    {
        int dartsPerProcessor = 10000;
        using (new MPI.Environment(ref args))
        {
            if (args.Length > 0)
                dartsPerProcessor = Convert.ToInt32(args[0]);
            
            Intracommunicator world = Communicator.world;
            Random random = new Random(5 * world.Rank);
            int dartsInCircle = 0;
            for (int i = 0; i < dartsPerProcessor; ++i)
            {
                double x = (random.NextDouble() - 0.5) * 2;
                double y = (random.NextDouble() - 0.5) * 2;
                if (x * x + y * y <= 1.0)
                    ++dartsInCircle;
            }

            if (world.Rank == 0)
            {
                int totalDartsInCircle = world.Reduce<int>(dartsInCircle, Operation<int>.Add, 0);
                System.Console.WriteLine("Pi is approximately {0:F15}.", 
                    4*(double)totalDartsInCircle/(world.Size*(double)dartsPerProcessor));
            }
            else
            {
                world.Reduce<int>(dartsInCircle, Operation<int>.Add, 0);
            }
        }
    }
}

