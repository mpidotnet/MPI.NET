(* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Ben Martin
 *          Douglas Gregor
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
 *)
#light
#r "/progra~1/MPI.NET/Lib/MPI.dll"

open System
open MPI

let args = Sys.argv
let env = new MPI.Environment(ref args)

let dartsPerProcessor = 10000
if args.Length > 0 then 
    let dartsPerProcessor = Convert.ToInt32(args.[args.Length - 1])
    printf ""
let world = MPI.Communicator.world
let random = Random(5*world.Rank)

let rec recThrow depth =    
    let x = 2.0 * (random.NextDouble() - 0.5)
    let y = 2.0 * (random.NextDouble() - 0.5)
    if depth = 0 then
        if x * x + y * y <= 1.0 then
            1
        else
            0
    else
        if x * x + y * y <= 1.0 then 
            1 + recThrow (depth - 1)
        else
            recThrow (depth - 1)
        
let dartsInCircle = recThrow dartsPerProcessor-1

if world.Rank = 0 then
    let totalDartsInCircle = world.Reduce(dartsInCircle, Operation<int>.Add, 0)
    let pi_approx = 4.0*double(totalDartsInCircle)/(double(world.Size)*double(dartsPerProcessor))
    printf "Pi is approximately %f15\n" pi_approx
else
    let totalDartsInCircle = world.Reduce(dartsInCircle, Operation<int>.Add, 0)
    ()

env.Dispose()
