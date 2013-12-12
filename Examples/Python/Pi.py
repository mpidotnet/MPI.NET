import sys
import System
import clr
clr.AddReference("MPI")
import MPI

args = System.Environment.GetCommandLineArgs()
argsref = clr.Reference[System.Array[str]](args)
env = MPI.Environment(argsref)

comm = MPI.Communicator.world

dartsPerProcessor = 10000

if len(sys.argv) > 1:
    dartsPerProcessor = int(sys.argv[-1])
            
random = System.Random(5 * comm.Rank)
dartsInCircle = 0
for i in range(dartsPerProcessor):
    x = (random.NextDouble() - 0.5) * 2.0
    y = (random.NextDouble() - 0.5) * 2.0
    if x * x + y * y <= 1.0:
        dartsInCircle += 1

if comm.Rank == 0:
    totalDartsInCircle = comm.Reduce[int](dartsInCircle, MPI.Operation[int].Add, 0)
    print "Pi is approximately " + str(4.0*float(totalDartsInCircle)/(float(comm.Size)*float(dartsPerProcessor)))
else:
    comm.Reduce[int](dartsInCircle, MPI.Operation[int].Add, 0)

env.Dispose()
