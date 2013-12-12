import sys
import System
import clr
clr.AddReference("MPI")
import MPI

args = System.Environment.GetCommandLineArgs()
argsref = clr.Reference[System.Array[str]](args)
env = MPI.Environment(argsref)

destHostname = ""
comm = MPI.Communicator.world
if comm.Rank == 0:
    print "Rank 0 is alive and running on " + MPI.Environment.ProcessorName
    for dest  in range(1, comm.Size):
        print "Pinging process with rank " + str(dest) + "..."
        comm.Send[str]("Ping!", dest, 0)
        destHostname = comm.Receive[str].Overloads[(int, int)](dest, 1)
        print " Pong!"
        print "  Rank " + str(dest) + " is alive and running on " + str(destHostname)
else:
    destHostname = comm.Receive[str].Overloads[(int, int)](0, 0)
    comm.Send[str](str(MPI.Environment.ProcessorName), 0, 1)

env.Dispose()