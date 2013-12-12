import sys
import System
import clr
clr.AddReference("MPI")
import MPI

args = System.Environment.GetCommandLineArgs()
argsref = clr.Reference[System.Array[str]](args)
env = MPI.Environment(argsref)

comm = MPI.Communicator.world
if (comm.Rank == 0):
    # program for rank 0
    comm.Send[str]("Rosie", 1, 0)

    # receive the final message
    msg = comm.Receive[str].Overloads[(int, int)](MPI.Communicator.anySource, 0)

    print "Rank " + str(comm.Rank) + " received message \"" + msg + "\"."

else: # not rank 0
    # program for all other ranks
    msg = comm.Receive[str].Overloads[(int, int)](comm.Rank - 1, 0)

    print "Rank " + str(comm.Rank) + " received message \"" + msg + "\"."

    comm.Send[str](msg + ", " + str(comm.Rank), (comm.Rank + 1) % comm.Size, 0)

env.Dispose()