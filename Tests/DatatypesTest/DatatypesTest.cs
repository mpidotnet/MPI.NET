/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises MPI.NET's ability to construct and use MPI datatypes 
 * for .NET value types using reflection.
 */
using System;
using MPI;
using System.Diagnostics;
using System.Collections.Generic;
using MPI.TestCommons;

unsafe public struct Dimensions
{
    public fixed float values[11];
}

public struct Secretive
{
    public Secretive(int x, int y)
    {
        this.x = x;
        this.y = y;
    }
     
    private int x;
    public int y;

    public static bool operator ==(Secretive s1, Secretive s2)
    {
        return s1.x == s2.x && s1.y == s2.y;
    }

    public static bool operator !=(Secretive s1, Secretive s2)
    {
        return !(s1 == s2);
    }

    public override bool Equals(object other)
    {
        if (other is Secretive)
        {
            return this == (Secretive)other;
        }
        throw new ArgumentException("other object is not Secretive");
    }

    public override int GetHashCode()
    {
        return x ^ y;
    }

    public override string ToString()
    {
        return x.ToString() + ":" + y.ToString();
    }
}

[Serializable]
class AggregateData
{
    public AggregateData(int NumBytes)
    {
        if (NumBytes >= 0)
        {
            Name = "Aggregate data";
            KeyValuePairs = new Dictionary<string, string>();
            KeyValuePairs.Add("Key1", "Value1");
            KeyValuePairs.Add("Key2", "Value2");
            KeyValuePairs.Add("Key3", "Value3");
            BinaryData = new byte[NumBytes];
            for (int i = 0; i < NumBytes; ++i)
                BinaryData[i] = (byte)(i % 256);
        }
    }

    public bool Check(int BytesExpected)
    {
        if (!Name.Equals("Aggregate data"))
            return false;
        if (!KeyValuePairs["Key1"].Equals("Value1"))
            return false;
        if (!KeyValuePairs["Key2"].Equals("Value2"))
            return false;
        if (!KeyValuePairs["Key3"].Equals("Value3"))
            return false;
        if (BinaryData.Length != BytesExpected)
            return false;
        for (int i = 0; i < BinaryData.Length; ++i)
            if (BinaryData[i] != i % 256)
                return false;
        return true;
    }

    private string Name;
    private Dictionary<string, string> KeyValuePairs;
    private byte[] BinaryData;
}

[Serializable]
struct ContainsBool
{
    int int1;
    char char1;
    bool bool1;
    char char2;

    public ContainsBool(int value)
    {
        int1 = value;
        char1 = value.ToString()[0];
        bool1 = value >= 10;
        char2 = bool1 ? value.ToString()[1] : 'X';
    }

    public static bool operator ==(ContainsBool x, ContainsBool y)
    {
        return x.int1 == y.int1 && x.char1 == y.char1
            && x.bool1 == y.bool1 && x.char2 == y.char2;
    }

    public static bool operator !=(ContainsBool x, ContainsBool y)
    {
        return !(x == y);
    }

    public override bool Equals(object other)
    {
        if (other is ContainsBool)
        {
            return this == (ContainsBool)other;
        }
        throw new ArgumentException("other object is not a ContainsBool");
    }

    public override int GetHashCode()
    {
        return int1;
    }

    public override string ToString()
    {
        return int1.ToString() + ':' + char1.ToString() + ':' 
            + bool1.ToString() + ':' + char2.ToString();
    }

}

class DatatypesTest
{
    private struct Hidden
    {
        public Hidden(int x, int y)
        {
            this.x = x;
            this.y = y;
        }

        private int x;
        public int y;

        public static bool operator ==(Hidden s1, Hidden s2)
        {
            return s1.x == s2.x && s1.y == s2.y;
        }

        public static bool operator !=(Hidden s1, Hidden s2)
        {
            return !(s1 == s2);
        }

        public override bool Equals(object other)
        {
            if (other is Hidden)
            {
                return this == (Hidden)other;
            }
            throw new ArgumentException("other object is not Hidden");
        }

        public override int GetHashCode()
        {
            return x ^ y;
        }

        public override string ToString()
        {
            return x.ToString() + ":" + y.ToString();
        }
    }

    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        int dataSize = 10000000;
        //using (MPI.Environment env = new MPI.Environment(ref args))
        {
            if (Communicator.world.Size != 2)
            {
                System.Console.WriteLine("The Datatypes test must be run with two processes.");
                System.Console.WriteLine("Try: mpiexec -np 2 datatypes.exe");
            }
            else if (Communicator.world.Rank == 0)
            {
                // Send an object that contains a "fixed" field
                Dimensions dims;
                unsafe
                {
                    for (int i = 0; i < 11; ++i)
                        dims.values[i] = (float)i;
                }
                Communicator.world.Send(dims, 1, 0);

                // Send an object that contains non-public fields
                Secretive secret = new Secretive(17, 25);
                Communicator.world.Send(secret, 1, 1);

                // Send an object with complex data
                AggregateData aggregate = new AggregateData(dataSize);
                Communicator.world.Send(aggregate, 1, 2);

                // Send an object with a private type
                Hidden hidden = new Hidden(17, 25);
                Communicator.world.Send(hidden, 1, 3);

                // Send a struct that requires serialization.
                ContainsBool containsBool = new ContainsBool(17);
                Communicator.world.Send(containsBool, 1, 4);
            }
            else
            {
                // Receive and check an object that contains a "fixed" field
                Dimensions dims;
                Communicator.world.Receive(0, 0, out dims);
                unsafe
                {
                    for (int i = 0; i < 11; ++i)
                    {
                        System.Console.WriteLine(dims.values[i].ToString() + " ");
                        MPIDebug.Assert(dims.values[i] == (float)i);
                    }
                }

                // Receive and check an object that contains non-public fields
                Secretive secret;
                Communicator.world.Receive(0, 1, out secret);
                System.Console.WriteLine(secret);
                MPIDebug.Assert(secret == new Secretive(17, 25));

                // Receive and check the "complex data"
                AggregateData aggregate = Communicator.world.Receive<AggregateData>(0, 2);
                if (!aggregate.Check(dataSize))
                {
                    System.Console.Error.WriteLine("Error: complex data not properly transmitted");
                    MPI.Environment.Abort(1);
                }

                // Receive and check an object with a private type
                Hidden hidden;
                Communicator.world.Receive(0, 3, out hidden);
                System.Console.WriteLine(hidden);
                MPIDebug.Assert(hidden == new Hidden(17, 25));

                // Receive and check a struct that requires serialization
                ContainsBool containsBool;
                Communicator.world.Receive(0, 4, out containsBool);
                System.Console.WriteLine(containsBool);
                MPIDebug.Assert(containsBool == new ContainsBool(17));
            }

        }
    }
}
