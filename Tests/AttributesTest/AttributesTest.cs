/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This test exercises MPI.NET's attributes mechanism.
 */
using System;
using System.Diagnostics;
using MPI;
using MPI.TestCommons;

struct Point
{
    public Point(double x, double y, double z)
    {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public double x;
    public double y;
    public double z;

    public static bool operator ==(Point p1, Point p2)
    {
        return p1.x == p2.x && p1.y == p2.y && p1.z == p2.z;
    }

    public static bool operator !=(Point p1, Point p2)
    {
        return !(p1 == p2);
    }

    public override bool Equals(object o)
    {
        if (o is Point)
            return (Point)o == this;
        else
            return false;
    }

    public override int GetHashCode()
    {
        return x.GetHashCode() ^ y.GetHashCode() ^ z.GetHashCode();
    }

    public override string ToString()
    {
        return "(" + x + ", " + y + ", " + z + ")";
    }
}

class StringHolder : ICloneable
{
    public StringHolder(string str)
    {
        this.str = str;
    }

    public string str;

    public object Clone()
    {
        return new StringHolder(str);
    }
}

class AttributesTest
{
    static int Main(string[] args)
    {
        return MPIDebug.Execute(DoTest, args);
    }

    public static void DoTest(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            Intracommunicator parentComm = (Intracommunicator)Communicator.world.Clone();

            // Create a bunch of attributes of different kinds
            Console.Error.WriteLine("Creating attributes...");
            MPI.Attribute nocopyIntAttr = MPI.Attribute.Create<int>(AttributeDuplication.None);
            Console.Error.WriteLine("Creating second attribute...");
            MPI.Attribute shallowIntAttr = MPI.Attribute.Create<int>(AttributeDuplication.Shallow);
            MPI.Attribute deepIntAttr = MPI.Attribute.Create<int>(AttributeDuplication.Deep);
            MPI.Attribute nocopyPointAttr = MPI.Attribute.Create<Point>(AttributeDuplication.None);
            MPI.Attribute shallowPointAttr = MPI.Attribute.Create<Point>(AttributeDuplication.Shallow);
            MPI.Attribute deepPointAttr = MPI.Attribute.Create<Point>(AttributeDuplication.Deep);
            MPI.Attribute nocopyStringAttr = MPI.Attribute.Create<StringHolder>(AttributeDuplication.None);
            MPI.Attribute shallowStringAttr = MPI.Attribute.Create<StringHolder>(AttributeDuplication.Shallow);
            MPI.Attribute deepStringAttr = MPI.Attribute.Create<StringHolder>(AttributeDuplication.Deep);

            // Place initial values for these attributes into the parent communicator
            Console.Error.WriteLine("Initial values...");
            parentComm.Attributes[nocopyIntAttr] = 17;
            parentComm.Attributes[shallowIntAttr] = 25;
            parentComm.Attributes[deepIntAttr] = 42;
            parentComm.Attributes[nocopyPointAttr] = new Point(1.1, 1.2, 1.3);
            parentComm.Attributes[shallowPointAttr] = new Point(2.1, 2.2, 2.3);
            parentComm.Attributes[deepPointAttr] = new Point(3.1, 3.2, 3.3);
            parentComm.Attributes[nocopyStringAttr] = new StringHolder("Hello");
            parentComm.Attributes[shallowStringAttr] = new StringHolder("MPI");
            parentComm.Attributes[deepStringAttr] = new StringHolder("Attributes");

            // Check initial values in the parent communicator
            Console.Error.WriteLine("Checking initial values...");
            MPIDebug.Assert((int)parentComm.Attributes[nocopyIntAttr] == 17);
            MPIDebug.Assert((int)parentComm.Attributes[shallowIntAttr] == 25);
            MPIDebug.Assert((int)parentComm.Attributes[deepIntAttr] == 42);
            MPIDebug.Assert((Point)parentComm.Attributes[nocopyPointAttr] == new Point(1.1, 1.2, 1.3));
            MPIDebug.Assert((Point)parentComm.Attributes[shallowPointAttr] == new Point(2.1, 2.2, 2.3));
            MPIDebug.Assert((Point)parentComm.Attributes[deepPointAttr] == new Point(3.1, 3.2, 3.3));
            MPIDebug.Assert(((StringHolder)parentComm.Attributes[nocopyStringAttr]).str == "Hello");
            MPIDebug.Assert(((StringHolder)parentComm.Attributes[shallowStringAttr]).str == "MPI");
            MPIDebug.Assert(((StringHolder)parentComm.Attributes[deepStringAttr]).str == "Attributes");

            // Duplicate the communicator
            Communicator childComm = (Communicator)parentComm.Clone();

            // Check values in the parent communicator (again)
            MPIDebug.Assert((int)parentComm.Attributes[nocopyIntAttr] == 17);
            MPIDebug.Assert((int)parentComm.Attributes[shallowIntAttr] == 25);
            MPIDebug.Assert((int)parentComm.Attributes[deepIntAttr] == 42);
            MPIDebug.Assert((Point)parentComm.Attributes[nocopyPointAttr] == new Point(1.1, 1.2, 1.3));
            MPIDebug.Assert((Point)parentComm.Attributes[shallowPointAttr] == new Point(2.1, 2.2, 2.3));
            MPIDebug.Assert((Point)parentComm.Attributes[deepPointAttr] == new Point(3.1, 3.2, 3.3));
            MPIDebug.Assert(((StringHolder)parentComm.Attributes[nocopyStringAttr]).str == "Hello");
            MPIDebug.Assert(((StringHolder)parentComm.Attributes[shallowStringAttr]).str == "MPI");
            MPIDebug.Assert(((StringHolder)parentComm.Attributes[deepStringAttr]).str == "Attributes");

            // Check values in the child communicator
            MPIDebug.Assert(childComm.Attributes[nocopyIntAttr] == null);
            MPIDebug.Assert((int)childComm.Attributes[shallowIntAttr] == 25);
            MPIDebug.Assert((int)childComm.Attributes[deepIntAttr] == 42);
            MPIDebug.Assert(childComm.Attributes[nocopyPointAttr] == null);
            MPIDebug.Assert((Point)childComm.Attributes[shallowPointAttr] == new Point(2.1, 2.2, 2.3));
            MPIDebug.Assert((Point)childComm.Attributes[deepPointAttr] == new Point(3.1, 3.2, 3.3));
            MPIDebug.Assert(childComm.Attributes[nocopyStringAttr] == null);
            MPIDebug.Assert(((StringHolder)childComm.Attributes[shallowStringAttr]).str == "MPI");
            MPIDebug.Assert(((StringHolder)childComm.Attributes[deepStringAttr]).str == "Attributes");

            // Check modification of shallow-copy attributes
            parentComm.Attributes[shallowIntAttr] = 99;
            MPIDebug.Assert((int)parentComm.Attributes[shallowIntAttr] == 99);
            MPIDebug.Assert((int)childComm.Attributes[shallowIntAttr] == 25);

            parentComm.Attributes[shallowPointAttr] = new Point(4.1, 4.2, 4.3);
            MPIDebug.Assert((Point)parentComm.Attributes[shallowPointAttr] == new Point(4.1, 4.2, 4.3));
            MPIDebug.Assert((Point)childComm.Attributes[shallowPointAttr] == new Point(4.1, 4.2, 4.3));

            ((StringHolder)parentComm.Attributes[shallowStringAttr]).str = "Cached";
            MPIDebug.Assert(((StringHolder)parentComm.Attributes[shallowStringAttr]).str == "Cached");
            MPIDebug.Assert(((StringHolder)childComm.Attributes[shallowStringAttr]).str == "Cached");

            // Check modification of deep-copy attributes
            parentComm.Attributes[deepIntAttr] = 99;
            MPIDebug.Assert((int)parentComm.Attributes[deepIntAttr] == 99);
            MPIDebug.Assert((int)childComm.Attributes[deepIntAttr] == 42);

            parentComm.Attributes[deepPointAttr] = new Point(4.1, 4.2, 4.3);
            MPIDebug.Assert((Point)parentComm.Attributes[deepPointAttr] == new Point(4.1, 4.2, 4.3));
            MPIDebug.Assert((Point)childComm.Attributes[deepPointAttr] == new Point(3.1, 3.2, 3.3));

            ((StringHolder)parentComm.Attributes[deepStringAttr]).str = "Cached";
            MPIDebug.Assert(((StringHolder)parentComm.Attributes[deepStringAttr]).str == "Cached");
            MPIDebug.Assert(((StringHolder)childComm.Attributes[deepStringAttr]).str == "Attributes");

            // Check attribute deletion
            parentComm.Attributes.Remove(shallowIntAttr);
            MPIDebug.Assert(parentComm.Attributes[shallowIntAttr] == null);
            MPIDebug.Assert((int)childComm.Attributes[shallowIntAttr] == 25);
            parentComm.Attributes.Remove(shallowIntAttr);

            parentComm.Attributes.Remove(shallowPointAttr);
            MPIDebug.Assert(parentComm.Attributes[shallowPointAttr] == null);
            MPIDebug.Assert((Point)childComm.Attributes[shallowPointAttr] == new Point(4.1, 4.2, 4.3));
            parentComm.Attributes.Remove(shallowPointAttr);

            parentComm.Attributes.Remove(shallowStringAttr);
            MPIDebug.Assert(parentComm.Attributes[shallowStringAttr] == null);
            MPIDebug.Assert(((StringHolder)childComm.Attributes[shallowStringAttr]).str == "Cached");
            parentComm.Attributes.Remove(shallowStringAttr);
        }
    }
}
