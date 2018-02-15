/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This file provides the "Operations" class, which contains common
 * reduction operations such as addition and multiplication for any 
 * type.
 * 
 * This code was heavily influenced by Keith Farmer's
 *   Operator Overloading with Generics
 * at http://www.codeproject.com/csharp/genericoperators.asp
 */
using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;

namespace MPI
{
    // MPI data type definitions
#if MPI_HANDLES_ARE_POINTERS
    using MPI_Aint = IntPtr;
    using MPI_Comm = IntPtr;
    using MPI_Datatype = IntPtr;
    using MPI_Errhandler = IntPtr;
    using MPI_File = IntPtr;
    using MPI_Group = IntPtr;
    using MPI_Info = IntPtr;
    using MPI_Op = IntPtr;
    using MPI_Request = IntPtr;
    using MPI_User_function = IntPtr;
    using MPI_Win = IntPtr;
#else
    using MPI_Aint = IntPtr;
    using MPI_Comm = Int32;
    using MPI_Datatype = Int32;
    using MPI_Errhandler = Int32;
    using MPI_File = IntPtr;
    using MPI_Group = Int32;
    using MPI_Info = Int32;
    using MPI_Op = Int32;
    using MPI_Request = Int32;
    using MPI_User_function = IntPtr;
    using MPI_Win = Int32;
#endif

    unsafe delegate void MPIDelegate(void* invec, void* inoutvec, int* len, MPI_Datatype* datatype);

    /// <summary>
    /// The <c>Operation</c> class provides reduction operations for use with the
    /// reduction collectives.
    /// </summary>
    /// 
    /// <remarks>
    /// The <c>Operation</c> class is used with the reduction collective communication operations, such as
    /// <see cref="Intracommunicator.Allreduce&lt;T&gt;(T, ReductionOperation&lt;T&gt;)"/>. For example, 
    /// the <see cref="Add"/> property is a delegate that adds two values of type
    /// <typeparamref name="T"/>, while <see cref="Min"/> returns the minimum of the
    /// two values. The reduction operations provided by this class should be 
    /// preferred to hand-written reduction operations (particularly for built-in types)
    /// because it enables additional optimizations in the MPI library.
    /// 
    /// <para>The <c>Operation</c> class also has a second role for users that require
    /// access to the low-level MPI interface. Creating an instance of the <c>Operation</c>
    /// class will find or create an appropriate <c>MPI_Op</c> for that reduction
    /// operation. This <c>MPI_Op</c>, accessible through the <see cref="Op"/> property,
    /// can be used with low-level MPI reduction operations directly.</para>
    /// </remarks>
    /// <typeparam name="T">The type of data used in these operations.</typeparam>
    public class Operation<T> : IDisposable
    {
        /// <summary>
        /// Synthesize a new mpiDelegateMethod associated with a given operation.
        /// </summary>
        /// <param name="name">The name of the mpiDelegateMethod to build, e.g., "add".</param>
        /// <param name="opcode">The <c>OpCode</c> used for primitive types.</param>
        /// <param name="methodName">The name of the overloaded mpiDelegateMethod used for class types.</param>
        /// <returns>Returns a new delegate implementing the given operation.</returns>
        private static ReductionOperation<T> BuildMethod(string name, OpCode opcode, string methodName)
        {
            // Build the new mpiDelegateMethod
            DynamicMethod method =
                new DynamicMethod(
                    name + ":" + typeof(T).ToString(),
                    typeof(T),
                    new Type[] { typeof(T), typeof(T) },
                    typeof(Operation<T>));

            ILGenerator generator = method.GetILGenerator();

            // Load the arguments onto the stack
            generator.Emit(OpCodes.Ldarg_0);
            generator.Emit(OpCodes.Ldarg_1);

            if (typeof(T).IsPrimitive)
                // Emit the op-code for primitive types, and we're done.
                generator.Emit(opcode);
            else
            {
                // Find the overloaded operator and create a call to it
                MethodInfo opMethod = typeof(T).GetMethod(methodName, new Type[] { typeof(T), typeof(T) });
                if (opMethod == null) throw new MissingMethodException($"Type {typeof(T)} does not implement method {methodName}");
                generator.EmitCall(OpCodes.Call, opMethod, null);
            }

            // Return the intermediate
            generator.Emit(OpCodes.Ret);

            // Return a delegate to call the mpiDelegateMethod
            return (ReductionOperation<T>)method.CreateDelegate(typeof(ReductionOperation<T>));
        }

        #region Minimum and maximum
        /// <summary>
        /// Cached intermediate of the reduction operation that computes the minimum of 
        /// two values.
        /// </summary>
        private static ReductionOperation<T> min = null;

        /// <summary>
        /// Reduction operation that computes the minimum of two values.
        /// </summary>
        public static ReductionOperation<T> Min
        {
            get
            {
                if (min == null)
                {
                    // Build the new mpiDelegateMethod
                    DynamicMethod method =
                        new DynamicMethod(
                            "min:" + typeof(T).ToString(),
                            typeof(T),
                            new Type[] { typeof(T), typeof(T) },
                            typeof(Operation<T>));

                    ILGenerator generator = method.GetILGenerator();

                    // Load the arguments onto the stack
                    generator.Emit(OpCodes.Ldarg_0);
                    generator.Emit(OpCodes.Ldarg_1);

                    Label ifLess = generator.DefineLabel();

                    if (typeof(T).IsPrimitive)
                        // Compare the two operands with "<" and jump to ifLess if they are equal
                        generator.Emit(OpCodes.Blt_S, ifLess);
                    else
                    {
                        // Find the overloaded "<" operator
                        MethodInfo opMethod = typeof(T).GetMethod("op_LessThan", new Type[] { typeof(T), typeof(T) });
                        if (opMethod == null)
                            throw new ArgumentException("Type " + typeof(T).Name + " does not have a < operator");

                        // Create a call to the "<" operator
                        generator.EmitCall(OpCodes.Call, opMethod, null);

                        // If the intermediate was "1" (true), jump to ifLess
                        generator.Emit(OpCodes.Brtrue_S, ifLess);
                    }

                    // We're in the fall-through case, where the first argument is not less than the second.

                    // Load the second argument
                    generator.Emit(OpCodes.Ldarg_1);

                    // Return the intermediate
                    generator.Emit(OpCodes.Ret);

                    // Label the case where the first argument is less than the second
                    generator.MarkLabel(ifLess);

                    // Load the first argument 
                    generator.Emit(OpCodes.Ldarg_0);

                    // Return the intermediate
                    generator.Emit(OpCodes.Ret);

                    // Create the delegate to call the mpiDelegateMethod
                    min = (ReductionOperation<T>)method.CreateDelegate(typeof(ReductionOperation<T>));
                }
                return min;
            }
        }

        /// <summary>
        /// Cached intermediate of the reduction operation that computes the maximum of 
        /// two values.
        /// </summary>
        private static ReductionOperation<T> max = null;

        /// <summary>
        /// Reduction operation that computes the maximum of two values.
        /// </summary>
        public static ReductionOperation<T> Max
        {
            get
            {
                if (max == null)
                {
                    // Build the new mpiDelegateMethod
                    DynamicMethod method =
                        new DynamicMethod(
                            "max:" + typeof(T).ToString(),
                            typeof(T),
                            new Type[] { typeof(T), typeof(T) },
                            typeof(Operation<T>));

                    ILGenerator generator = method.GetILGenerator();

                    // Load the arguments onto the stack
                    generator.Emit(OpCodes.Ldarg_0);
                    generator.Emit(OpCodes.Ldarg_1);

                    Label ifLess = generator.DefineLabel();

                    if (typeof(T).IsPrimitive)
                        // Compare the two operands with "<" and jump to ifLess if they are equal
                        generator.Emit(OpCodes.Blt_S, ifLess);
                    else
                    {
                        // Find the overloaded "<" operator
                        MethodInfo opMethod = typeof(T).GetMethod("op_LessThan", new Type[] { typeof(T), typeof(T) });
                        if (opMethod == null)
                            throw new ArgumentException("Type " + typeof(T).Name + " does not have a < operator");

                        // Create a call to the "<" operator
                        generator.EmitCall(OpCodes.Call, opMethod, null);

                        // If the intermediate was "1" (true), jump to ifLess
                        generator.Emit(OpCodes.Brtrue_S, ifLess);
                    }

                    // We're in the fall-through case, where the first argument is not less than the second.

                    // Load the first argument
                    generator.Emit(OpCodes.Ldarg_0);

                    // Return the intermediate
                    generator.Emit(OpCodes.Ret);

                    // Label the case where the first argument is less than the second
                    generator.MarkLabel(ifLess);

                    // Load the second argument 
                    generator.Emit(OpCodes.Ldarg_1);

                    // Return the intermediate
                    generator.Emit(OpCodes.Ret);

                    // Create the delegate to call the mpiDelegateMethod
                    max = (ReductionOperation<T>)method.CreateDelegate(typeof(ReductionOperation<T>));
                }
                return max;
            }
        }
        #endregion

        #region Addition and multiplication
        /// <summary>
        /// Cached intermediate of the reduction operation that adds two values.
        /// </summary>
        private static ReductionOperation<T> add = null;

        /// <summary>
        /// Reduction operation that adds two values.
        /// </summary>
        public static ReductionOperation<T> Add
        {
            get
            {
                if (add == null)
                {
                    if (typeof(T) == typeof(string))
                        add = BuildMethod("add", OpCodes.Add, "Concat");
                    else
                        add = BuildMethod("add", OpCodes.Add, "op_Addition");
                }
                return add;
            }
        }

        /// <summary>
        /// Cached intermediate of the reduction operation that multiplies two values.
        /// </summary>
        private static ReductionOperation<T> multiply = null;

        /// <summary>
        /// Reduction operation that multiply two values.
        /// </summary>
        public static ReductionOperation<T> Multiply
        {
            get
            {
                if (multiply == null)
                    multiply = BuildMethod("multiply", OpCodes.Mul, "op_Multiply");
                return multiply;
            }
        }
        #endregion

        #region Logical operations
        /// <summary>
        /// Cached intermediate of the reduction operation that computes the logical
        /// AND of two values.
        /// </summary>
        private static ReductionOperation<T> logicalAnd = null;

        /// <summary>
        /// Reduction operation that computes the logical AND of two values,
        /// including integral types.
        /// </summary>
        public static ReductionOperation<T> LogicalAnd
        {
            get
            {
                if (logicalAnd == null)
                    logicalAnd = BuildMethod("logicalAnd", OpCodes.And, "op_LogicalAnd");
                return logicalAnd;
            }
        }

        /// <summary>
        /// Cached intermediate of the reduction operation that computes the logical
        /// OR of two values.
        /// </summary>
        private static ReductionOperation<T> logicalOr = null;

        /// <summary>
        /// Reduction operation that computes the logical OR of two values,
        /// including integral types.
        /// </summary>
        public static ReductionOperation<T> LogicalOr
        {
            get
            {
                if (logicalOr == null)
                    logicalOr = BuildMethod("logicalOr", OpCodes.Or, "op_LogicalOr");
                return logicalOr;
            }
        }
        #endregion

        #region Bitwise operations
        /// <summary>
        /// Cached intermediate of the reduction operation that computes the bitwise
        /// AND of two values.
        /// </summary>
        private static ReductionOperation<T> bitwiseAnd = null;

        /// <summary>
        /// Reduction operation that computes the bitwise AND of two values.
        /// </summary>
        public static ReductionOperation<T> BitwiseAnd
        {
            get
            {
                if (bitwiseAnd == null)
                    bitwiseAnd = BuildMethod("bitwiseAnd", OpCodes.And, "op_BitwiseAnd");
                return bitwiseAnd;
            }
        }

        /// <summary>
        /// Cached intermediate of the reduction operation that computes the bitwise
        /// OR of two values.
        /// </summary>
        private static ReductionOperation<T> bitwiseOr = null;

        /// <summary>
        /// Reduction operation that computes the bitwise OR of two values.
        /// </summary>
        public static ReductionOperation<T> BitwiseOr
        {
            get
            {
                if (bitwiseOr == null)
                    bitwiseOr = BuildMethod("bitwiseOr", OpCodes.Or, "op_BitwiseOr");
                return bitwiseOr;
            }
        }

        /// <summary>
        /// Cached intermediate of the reduction operation that computes the bitwise
        /// exclusive OR of two values.
        /// </summary>
        private static ReductionOperation<T> exclusiveOr = null;

        /// <summary>
        /// Reduction operation that computes the bitwise exclusive OR of two values.
        /// </summary>
        public static ReductionOperation<T> ExclusiveOr
        {
            get
            {
                if (exclusiveOr == null)
                    exclusiveOr = BuildMethod("exclusiveOr", OpCodes.Xor, "op_ExclusiveOr");
                return exclusiveOr;
            }
        }
        #endregion

        #region Datatype categorization
        /// <summary>
        /// The kind of MPI datatype. MPI classifies the predefined data types into 
        /// several different categories. This classification is used primarily to determine 
        /// which predefined reduction operations are supported by the low-level MPI
        /// implementation.
        /// </summary>
        internal enum DatatypeKind
        {
            /// <summary>
            /// C integer types, such as the predefined integral types in C# and .NET.
            /// </summary>
            CInteger,

            /// <summary>
            /// Fortran integer types. At present, there are no C# or .NET types that 
            /// are classified this way.
            /// </summary>
            FortranInteger,

            /// <summary>
            /// Floating point types, such as the predefined <c>float</c> and <c>double</c> types.
            /// </summary>
            FloatingPoint,

            /// <summary>
            /// The MPI logical type. At present, there are no C# or .NET types that 
            /// are classified this way.
            /// </summary>
            Logical,

            /// <summary>
            /// The MPI complex type. At present, there are no C# or .NET types that 
            /// are classified this way.
            /// </summary>
            Complex,

            /// <summary>
            /// The MPI byte type, which corresponds to the <c>byte</c> type in C#.
            /// </summary>
            Byte,

            /// <summary>
            /// Any type that does not fit into one of the other MPI datatype classifications.
            /// </summary>
            Other
        }

        /// <summary>
        /// The MPI datatype classification of the type <typeparamref name="T"/>.
        /// </summary>
        internal static DatatypeKind Kind = GetDatatypeKind();

        /// <summary>
        ///   Retrieves the kind of datatype for the type <typeparamref name="T"/>. 
        ///   Used only to initialize <see cref="Kind"/>.
        /// </summary>
        private static DatatypeKind GetDatatypeKind()
        {
            if (!typeof(T).IsPrimitive)
                // Non-primitive types don't fit any of the MPI datatype categories
                return DatatypeKind.Other;

            // C Integer types
            if (typeof(T) == typeof(short) || typeof(T) == typeof(ushort)
                || typeof(T) == typeof(int) || typeof(T) == typeof(uint)
                || typeof(T) == typeof(long) || typeof(T) == typeof(ulong))
                return DatatypeKind.CInteger;

            // Floating point types
            if (typeof(T) == typeof(float) || typeof(T) == typeof(double))
                return DatatypeKind.FloatingPoint;

            // TODO: Deal with other kinds of types
            return DatatypeKind.Other;
        }
        #endregion

        /// <summary>
        /// Determine the predefined <c>MPI_Op</c> that is associated with
        /// this reduction operation. If no such <c>MPI_Op</c> exists, 
        /// returns <see cref="Unsafe.MPI_OP_NULL"/>.
        /// </summary>
        public static MPI_Op GetPredefinedOperation(ReductionOperation<T> op)
        {
            // Predefined reduction operations with no representation in C#
            //   MPI_LXOR: There is no equivalent operator in C# or C++
            //   MPI_MAXLOC: We don't permit the data type used in reduction to differ
            //               from the type used in reduction.
            //   MPI_MINLOC: We don't permit the data type used in reduction to differ
            //               from the type used in reduction.

            // Min/max
            if (Kind == DatatypeKind.CInteger
                || Kind == DatatypeKind.FortranInteger
                || Kind == DatatypeKind.FloatingPoint)
            {
                if (op == min) return Unsafe.MPI_MIN;
                if (op == max) return Unsafe.MPI_MAX;
            }

            // Product/sum
            if (Kind == DatatypeKind.CInteger
                || Kind == DatatypeKind.FortranInteger
                || Kind == DatatypeKind.FloatingPoint
                || Kind == DatatypeKind.Complex)
            {
                if (op == add) return Unsafe.MPI_SUM;
                if (op == multiply) return Unsafe.MPI_PROD;
            }

            // Logical and/or/xor
            if (Kind == DatatypeKind.CInteger
                || Kind == DatatypeKind.Logical)
            {
                if (op == logicalAnd) return Unsafe.MPI_LAND;
                if (op == logicalOr) return Unsafe.MPI_LOR;
            }

            // Bitwise and/or/xor
            if (Kind == DatatypeKind.CInteger
                || Kind == DatatypeKind.FortranInteger
                || Kind == DatatypeKind.Byte)
            {
                if (op == bitwiseAnd) return Unsafe.MPI_BAND;
                if (op == bitwiseOr) return Unsafe.MPI_BOR;
                if (op == exclusiveOr) return Unsafe.MPI_BXOR;
            }

            return Unsafe.MPI_OP_NULL;
        }

        /// <summary>
        /// When true, we will use user-defined MPI operations generated on-the-fly for
        /// reduction operations on value types. Otherwise, we will use the more generic,
        /// static MPI operations.
        /// </summary>
        public static bool UseGeneratedUserOps = 
#if BROKEN_IL_EMIT
	  false
#else
	  true
#endif
	  ;

        /// <summary>
        /// The dynamically-generated method that provides a delegate with a
        /// signature compatible with <c>MPI_User_function</c> that calls
        /// the user's <see cref="ReductionOperation&lt;T&gt;"/> repeatedly.
        /// </summary>
        private static DynamicMethod mpiDelegateMethod = null;

        /// <summary>
        /// Creates a new MPI delegate from a reduction operation.
        /// </summary>
        private MPIDelegate MakeMPIDelegate(ReductionOperation<T> op)
        {
            if (mpiDelegateMethod == null)
            {
                unsafe 
                {
                    // Build the new mpiDelegateMethod
                    mpiDelegateMethod =
                        new DynamicMethod("reduce:" + typeof(T).ToString(),
                            typeof(void),
                        //                     op                       invec        inoutvec       count           datatype
                            new Type[] { typeof(ReductionOperation<T>), typeof(void*), typeof(void*), typeof(int*), typeof(MPI_Datatype*) },
                            typeof(ReductionOperation<T>));
                }
                ILGenerator generator = mpiDelegateMethod.GetILGenerator();

                // Local variables:
                /*loopCounter*/generator.DeclareLocal(typeof(int));
                /*count*/generator.DeclareLocal(typeof(int));

                // Labels we'll be using to jump around
                Label loopStart = generator.DefineLabel();
                Label loopDone = generator.DefineLabel();

                // Initialize loopCounter with zero
                generator.Emit(OpCodes.Ldc_I4_0);
                generator.Emit(OpCodes.Stloc_0);

                // Load the count into "count"
                generator.Emit(OpCodes.Ldarg_3);
                generator.Emit(OpCodes.Ldind_I4); // TODO: Can we assume "int" is always 32 bits? It is for Windows 32- and 64-bit
                generator.Emit(OpCodes.Stloc_1);

                // We're at the beginning of the loop
                generator.MarkLabel(loopStart);

                // Test to see whether we're done. If so, jump to loopDone
                generator.Emit(OpCodes.Ldloc_0);
                generator.Emit(OpCodes.Ldloc_1);
                generator.Emit(OpCodes.Clt);
                generator.Emit(OpCodes.Brfalse, loopDone);

                // Load the address of inoutvec[loopCounter] onto the stack, to be used after we invoke the delegate
                generator.Emit(OpCodes.Ldarg_2);

                // Load the delegate onto the stack
                generator.Emit(OpCodes.Ldarg_0);

                // Load the first argument (invec[loopCounter]) to the user delegate
                generator.Emit(OpCodes.Ldarg_1);
                generator.Emit(OpCodes.Ldobj, typeof(T));

                // Load the second argument (inoutvec[loopCounter]) to the user delegate
                generator.Emit(OpCodes.Ldarg_2);
                generator.Emit(OpCodes.Ldobj, typeof(T));

                // Call the delegate
                generator.EmitCall(OpCodes.Callvirt,
                                   typeof(ReductionOperation<T>).GetMethod("Invoke", new Type[] { typeof(T), typeof(T) }),
                                   null);

                // Store the intermediate back into inoutvec
                generator.Emit(OpCodes.Stobj, typeof(T));

                // Increment the loop count
                generator.Emit(OpCodes.Ldloc_0);
                generator.Emit(OpCodes.Ldc_I4_1);
                generator.Emit(OpCodes.Add);
                generator.Emit(OpCodes.Stloc_0);

                // Increment invec by the size of T
                generator.Emit(OpCodes.Ldarg_1);
                generator.Emit(OpCodes.Sizeof, typeof(T));
                generator.Emit(OpCodes.Add);
                generator.Emit(OpCodes.Starg, 1);

                // Increment inoutvec by the size of T
                generator.Emit(OpCodes.Ldarg_2);
                generator.Emit(OpCodes.Sizeof, typeof(T));
                generator.Emit(OpCodes.Add);
                generator.Emit(OpCodes.Starg, 2);

                // Jump to the beginning of the loop
                generator.Emit(OpCodes.Br, loopStart);

                // End of our function
                generator.MarkLabel(loopDone);
                generator.Emit(OpCodes.Ret);
            }

            // Return a delegate to call the mpiDelegateMethod
            return (MPIDelegate)mpiDelegateMethod.CreateDelegate(typeof(MPIDelegate), op);
        }

        /// <summary>
        /// Wraps a <see cref="ReductionOperation&lt;T&gt;"/> to provide an MPI-compatible interface
        /// for use with <see cref="Unsafe.MPI_Op_create"/>.
        /// </summary>
        private class WrapReductionOperation
        {
            /// <summary>
            /// Construct a new reduction operation wrapper.
            /// </summary>
            public WrapReductionOperation(ReductionOperation<T> op)
            {
                this.op = op;
            }

            /// <summary>
            /// Applies a reduction operation to each of the values in <paramref name="invec"/> and 
            /// <paramref name="inoutvec"/>, writing the results back into the corresponding
            /// position in <paramref name="inoutvec"/>.
            /// </summary>
            /// <param name="invec">Incoming values of type <c>T</c>.</param>
            /// <param name="inoutvec">Incoming values of type <c>T</c>. The results of the reduction
            /// operation will be written back to this memory.</param>
            /// <param name="len">
            ///   The length of the <paramref name="invec"/> and <paramref name="inoutvec"/> arrays.
            /// </param>
            /// <param name="datatype">
            ///   The MPI datatype for the data stored in <paramref name="invec"/> and <paramref name="inoutvec"/>.
            ///   This should be the same as the intermediate of <see cref="DatatypeCache.GetDatatype"/> applied to the
            ///   type <c>T</c>.
            /// </param>
            public unsafe void Apply(void* invec, void* inoutvec, int* len, MPI_Datatype* datatype)
            {
                unsafe
                {
                    int size = Marshal.SizeOf(typeof(T));
                    int count = *len;
                    for (int i = 0; i < count; ++i) checked
                    {
                        // Note: we end up having to marshal from untyped memory into values of type T,
                        // compute in terms of 'T', then marshal back into untyped memory. 
                        T x = (T)Marshal.PtrToStructure(new IntPtr((byte*)invec + i * size), typeof(T));
                        T y = (T)Marshal.PtrToStructure(new IntPtr((byte*)inoutvec + i * size), typeof(T));
                        T result = op(x, y);
                        Marshal.StructureToPtr(result, new IntPtr((byte*)inoutvec + i * size), true);
                    }
                }
            }

            public ReductionOperation<T> op;
        }

        /// <summary>
        /// Create a user-defined MPI operation based on the given reduction operation.
        /// </summary>
        /// <param name="op">The reduction operation.</param>
        public Operation(ReductionOperation<T> op)
        {
            // Try to find the predefined MPI operation
            mpiOp = GetPredefinedOperation(op);

            if (mpiOp == Unsafe.MPI_OP_NULL)
            {
                // Since we could not find a predefined operation, wrap up the user's operation 
                // in a delegate that matches the signature of MPI_User_function

                unsafe
                {
                    // Create the MPI_Op from the wrapper delegate
                    if (UseGeneratedUserOps)
                        wrapperDelegate = MakeMPIDelegate(op);
                    else
                    {
                        WrapReductionOperation wrapper = new WrapReductionOperation(op);
                        wrapperDelegate = new MPIDelegate(wrapper.Apply);
                    }
                    int errorCode = Unsafe.MPI_Op_create(Marshal.GetFunctionPointerForDelegate(wrapperDelegate), 0, out mpiOp);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        /// Free the MPI operation that this object wraps, but only if it is not a 
        /// predefined MPI operation.
        /// </summary>
        public void Dispose()
        {
            if (wrapperDelegate != null)
            {
                unsafe
                {
                    // Free the MPI_Op
                    int errorCode = Unsafe.MPI_Op_free(ref mpiOp);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
            }
        }

        /// <summary>
        /// The MPI operation that can corresponds to the user's reduction operation,
        /// This operation may be either a predefined MPI reduction operation or a 
        /// user-defined <c>MPI_Op</c> created by the <c>Operation</c> constructor.
        /// </summary>
        public MPI_Op Op
        {
            get
            {
                return mpiOp;
            }
        }

        /// <summary>
        /// The wrapper around the user's reduction operation.  
        /// We keep a reference so that it doesn't get garbage collected until the operation is disposed.
        /// </summary>
        private MPIDelegate wrapperDelegate = null;

        /// <summary>
        /// The actual <c>MPI_Op</c> corresponding to this operation.
        /// </summary>
        private MPI_Op mpiOp;
    }
}
