/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 */
using System;
using System.Collections.Generic;
using System.Reflection;
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

    /// <summary>
    /// Provides a mapping from .NET types to their corresponding MPI datatypes.
    /// This class should only be used by experts in both MPI's low-level (C) 
    /// interfaces and the interaction between managed and unmanaged code in .NET.
    /// </summary>
    public class DatatypeCache
    {
        /// <summary>
        /// Placeholder type that is used to indicate that data being sent by one of the
        /// low-level MPI routines is packed by MPI.
        /// </summary>
        /// <remarks>
        /// Advanced users can explicitly pack data to be transmitted via MPI with the 
        /// <see cref="Unsafe.MPI_Pack"/> routine, then unpack that data via
        /// <see cref="Unsafe.MPI_Unpack"/>.
        /// </remarks>
        public struct Packed
        {
        }

        /// <summary>
        /// Builds a new MPI datatype by using reflection on the given type.
        /// </summary>
        private static MPI_Datatype BuildDatatype(Type type)
        {
            // Try to build an MPI datatype using reflection, and return that value.
            FieldInfo[] fields = type.GetFields(BindingFlags.Public|BindingFlags.NonPublic|BindingFlags.Instance);
            int[] blocklens = new int[fields.Length];
            MPI_Aint[] indices = new MPI_Aint[fields.Length];
            MPI_Datatype[] types = new MPI_Datatype[fields.Length];
            int actualLength = 0;

            // Walk the fields of the data structure, computing offsets and types
            // for the MPI datatype
            foreach(FieldInfo field in fields)
            {
                // Only include non-static, serialized fields
                if (!field.IsStatic && !field.IsNotSerialized)
                {
                    Type fieldType = field.FieldType; // The type stored in the field
                    int fieldLength = 1; // Number of elements in this field. > 1 only for "fixed" fields

                    // Handle "fixed" arrays in structures, such as:
                    //
                    //   struct Dimensions
                    //   {
                    //     public unsafe fixed float values[11];
                    //   }
                    //
                    // .NET 2.0 uses a synthesized type whose name contains "__FixedBuffer" for this
                    // field. The type itself has size = sizeof(float)*11, and contains a field 
                    // named "FixedElementType", whose type is "float".
                    if (fieldType.Name.Contains("__FixedBuffer"))
                    {
                        // Dig out the inner field
                        FieldInfo innerField = fieldType.GetFields()[0];

                        // Determine the number of elements
                        fieldLength = Marshal.SizeOf(fieldType) / Marshal.SizeOf(innerField.FieldType);

                        // Pull out the type of the fixed array
                        fieldType = innerField.FieldType;
                    }

                    // Find the field's MPI datatype
                    MPI_Datatype fieldDatatype = GetDatatype(fieldType);
                    if (fieldDatatype == Unsafe.MPI_DATATYPE_NULL)
                    {
                        // There is no MPI datatype for this field, so we cannot build a
                        // datatype for this struct. Note this failure and return.
                        return Unsafe.MPI_DATATYPE_NULL;
                    }

                    // Compute the offset of this field
                    int fieldOffset = (int)Marshal.OffsetOf(type, field.Name);

                    // Record this field
                    blocklens[actualLength] = fieldLength;
                    indices[actualLength] = new IntPtr(fieldOffset);
                    types[actualLength] = fieldDatatype;
                    ++actualLength;
                }
            }

            // Create the MPI datatype
            MPI_Datatype datatype;
            unsafe
            {
                // Build the MPI datatype as a "structure"
                int errorCode = Unsafe.MPI_Type_struct(actualLength, blocklens, indices, types, out datatype);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);

                // Commit the newly-constructed MPI datatype
                errorCode = Unsafe.MPI_Type_commit(ref datatype);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            return datatype;
        }

        /// <summary>
        /// Get the MPI datatype associated with the given type. 
        /// </summary>
        /// <param name="type">
        ///   The type for which we want to build or find an MPI datatype.
        /// </param>
        /// <returns>
        ///   <see cref="Unsafe.MPI_DATATYPE_NULL"/> if there is no corresponding MPI 
        ///   datatype for this type. Otherwise, returns the MPI datatype that can be
        ///   used to transfer objects of this type without serialization.
        /// </returns>
        public static MPI_Datatype GetDatatype(Type type)
        {
            if (type == typeof(bool))
            {
                return Unsafe.MPI_DATATYPE_NULL;
            }
            else if (type.IsValueType)
            {
                // We can only build MPI datatypes for value types
                MPI_Datatype datatype;
                if (cache.TryGetValue(type, out datatype))
                {
                    // We found the type in the cache. It must have been built in a context
                    // were we recovered the type through reflection. Just return that type.
                    return datatype;
                }
                else
                {
                    // Try to build the MPI datatype using reflection
                    datatype = BuildDatatype(type);

                    // Cache the MPI datatype and we're done
                    cache.Add(type, datatype);
                    return datatype;
                }
            }
            else
            {
                // If it's not a value type, we don't have 
                return Unsafe.MPI_DATATYPE_NULL;
            }
        }

        /// <summary>
        /// Builds a dictionary containing all of the built-in datatypes.
        /// </summary>
        /// <returns></returns>
        private static Dictionary<Type, MPI_Datatype> BuiltinDatatypes()
        {
            Dictionary<Type, MPI_Datatype> builtins = new Dictionary<Type, MPI_Datatype>();

            // Integral types
            builtins.Add(typeof(System.SByte), Unsafe.MPI_SIGNED_CHAR);
            builtins.Add(typeof(System.Byte), Unsafe.MPI_BYTE);
            builtins.Add(typeof(System.Int16), Unsafe.MPI_SHORT);
            builtins.Add(typeof(System.UInt16), Unsafe.MPI_UNSIGNED_SHORT);
            builtins.Add(typeof(System.Int32), Unsafe.MPI_INT);
            builtins.Add(typeof(System.UInt32), Unsafe.MPI_UNSIGNED);
            builtins.Add(typeof(System.Int64), Unsafe.MPI_LONG_LONG);
            builtins.Add(typeof(System.UInt64), Unsafe.MPI_UNSIGNED_LONG_LONG);

            // Floating-point types
            builtins.Add(typeof(System.Single), Unsafe.MPI_FLOAT);
            builtins.Add(typeof(System.Double), Unsafe.MPI_DOUBLE);

            // Character type
            builtins.Add(typeof(System.Char), Unsafe.MPI_WCHAR);

            // Determine whether we can use MPI_LONG and MPI_UNSIGNED_LONG for 
            // System.IntPtr or System.UIntPtr.
            IntPtr mpiLongSize;
            unsafe
            {
                int errorCode = Unsafe.MPI_Type_extent(Unsafe.MPI_LONG, out mpiLongSize);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            if (mpiLongSize.ToInt32() == Marshal.SizeOf(typeof(System.IntPtr)))
            {
                // Since the MPI datatype size for "long" is the same as
                // for IntPtr and UIntPtr, use MPI's long and unsigned long
                builtins.Add(typeof(IntPtr), Unsafe.MPI_LONG);
                builtins.Add(typeof(UIntPtr), Unsafe.MPI_UNSIGNED_LONG);
            }
            else
            {
                // Build a derived datatype for IntPtr.
                MPI_Datatype datatype;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Type_contiguous(Marshal.SizeOf(typeof(System.IntPtr)), Unsafe.MPI_BYTE, out datatype);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                    errorCode = Unsafe.MPI_Type_commit(ref datatype);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                builtins.Add(typeof(System.IntPtr), datatype);

                // Build a derived datatype for UIntPtr.
                unsafe
                {
                    int errorCode = Unsafe.MPI_Type_contiguous(Marshal.SizeOf(typeof(System.UIntPtr)), Unsafe.MPI_BYTE, out datatype);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                    errorCode = Unsafe.MPI_Type_commit(ref datatype);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                      throw Environment.TranslateErrorIntoException(errorCode);
                }
                builtins.Add(typeof(System.UIntPtr), datatype);
            }

            // Build a derived datatype for System.Decimal
            MPI_Datatype decimalDatatype;
            unsafe
            {
                int errorCode = Unsafe.MPI_Type_contiguous(Marshal
                    .SizeOf(typeof(System.Decimal)), Unsafe.MPI_BYTE, out decimalDatatype);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
                errorCode = Unsafe.MPI_Type_commit(ref decimalDatatype);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            builtins.Add(typeof(System.Decimal), decimalDatatype);

            // Packed data
            builtins.Add(typeof(Packed), Unsafe.MPI_PACKED);

            return builtins;
        }

        /// <summary>
        /// Contains a mapping from value types to their MPI datatype equivalents
        /// </summary>
        protected static Dictionary<Type, MPI_Datatype> cache = BuiltinDatatypes();
    }

    class FastDatatypeCache<T> : DatatypeCache
    {
        public static MPI_Datatype datatype = GetDatatype(typeof(T));
    }
}
