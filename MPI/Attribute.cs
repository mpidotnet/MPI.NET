/* Copyright (C) 2007,2008  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 */
using System;
using System.Runtime.InteropServices;
using System.Collections.Generic;

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
    /// Enumeration describing how a given attribute should be copied
    /// (or not) when the communicator is cloned (duplicated).
    /// </summary>
    public enum AttributeDuplication
    {
        /// <summary>
        /// When the communicator is duplicated, this value is not copied.
        /// 
        /// For value types, small value types (less than the size of 
        /// a native pointer) will be kept in the low-level MPI communicator.
        /// With larger value types, the low-level MPI communicator's 
        /// attribute will store a pointer to the value, which will be
        /// automatically allocated and deallocated as appropriate. This
        /// behavior can be used to interoperate with C MPI programs that
        /// store attributes on the communicator.
        /// </summary>
        None,

        /// <summary>
        /// When the communicator is duplicated, the new communicator will
        /// hold a "shallow" copy of the underlying data, so that both
        /// communicators reference the same value. 
        /// 
        /// For value types smaller than a native pointer, the value will
        /// be stored directly in the low-level communicator. This approach 
        /// can be used for interoperability with C MPI programs that store
        /// primitive types or MPI handles directly in the attributes.
        /// 
        /// For object types and "large" value types, the value will be 
        /// stored on the (garbage-collected) heap. Copying entails
        /// creating new references to the object.
        /// </summary>
        Shallow,

        /// <summary>
        /// When the communicator is duplicated, the new communicator will
        /// contain a completely-new, distinct copy of the underlying data.
        /// 
        /// For value types, each low-level MPI communicator will store a 
        /// pointer to the data, which will be deallocated when the
        /// MPI communicator is destroyed. This approach can be used for 
        /// interoperability with C MPI programs that store attributes as 
        /// pointers to primitives or structures.
        /// 
        /// For class types, MPI.NET will use the <c>ICloneable</c> interface
        /// to clone objects for a deep copy.
        /// </summary>
        Deep
    }

    /// <summary>
    /// Attributes are key/value pairs that can be attached to communicators, 
    /// and are generally used to maintain communicator-specific information 
    /// across different libraries or different languages.
    /// </summary>
    /// 
    /// <remarks>Each instance
    /// of the <c>Attribute</c> class is the "key" for a different kind of
    /// data, and can be used to index the <see cref="AttributeSet"/> associated
    /// with a communicator to query or modify the corresponding value. Each
    /// attribute is created with the type of data its value will store (e.g., 
    /// an integer, a string, an object) and a duplication policy, which 
    /// describes how a value of that type is propagated when communicators
    /// are cloned.
    /// 
    /// <para>
    ///   Attributes can be used for interoperability with native code. Any
    ///   attribute whose type is a value type (e.g., primitive type or structure)
    ///   that has either deep-copy or no-copy semantics will be stored on the
    ///   low-level MPI communicator. These attributes can then be accessed by
    ///   native code directly via the low-level MPI interface.
    /// </para>
    /// </remarks>
    public class Attribute : IDisposable
    {
        /// <summary>
        /// Creates a new attribute. Users need to use the factory function <see cref="Create"/>.
        /// </summary>
        /// <param name="duplication"></param>
        internal Attribute(AttributeDuplication duplication) 
        { 
            this.Duplication = duplication;  
        }

        /// <summary>
        /// Create a new attribute, which can be attached to MPI communicators.
        /// </summary>
        /// <typeparam name="T">
        ///   The type of value that will be stored with the attribute.
        /// </typeparam>
        /// <param name="duplication">
        ///   Describes how (or if) an attribute is copied when a communicator
        ///   is cloned.
        /// </param>
        /// <returns>
        ///   A new attribute whose value type is <c>T</c>. This attribute
        ///   can be attached to any communicator.
        /// </returns>
        public static Attribute Create<T>(AttributeDuplication duplication)
        {
            if (typeof(T).IsValueType)
            {
                bool isLarge = Marshal.SizeOf(typeof(T)) > Marshal.SizeOf(typeof(IntPtr));
                switch (duplication)
                {
                    case AttributeDuplication.None:
                        // Since duplication isn't an issue, allocate on the heap
                        // only when the value type is "large"
                        return new StoredAttribute<T>(duplication, isLarge);

                    case AttributeDuplication.Shallow:
                        // Small value types are stored directly in the communicator,
                        // and shallow copies are the only option. 
                        // For large value types, we handle them like objects with
                        // special holders in them, so that the garbage collector
                        // will keep track of the memory for us.
                        if (!isLarge)
                            return new StoredAttribute<T>(duplication, false);
                        else
                            return new ShallowValueAttribute<T>(duplication);
                      
                    case AttributeDuplication.Deep:
                        // For deep copies, small value types are still stored
                        // directly in the communicator (since shallow and deep
                        // copies are identical). Larger value types will be 
                        // automatically allocated/deallocated on the heap, 
                        // and each communicator will "own" its attribute value.
                        return new StoredAttribute<T>(duplication, isLarge);
                }
            }

            // For class types, which need to be on the garbage-collected heap.
            return new Attribute(duplication);
        }

        /// <summary>
        /// Dispose of this attribute. 
        /// This operation should not be performed until the attribute has
        /// been deleted from all communicators.
        /// </summary>
        public virtual void Dispose() { }

        /// <summary>
        /// Updates the object stored in <paramref name="storedValue"/> with a new
        /// <paramref name="value"/>, and returns the new stored value.
        /// </summary>
        /// <param name="storedValue">The stored value to update.</param>
        /// <param name="value">The new value.</param>
        /// <returns>The new value to store.</returns>
        internal virtual object Update(object storedValue, object value) { return value; }

        /// <summary>
        /// How this attribute will be transferred when a communicator
        /// is cloned/duplicated (via <see cref="Communicator.Clone"/>).
        /// </summary>
        public readonly AttributeDuplication Duplication;
    }

    /// <summary>
    /// An attribute attached to the low-level communicator. This abstract class
    /// only exists so that we can deal with the various different instantiations of
    /// the <see cref="StoredAttribute&lt;T&gt;"/> class without having to know the
    /// type <c>T</c>.
    /// </summary>
    abstract class LowLevelAttribute : Attribute
    {
        /// <summary>
        /// Construct a low-level attribute object. 
        /// </summary>
        internal LowLevelAttribute(AttributeDuplication duplication)
            : base(duplication)
        {
            keyval = Unsafe.MPI_KEYVAL_INVALID;
        }

        /// <summary>
        /// Set this value's attribute to a particular value.
        /// </summary>
        /// <param name="comm">The communicator to modify.</param>
        /// <param name="value">The value to store.</param>
        internal abstract void Set(MPI_Comm comm, Object value);

        /// <summary>
        /// Retrieve this attribute's value from a particular communicator.
        /// </summary>
        /// <param name="comm">The communicator to query.</param>
        /// <returns>The object stored in the communicator, if any.</returns>
        internal abstract Object Get(MPI_Comm comm);

        /// <summary>
        /// Remove this attribute from a particular communicator.
        /// </summary>
        /// <param name="comm">The communicator storing the attribute.</param>
        internal abstract void Remove(MPI_Comm comm);

        /// <summary>
        /// Retrieve the value of this attribute in a communicator,
        /// if it exists.
        /// </summary>
        /// <param name="comm">The communicator to query.</param>
        /// <returns>The value of this attribute in the communicator, if any.</returns>
        internal IntPtr? GetIntPtr(MPI_Comm comm)
        {
            int flag;
            IntPtr result;
            unsafe
            {
                int errorCode = Unsafe.MPI_Attr_get(comm, keyval, new IntPtr(&result), out flag);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            if (flag != 0)
                return result;
            else
                return null;
        }

        /// <summary>
        /// Set the value of this attribute in a communicator.
        /// </summary>
        /// <param name="comm">The communicator to modify.</param>
        /// <param name="value">The new value.</param>
        internal void SetIntPtr(MPI_Comm comm, IntPtr value)
        {
            unsafe
            {
                int errorCode = Unsafe.MPI_Attr_put(comm, keyval, value);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

        /// <summary>
        /// Internal key value. This value will be the key value returned from
        /// <see cref="Unsafe.MPI_Keyval_create"/>. 
        /// </summary>
        internal int keyval;
    }

    /// <summary>
    /// An attribute with value type <c>T</c> stored on the low-level communicator.
    /// </summary>
    /// <typeparam name="T">The type of value stored with the attribute.</typeparam>
    class StoredAttribute<T> : LowLevelAttribute
    {
        /// <summary>
        /// Creates a new attribute that will be stored inside the low-level communicator.
        /// </summary>
        /// <param name="duplication">How this attribute will be duplicated.</param>
        /// <param name="onHeap">Whether this attribute will be allocated on the heap.</param>
        public StoredAttribute(AttributeDuplication duplication, bool onHeap)
            : base(duplication)
        {
            this.onHeap = onHeap;
            unsafe
            {
               Unsafe.MPI_Copy_function copyFn = Unsafe.MPI_NULL_COPY_FN;
               Unsafe.MPI_Delete_function deleteFn = Unsafe.MPI_NULL_DELETE_FN;
#if BROKEN_NULL_DELEGATE
               copyFn = NullCopy;
               deleteFn = NullDelete;
#endif
               if (duplication != AttributeDuplication.None)
               {
                 if (onHeap)
                 {
                   copyFn = DeepCopy;
                   deleteFn = DeleteAttributeMemory;
                 }
                 else
                 {
                   copyFn = ShallowCopy;
                 }
               }

               int errorCode = Unsafe.MPI_Keyval_create(copyFn, deleteFn,
                                                         out keyval, new IntPtr());
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
        }

#if BROKEN_NULL_DELEGATE
        /// <summary>
        /// Does not copy the attribute. This is the C# equivalent of
        /// MPI's <c>MPI_NULL_COPY_FN</c>, used to work around a Mono
        /// crash in AttributesTest.
        /// </summary>
        private int NullCopy(MPI_Comm comm, int keyval, IntPtr extra_state, IntPtr attribute_val_in,
                                IntPtr attribute_val_out, out int flag)
        {
          flag = 0;
          return Unsafe.MPI_SUCCESS;
        }

        /// <summary>
        /// Does not delete the attribute. This is the C# equivalent
        /// of MPI's <c>MPI_NULL_DELETE_FN</c>, used to work around a
        /// Mono crash in AttributesTest.
        /// </summary>
        private int NullDelete(MPI_Comm comm, int keyval, IntPtr attribute_val, IntPtr extra_state)
        {
          return Unsafe.MPI_SUCCESS;
        }
#endif
        
        /// <summary>
        /// Copies an attribute value or pointer directly. This is the C# equivalent of MPI's 
        /// <c>MPI_DUP_FN</c>, which we don't have access to from within C#.
        /// </summary>
        private int ShallowCopy(MPI_Comm comm, int keyval, IntPtr extra_state, IntPtr attribute_val_in,
                                IntPtr attribute_val_out, out int flag)
        {
            unsafe
            {
                IntPtr* outPtr = (IntPtr*)attribute_val_out;
                *outPtr = attribute_val_in;
            }
            flag = 1;
            return Unsafe.MPI_SUCCESS;
        }

        /// <summary>
        /// Makes a "deep" copy of an attribute that is referenced by a pointer.
        /// </summary>
        private int DeepCopy(MPI_Comm comm, int keyval, IntPtr extra_state, IntPtr attribute_val_in,
                             IntPtr attribute_val_out, out int flag)
        {
            unsafe
            {
                IntPtr* outPtr = (IntPtr*)attribute_val_out;
                *outPtr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(T)));

                // Copy the bits
                Marshal.StructureToPtr(Marshal.PtrToStructure(attribute_val_in, typeof(T)), *outPtr, false);
            }
            flag = 1;
            return Unsafe.MPI_SUCCESS;
        }

        /// <summary>
        /// Delete the memory associated with an attribute allocated on the heap.
        /// Use as a parameter to <see cref="Unsafe.MPI_Keyval_create"/>
        /// </summary>
        private int DeleteAttributeMemory(MPI_Comm comm, int keyval, IntPtr attribute_val, IntPtr extra_state)
        {
            Marshal.FreeHGlobal(attribute_val);
            return Unsafe.MPI_SUCCESS;
        }

        /// <summary>
        /// Free the low-level attribute associated with this attribute.
        /// </summary>
        public override void Dispose()
        {
            if (keyval != Unsafe.MPI_KEYVAL_INVALID)
            {
                if (!Environment.Finalized)
                {
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Keyval_free(ref keyval);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
                keyval = Unsafe.MPI_KEYVAL_INVALID;
            }
        }

        internal override void Set(MPI_Comm comm, object value)
        {
            if (value.GetType() != typeof(T))
                throw new InvalidCastException("Unable to convert value when setting communicator attribute");

            if (onHeap)
            {
                IntPtr? ptr = GetIntPtr(comm);
                if (ptr == null)
                {
                    // There was no attribute here before, so allocate
                    // memory for this pointer and put it into the
                    // communicator.
                    ptr = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(T)));
                    SetIntPtr(comm, ptr.Value);                    
                }

                Marshal.StructureToPtr(value, ptr.Value, false);

            }
            else
            {
                IntPtr newValue;
                unsafe
                {
                    Marshal.StructureToPtr(value, new IntPtr(&newValue), false);
                }
                SetIntPtr(comm, newValue);
            }
        }

        internal override object Get(MPI_Comm comm)
        {
            IntPtr? ptr = GetIntPtr(comm);
            if (ptr == null)
                return null;
            if (onHeap)
                return Marshal.PtrToStructure(ptr.Value, typeof(T));
            else
            {
                IntPtr ptrValue = ptr.Value;
                unsafe
                {
                    return Marshal.PtrToStructure(new IntPtr(&ptrValue), typeof(T));
                }
            }
        }

        internal override void Remove(MPI_Comm comm)
        {
            unsafe
            {
                Unsafe.MPI_Attr_delete(comm, keyval);
            }
        }

        /// <summary>
        /// Whether the attribute's value is stored on the heap or not.
        /// <para>
        ///   When the attribute is stored on the heap, a pointer to the
        ///   value is stored in the low-level communicator, and will be 
        ///   freed when the attribute is deleted.
        /// </para>
        /// 
        /// <para>
        ///   When the attribute is not stored on the heap, the value itself
        ///   will be placed into the communicator directly. The value
        ///   must not be larger than the size of an <c>IntPtr</c>.
        /// </para>
        /// </summary>
        private bool onHeap;
    }

    /// <summary>
    /// A value-type attribute allocated with shallow copy semantics. These attributes 
    /// are stored as (boxed) objects in the <see cref="AttributeSet"/>. When we update
    /// their values, we copy those values directly into the object, rather than 
    /// creating a new object, so we achieve shallow semantics.
    /// </summary>
    /// <typeparam name="T">The value type of the attribute.</typeparam>
    class ShallowValueAttribute<T> : Attribute
    {
        internal ShallowValueAttribute(AttributeDuplication duplication)
            : base(duplication)
        {
        }

        internal override object Update(object storedValue, object value)
        {
            GCHandle handle = GCHandle.Alloc(storedValue, GCHandleType.Pinned);
            Marshal.StructureToPtr(value, handle.AddrOfPinnedObject(), true);
            handle.Free();
            return storedValue;
        }
    }

    /// <summary>
    /// Contains the attributes attached to a communicator.
    /// </summary>
    /// 
    /// <remarks>Each communicator
    /// can contain key/value pairs with extra information about the communicator
    /// that can be queried from other languages and compilers. The keys in the 
    /// attribute set are instances of the <see cref="Attribute"/> class, each of 
    /// which will be associated with a specific type of value. The values associated
    /// with any attribute can be added, modified, queried, or removed for a 
    /// particular communicator.
    /// 
    /// <para>
    ///   When a communicator is cloned, the attributes are copied to the new
    ///   communicator. When creating an <see cref="Attribute"/>, decide whether
    ///   not to copy the attribute (<see cref="AttributeDuplication.None"/>),
    ///   to copy only a reference to the attribute (<see cref="AttributeDuplication.Shallow"/>),
    ///   or make a clone of the attribute (<see cref="AttributeDuplication.Deep"/>).
    /// </para>
    /// </remarks>
    public sealed class AttributeSet
    {
        internal AttributeSet(MPI_Comm comm)
        {
            this.comm = comm;
            this.objectAttributes = new Dictionary<Attribute, object>();
        }

        /// <summary>
        /// Access or modify the value associated with the given attribute.
        /// </summary>
        /// <param name="attribute">The attribute key.</param>
        /// <returns>
        ///   The value associated with the given attribute, 
        ///   or <c>null</c> if the attribute hasn't been set for this 
        ///   communicator.
        /// </returns>
        public Object this[Attribute attribute]
        {
            get
            {
                if (attribute is LowLevelAttribute)
                    // Attribute is stored with the low-level communicator: extract it
                    return ((LowLevelAttribute)attribute).Get(comm);
                else
                {
                    if (objectAttributes.ContainsKey(attribute))
                        return objectAttributes[attribute];
                    else
                        return null;
                }
            }

            set
            {
                if (attribute is LowLevelAttribute)
                    // This is a low-level attribute; set it on the communicator
                    ((LowLevelAttribute)attribute).Set(comm, value);
                else if (objectAttributes.ContainsKey(attribute))
                    objectAttributes[attribute] = attribute.Update(objectAttributes[attribute], value);
                else
                    objectAttributes[attribute] = value;
            }
        }

        /// <summary>
        /// Remove the given attribute from this communicator. If not such attribute exists,
        /// the communicator will not be changed and no exception will be thrown.
        /// </summary>
        /// <param name="attribute">The attribute to remove.</param>
        public void Remove(Attribute attribute)
        {
            if (attribute is LowLevelAttribute)
                // Attribute is stored with the low-level communicator: delete it
                ((LowLevelAttribute)attribute).Remove(comm);
            else
                objectAttributes.Remove(attribute);
        }

        /// <summary>
        /// Copies the attributes from the communicator being duplicated to
        /// this attribute set (which is associated with the new communicator).
        /// </summary>
        /// <param name="otherAttributes">
        ///   The attributes from the communicator being duplicated.
        /// </param>
        internal void CopyAttributesFrom(AttributeSet otherAttributes)
        {
            foreach (KeyValuePair<Attribute, Object> kvp in otherAttributes.objectAttributes)
            {
                switch (kvp.Key.Duplication)
                {
                    case AttributeDuplication.None: 
                        // attribute not copied
                        break;

                    case AttributeDuplication.Shallow: 
                        objectAttributes[kvp.Key] = kvp.Value; 
                        break;

                    case AttributeDuplication.Deep:
                        objectAttributes[kvp.Key] = ((ICloneable)kvp.Value).Clone();
                        break;
                }
            }
        }

        /// <summary>
        /// The low-level MPI communicator with which this attribute
        /// set is associated.
        /// </summary>
        private MPI_Comm comm;

        /// <summary>
        /// Extra attributes not stored within the low-level MPI communicator.
        /// </summary>
        private Dictionary<Attribute, Object> objectAttributes;
    };
}
