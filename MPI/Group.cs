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
    ///   The <c>Group</c> class provides the ability to manipulate sets of MPI processes.
    /// </summary>
    public class Group
    {
        /// <summary>
        /// A predefined group containing no processes.
        /// </summary>
        public static Group empty = Adopt(Unsafe.MPI_GROUP_EMPTY);

        #region Group management
        /// <summary>
        ///   Users cannot construct a Group directly. They typically will retrieve a group from
        ///   a communicator, from other groups, or, in rare cases, adopt one from the low-level
        ///   interface.
        /// </summary>
        private Group() { }

        /// <summary>
        ///   Adopts a low-level MPI group that was created with any of the low-level MPI facilities.
        ///   The resulting <c>Group</c> object will manage the lifetime of the low-level MPI group,
        ///   and will call <see cref="Unsafe.MPI_Group_free"/> when it is disposed or finalized.
        /// </summary>
        /// <remarks>
        ///   This constructor should only be used in rare cases where the program 
        ///   is manipulating MPI groups through the low-level MPI interface.
        /// </remarks>
        public static Group Adopt(MPI_Group group)
        {
            if (group == Unsafe.MPI_GROUP_NULL)
                return null;

            Group result = new Group();
            result.group = group;
            return result;
        }

        /// <summary>
        /// Finalizer that frees the MPI group.
        /// </summary>
        ~Group()
        {
            // Free any non-predefined groups
            if (group != Unsafe.MPI_GROUP_EMPTY && group != Unsafe.MPI_GROUP_NULL)
            {
                if (!Environment.Finalized)
                {
                    unsafe
                    {
                        int errorCode = Unsafe.MPI_Group_free(ref group);
                        if (errorCode != Unsafe.MPI_SUCCESS)
                            throw Environment.TranslateErrorIntoException(errorCode);
                    }
                }
                group = Unsafe.MPI_GROUP_NULL;
            }
        }

        /// <summary>
        /// Free the MPI group explicitly.
        /// </summary>
        public void Dispose()
        {
            // Free any non-predefined groups
            if (group != Unsafe.MPI_GROUP_EMPTY && group != Unsafe.MPI_GROUP_NULL)
            {
                unsafe
                {
                  int errorCode = Unsafe.MPI_Group_free(ref group);
                  if (errorCode != Unsafe.MPI_SUCCESS)
                      throw Environment.TranslateErrorIntoException(errorCode);
                }
                group = Unsafe.MPI_GROUP_NULL;
            }
 
            // We don't need to finalize this object
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Constructs the union of two groups, containing those processes that are either in
        /// <paramref name="group1"/> or <paramref name="group2"/>.
        /// </summary>
        public static Group operator |(Group group1, Group group2)
        {
            MPI_Group newGroup;
            unsafe
            {
                int errorCode = Unsafe.MPI_Group_union(group1.group, group2.group, out newGroup);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            if (newGroup == Unsafe.MPI_GROUP_EMPTY)
                return empty;
            else
                return Adopt(newGroup);
        }

        /// <summary>
        /// Constructs the intersection of two groups, containing those processes that are in
        /// both <paramref name="group1"/> and <paramref name="group2"/>.
        /// </summary>
        public static Group operator &(Group group1, Group group2)
        {
            MPI_Group newGroup;
            unsafe
            {
                int errorCode = Unsafe.MPI_Group_intersection(group1.group, group2.group, out newGroup);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            if (newGroup == Unsafe.MPI_GROUP_EMPTY)
                return empty;
            else
                return Adopt(newGroup);
        }

        /// <summary>
        /// Constructs the difference of two groups, containing those processes that are in
        /// <paramref name="group1"/> but not <paramref name="group2"/>.
        /// </summary>
        public static Group operator -(Group group1, Group group2)
        {
            MPI_Group newGroup;
            unsafe
            {
                int errorCode = Unsafe.MPI_Group_difference(group1.group, group2.group, out newGroup);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            if (newGroup == Unsafe.MPI_GROUP_EMPTY)
                return empty;
            else
                return Adopt(newGroup);
        }

        /// <summary>
        /// Create a subset of this group that includes only the processes with the
        /// given ranks.
        /// </summary>
        public Group IncludeOnly(int[] ranks)
        {
            MPI_Group newGroup;
            unsafe
            {
                int errorCode = Unsafe.MPI_Group_incl(group, ranks.Length, ranks, out newGroup);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            if (newGroup == Unsafe.MPI_GROUP_EMPTY)
                return empty;
            else
                return Adopt(newGroup);
        }

        /// <summary>
        /// Create a subset of this group that includes all of the processes in this
        /// group except those identified by the given ranks.
        /// </summary>
        public Group Exclude(int[] ranks)
        {
            MPI_Group newGroup;
            unsafe
            {
                int errorCode = Unsafe.MPI_Group_excl(group, ranks.Length, ranks, out newGroup);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }

            if (newGroup == Unsafe.MPI_GROUP_EMPTY)
                return empty;
            else
                return Adopt(newGroup);
        }
        #endregion

        #region Group accessors
        /// <summary>
        /// Sentinel value used to indicate that a particular process is not part of a group.
        /// </summary>
        public const int NoProcess = Unsafe.MPI_UNDEFINED;

        /// <summary>
        ///   The number of processes within this group.
        /// </summary>
        public int Size
        {
            get
            {
                int size;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Group_size(group, out size);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                return size;
            }
        }

        /// <summary>
        ///   The rank of the calling process within this group. This will be a value in [0, <see cref="Size"/>-1).
        /// </summary>
        public int Rank
        {
            get
            {
                int rank;
                unsafe
                {
                    int errorCode = Unsafe.MPI_Group_rank(group, out rank);
                    if (errorCode != Unsafe.MPI_SUCCESS)
                        throw Environment.TranslateErrorIntoException(errorCode);
                }
                return rank;
            }
        }

        /// <summary>
        ///   Translates the ranks of processes in this group to the ranks of the same processes within a different group.
        /// </summary>
        /// <param name="ranks">The rank values in this group that will be translated.</param>
        /// <param name="other">The group whose ranks we are translating to.</param>
        /// <returns>
        ///   An integer array containing the ranks of the processes in <paramref name="other"/> that correspond to
        ///   the ranks of the same processes in this group. For processes that are in this group but not 
        ///   <paramref name="other"/>, the resulting array will contain <see cref="Group.NoProcess"/>.
        /// </returns>
        public int[] TranslateRanks(int[] ranks, Group other)
        {
            int[] result = new int[ranks.Length];
            unsafe
            {
                int errorCode = Unsafe.MPI_Group_translate_ranks(group, ranks.Length, ranks, other.group, result);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            return result;
        }

        /// <summary>
        ///   Compare two MPI groups.
        /// </summary>
        /// <list>
        ///   <listheader>
        ///     <term>Value</term>
        ///     <description>Description</description>
        ///   </listheader>
        /// <item>
        ///   <term><see cref="Comparison.Identical"/></term>
        ///   <description>The two <c>Group</c> objects represent the same group.</description>
        /// </item>
        /// <item>
        ///   <term><see cref="Comparison.Congruent"/></term>
        ///   <description>
        ///     The two <c>Group</c> objects contain the same processes with the same ranks,
        ///     but represent different groups.
        ///   </description>
        /// </item>
        /// <item>
        ///   <term><see cref="Comparison.Similar"/></term>
        ///   <description>
        ///     The two <c>Group</c> objects contain the same processes, but with different ranks.
        ///   </description>
        /// </item>
        /// <item>
        ///   <term><see cref="Comparison.Unequal"/></term>
        ///   <descsription>The two <c>Group</c> objects are different.</descsription>
        /// </item>
        /// </list>      
        public Comparison Compare(Group other)
        {
            int result;
            unsafe
            {
                int errorCode = Unsafe.MPI_Group_compare(group, other.group, out result);
                if (errorCode != Unsafe.MPI_SUCCESS)
                    throw Environment.TranslateErrorIntoException(errorCode);
            }
            return Unsafe.ComparisonFromInt(result);
        }

        #endregion

        /// <summary>
        /// The low-level MPI group handle.
        /// </summary>
        internal MPI_Group group = Unsafe.MPI_GROUP_EMPTY;
    }
}
