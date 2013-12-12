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

namespace MPI
{
    /// <summary>
    /// Allocates unique tags for the various point-to-point communications that are used within 
    /// the MPI.NET implementation but must remain hidden from the user. All of the routines
    /// in the <c>TagAllocator</c> class are thread-safe when MPI is running in a mode that supports
    /// multi-threading.
    /// </summary>
    class TagAllocator
    {
        /// <summary>
        /// Initialize a new tag allocator
        /// </summary>
        public TagAllocator() 
        {
            nextSendTag = 1; // 0 we reserve for collectives
            returnedSendTags = new Stack<int>();
        }

        /// <summary>
        /// Allocate a new, unique tag for a single serialized send. The tag
        /// must be returned via <see cref="ReturnSendTag"/>.
        /// </summary>
        /// <returns>A new, unique tag for use with this serialized send.</returns>
        public int AllocateSendTag()
        {
            if (Environment.Threading == Threading.Multiple)
            {
                lock (this)
                {
                    return AllocateSendTagImpl();
                }
            }
            else
                return AllocateSendTagImpl();
        }

        /// <summary>
        /// Actual implementation of <see cref="AllocateSendTag"/>.
        /// </summary>
        private int AllocateSendTagImpl()
        {
            if (returnedSendTags.Count > 0)
                return returnedSendTags.Pop();
            else
                return nextSendTag++;
        }

        /// <summary>
        /// Returns a tag allocated via <see cref="AllocateSendTag"/>.
        /// </summary>
        /// <param name="tag">The tag to return.</param>
        public void ReturnSendTag(int tag)
        {
            if (Environment.Threading == Threading.Multiple)
            {
                lock (this)
                {
                    ReturnSendTagImpl(tag);
                }
            }
            else
                ReturnSendTagImpl(tag);
        }

        /// <summary>
        /// Implementation of <see cref="ReturnSendTag"/>.
        /// </summary>
        /// <param name="tag">Tag value being returned.</param>
        private void ReturnSendTagImpl(int tag)
        {
            if (tag == nextSendTag - 1)
                --nextSendTag;
            else
                returnedSendTags.Push(tag);
        }

        /// <summary>
        /// The next unique tag that will be given out for a serialized send. This
        /// value starts at zero and increases as more tags are requested.
        /// </summary>
        private int nextSendTag = 1; // 0 we reserve for collectives

        /// <summary>
        /// Stack of send tags that have been returned out-of-order. We always
        /// exhaust these tags first, before allocating a new send tag by increasing
        /// <see cref="nextSendTag"/>.
        /// </summary>
        private Stack<int> returnedSendTags;
    }
}
