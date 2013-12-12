/* Copyright (C) 2007  The Trustees of Indiana University
 *
 * Use, modification and distribution is subject to the Boost Software
 * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *  
 * Authors: Douglas Gregor
 *          Andrew Lumsdaine
 * 
 * This file contains support for the RequestList class, which is a container
 * of MPI requests that allows one to perform operations on a set of requests
 * at the same time, e.g., wait for all requests to complete.
 */
using System;
using System.Collections.Generic;

namespace MPI
{
    /// <summary>
    /// A request list contains a list of outstanding MPI requests. 
    /// </summary>
    /// 
    /// <remarks>
    /// The requests in a <c>RequestList</c>
    /// are typically non-blocking send or receive operations (e.g.,
    /// <see cref="Communicator.ImmediateSend&lt;T&gt;(T, int, int)"/>,
    /// <see cref="Communicator.ImmediateReceive&lt;T&gt;(int, int)"/>). The
    /// request list provides the ability to operate on the set of MPI requests
    /// as a whole, for example by waiting until all requests complete before
    /// returning or testing whether any of the requests have completed.
    /// </remarks>
    public class RequestList
    {
        /// <summary>
        /// Create a new, empty request list.
        /// </summary>
        public RequestList()
        {
            this.requests = new List<Request>();
        }

        /// <summary>
        /// Add a new request to the request list.
        /// </summary>
        /// <param name="request">The request to add.</param>
        public void Add(Request request)
        {
            requests.Add(request);
        }

        /// <summary>
        /// Remove a request from the request list.
        /// </summary>
        /// <param name="request">Request to remove.</param>
        public void Remove(Request request)
        {
            requests.Remove(request);
        }

        /// <summary>
        /// Retrieves the number of elements in this list of requests.
        /// </summary>
        public int Count
        {
            get
            {
                return this.requests.Count;
            }
        }

        /// <summary>
        /// Waits until any request has completed. That request will then be removed 
        /// from the request list and returned.
        /// </summary>
        /// <returns>The completed request, which has been removed from the request list.</returns>
        public Request WaitAny()
        {
            if (requests.Count == 0)
                throw new ArgumentException("Cannot call MPI.RequestList.WaitAny with an empty request list");

            while (true)
            {
                Request req = TestAny();
                if (req != null)
                    return req;
            }
        }

        /// <summary>
        /// Determines whether any request has completed. If so, that request will be removed
        /// from the request list and returned. 
        /// </summary>
        /// <returns>
        ///   The first request that has completed, if any. Otherwise, returns <c>null</c> to
        ///   indicate that no request has completed.
        /// </returns>
        public Request TestAny()
        {
            int n = requests.Count;
            for (int i = 0; i < n; ++i)
            {
                Request req = requests[i];
                if (req.Test() != null)
                {
                    requests.RemoveAt(i);
                    return req;
                }
            }

            return null;
        }

        /// <summary>
        /// Wait until all of the requests has completed before returning.
        /// </summary>
        /// <returns>A list containing all of the completed requests.</returns>
        public List<Request> WaitAll()
        {
            List<Request> result = new List<Request>();
            while (requests.Count > 0)
            {
                Request req = WaitAny();
                result.Add(req);
            }
            return result;
        }

        /// <summary>
        /// Test whether all of the requests have completed. If all of the
        /// requests have completed, the result is the list of requests. 
        /// Otherwise, the result is <c>null</c>.
        /// </summary>
        /// <returns>Either the list of all completed requests, or null.</returns>
        public List<Request> TestAll()
        {
            int n = requests.Count;
            for (int i = 0; i < n; ++i)
            {
                if (requests[i].Test() == null)
                    return null;
            }

            List<Request> result = requests;
            requests = new List<Request>();
            return result;
        }

        /// <summary>
        /// Wait for at least one request to complete, then return a list of
        /// all of the requests that have completed at this point.
        /// </summary>
        /// <returns>
        ///   A list of all of the requests that have completed, which
        ///   will contain at least one element.
        /// </returns>
        public List<Request> WaitSome()
        {
            if (requests.Count == 0)
                throw new ArgumentException("Cannot call MPI.RequestList.WaitAny with an empty request list");

            List<Request> result = new List<Request>();
            while (result.Count == 0)
            {
                int n = requests.Count;
                for (int i = 0; i < n; ++i)
                {
                    Request req = requests[i];
                    if (req.Test() != null)
                    {
                        requests.RemoveAt(i);
                        --i;
                        --n;
                        result.Add(req);
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// Return a list of all requests that have completed.
        /// </summary>
        /// <returns>
        ///   A list of all of the requests that have completed. If
        ///   no requests have completed, returns <c>null</c>.
        /// </returns>
        public List<Request> TestSome()
        {
            List<Request> result = null;
            int n = requests.Count;
            for (int i = 0; i < n; ++i)
            {
                Request req = requests[i];
                if (req.Test() != null)
                {
                    requests.RemoveAt(i);
                    --i;
                    --n;

                    if (result == null)
                        result = new List<Request>();
                    result.Add(req);
                }
            }
            return result;
        }

        /// <summary>
        /// The actual list of requests.
        /// </summary>
        protected List<Request> requests;
    }
}
