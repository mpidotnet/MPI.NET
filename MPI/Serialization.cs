using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MPI
{
    public class Serialization : IDisposable
    {
        /// <summary>
        /// Used to serialize non-primitive objects.  
        /// All processes communicating with this Communicator must have the same value.
        /// </summary>
        public ISerializer Serializer { get; set; } = BinaryFormatterSerializer.Default;
        /// <summary>
        /// Delegate to be called with the size of every block of serialized data sent.
        /// </summary>
        public Action<int, int> SendLogger { get; set; }
        /// <summary>
        /// Delegate to be called when a process is waiting.
        /// </summary>
        public Action<string> WaitLogger { get; set; }
        /// <summary>
        /// Only relevant if SplitLargeObjects = true.  
        /// All processes communicating with this Communicator must have the same value.
        /// </summary>
        public int BufferSize { get; set; } = 1024 * 1024 * 16;
        /// <summary>
        /// The maximum number of serialization worker threads to create.
        /// </summary>
        public int WorkerCount { get; set; } = 16;
        internal int SendWorkerCount, ReceiveWorkerCount;
        internal BlockingCollection<ISerializedReceiveInfo> Receives = new BlockingCollection<ISerializedReceiveInfo>();
        internal BlockingCollection<ISerializedSendInfo> Sends = new BlockingCollection<ISerializedSendInfo>();
        internal ConcurrentQueue<Serialization.RequestGenerator> WorkItems = new ConcurrentQueue<Serialization.RequestGenerator>();

        public void Dispose()
        {
            while (SendWorkerCount > 0)
            {
                Sends.Add(null);
                SendWorkerCount--;
            }
            while (ReceiveWorkerCount > 0)
            {
                Receives.Add(null);
                ReceiveWorkerCount--;
            }
        }

        /// <summary>
        /// Send an object with a large serialized representation.
        /// </summary>
        /// <param name="comm"></param>
        /// <param name="value"></param>
        /// <param name="dest"></param>
        /// <param name="tag"></param>
        internal static void SendLarge<T>(Communicator comm, T value, int dest, int tag)
        {
            var batch = new BatchSendReceive();
            batch.ImmediateSend(comm, value, dest, tag);
            batch.WaitAll(comm);
        }

        /// <summary>
        /// Send an object with a large serialized representation.
        /// </summary>
        /// <param name="comm"></param>
        /// <param name="value"></param>
        /// <param name="dest"></param>
        /// <param name="tag"></param>
        internal static void SendLarge1<T>(Communicator comm, T value, int dest, int tag)
        {
            bool done = false;
            Action flushAction = delegate ()
            {
                if (!done)
                {
                    comm.Send(0, dest, tag);
                    done = true;
                }
            };
            Action<byte[]> writeAction = delegate (byte[] bytes)
            {
                comm.Serialization.SendLogger?.Invoke(bytes.Length, dest);
                comm.Send(bytes.Length, dest, tag);
                comm.Send(bytes, dest, tag);
                if (bytes.Length < comm.Serialization.BufferSize)
                    done = true; // the receiver knows that this is the last block.
            };
            using (var stream = new BufferedStream(new ActionStream(null, writeAction, flushAction), comm.Serialization.BufferSize))
            {
                comm.Serialization.Serializer.Serialize(stream, value);
            }
        }

        public class RequestGenerator
        {
            public readonly Func<Request> generator;
            public readonly string Tag;
            public readonly EventWaitHandle waitHandle;

            public RequestGenerator(EventWaitHandle waitHandle, string tag, Func<Request> generator)
            {
                this.generator = generator;
                this.Tag = tag;
                this.waitHandle = waitHandle;
            }
        }

        /// <summary>
        /// Receive an object with a large serialized representation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="comm"></param>
        /// <param name="source"></param>
        /// <param name="tag"></param>
        /// <param name="outValue"></param>
        public static void ReceiveLarge<T>(Communicator comm, int source, int tag, out T outValue)
        {
            var batch = new BatchSendReceive();
            T outValueLocal = default(T);
            batch.ImmediateReceive<T>(comm, source, tag, value =>
            {
                outValueLocal = value;
            });
            batch.WaitAll(comm);
            outValue = outValueLocal;
        }

        /// <summary>
        /// Receive an object with a large serialized representation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="comm"></param>
        /// <param name="source"></param>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="status"></param>
        public static void ReceiveLarge<T>(Communicator comm, int source, int tag, out T value, out CompletedStatus status)
        {
            CompletedStatus innerStatus = default(CompletedStatus);
            bool done = false;
            byte[] leftoverBytes = null;
            int leftoverPosition = 0;
            Func<byte[], int> readAction = delegate (byte[] bytes)
            {
                if (done)
                    return 0;
                int length;
                if (leftoverBytes != null)
                {
                    length = Math.Min(bytes.Length, checked(leftoverBytes.Length - leftoverPosition));
                }
                else
                {
                    comm.Receive(source, tag, out length, out innerStatus);
                }
                if (length < comm.Serialization.BufferSize)
                    done = true;
                if (length == 0)
                    return 0;
                if (leftoverBytes != null)
                {
                    Array.Copy(leftoverBytes, leftoverPosition, bytes, 0, length);
                    checked { leftoverPosition += length; }
                }
                else
                {
                    byte[] array = new byte[length];
                    comm.Receive(source, tag, ref array);
                    if (length > bytes.Length)
                    {
                        length = bytes.Length;
                        leftoverBytes = array;
                        leftoverPosition = length;
                        Array.Copy(array, 0, bytes, 0, length);
                    }
                    else
                    {
                        array.CopyTo(bytes, 0);
                    }
                }
                return length;
            };
            using (var stream = new BufferedStream(new ActionStream(readAction, null), comm.Serialization.BufferSize))
            {
                value = comm.Serialization.Serializer.Deserialize<T>(stream);
            }
            status = innerStatus;
        }

        /// <summary>
        /// Gather an object with large serialized representation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="comm"></param>
        /// <param name="isRoot"></param>
        /// <param name="size"></param>
        /// <param name="inValue"></param>
        /// <param name="root"></param>
        /// <param name="outValues"></param>
        public static void GatherLarge<T>(Communicator comm, bool isRoot, int size, T inValue, int root, ref T[] outValues)
        {
            const int tag = 5;
            if (isRoot)
            {
                if (outValues == null || outValues.Length != size)
                    outValues = new T[size];

                var batch = new BatchSendReceive();
                T[] outValuesLocal = outValues;
                for (int sender = 0; sender < size; sender++)
                {
                    if (sender == root)
                    {
                        outValues[sender] = inValue;
                        continue;
                    }
                    // make a copy since 'sender' is being modified
                    int senderLocal = sender;
                    batch.ImmediateReceive<T>(comm, sender, tag, value =>
                    {
                        outValuesLocal[senderLocal] = value;
                    });
                }
                batch.WaitAll(comm);
            }
            else
            {
                SendLarge(comm, inValue, root, tag);
            }
        }

        const int Tag_Alltoall = 11;

        public static void Alltoall<T>(Communicator comm, T[] inValues, T[] outValues, int[] sendCounts = null, int[] recvCounts = null)
        {
            SpanTimer.Enter("Alltoall");
            int tag = Tag_Alltoall;
            var batch = new BatchSendReceive();
            for (int destination = 0; destination < comm.Size; destination++)
            {
                if (destination == comm.Rank)
                    continue;
                if (sendCounts != null && sendCounts[destination] == 0)
                    continue;
                batch.ImmediateSend(comm, inValues[destination], destination, tag);
            }
            for (int sender = 0; sender < comm.Size; sender++)
            {
                if (sender == comm.Rank)
                {
                    outValues[sender] = inValues[sender];
                    continue;
                }
                if (recvCounts != null && recvCounts[sender] == 0)
                    continue;
                // make a copy since 'sender' is being modified
                int senderLocal = sender;
                batch.ImmediateReceive<T>(comm, sender, tag, value =>
                {
                    outValues[senderLocal] = value;
                });
            }
            batch.WaitAll(comm);
            SpanTimer.Leave("Alltoall");
        }

        public static void AlltoallFlattened<T>(Communicator comm, T[] inValues, int[] sendCounts, int[] recvCounts, T[] outValues)
        {
            T[][] inValuesSplit = new T[sendCounts.Length][];
            int position = 0;
            for (int i = 0; i < sendCounts.Length; i++)
            {
                if (sendCounts[i] == 0)
                    continue;
                inValuesSplit[i] = new T[sendCounts[i]];
                for (int j = 0; j < sendCounts[i]; j++)
                {
                    inValuesSplit[i][j] = inValues[position++];
                }
            }
            T[][] outValuesSplit = new T[recvCounts.Length][];
            Alltoall(comm, inValuesSplit, outValuesSplit, sendCounts, recvCounts);
            position = 0;
            for (int i = 0; i < recvCounts.Length; i++)
            {
                for (int j = 0; j < recvCounts[i]; j++)
                {
                    outValues[position++] = outValuesSplit[i][j];
                }
            }
        }

        private class ArrayInterval<T> : ICollection<T>
        {
            readonly T[] array;
            readonly int start;
            readonly int count;

            public ArrayInterval(T[] array, int start, int count)
            {
                this.array = array;
                this.start = start;
                this.count = count;
            }

            public int Count
            {
                get
                {
                    return count;
                }
            }

            public bool IsReadOnly
            {
                get
                {
                    return true;
                }
            }

            public void Add(T item)
            {
                throw new NotImplementedException();
            }

            public void Clear()
            {
                throw new NotImplementedException();
            }

            public bool Contains(T item)
            {
                throw new NotImplementedException();
            }

            public void CopyTo(T[] array, int arrayIndex)
            {
                throw new NotImplementedException();
            }

            public IEnumerator<T> GetEnumerator()
            {
                int lastIndex = checked(start + count);
                for (int i = start; i < lastIndex; i++)
                {
                    yield return array[i];
                }
            }

            public bool Remove(T item)
            {
                throw new NotImplementedException();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        public static void AlltoallFlattened2<T>(Communicator comm, T[] inValues, int[] sendCounts, int[] recvCounts, T[] outValues)
        {
            ICollection<T>[] inValuesSplit = new ICollection<T>[sendCounts.Length];
            int position = 0;
            for (int i = 0; i < sendCounts.Length; i++)
            {
                if (sendCounts[i] == 0)
                    continue;
                inValuesSplit[i] = new ArrayInterval<T>(inValues, position, sendCounts[i]);
            }
            ICollection<T>[] outValuesSplit = new ICollection<T>[recvCounts.Length];
            Alltoall(comm, inValuesSplit, outValuesSplit, sendCounts, recvCounts);
            position = 0;
            for (int i = 0; i < recvCounts.Length; i++)
            {
                if (recvCounts[i] == 0)
                    continue;
                foreach (var item in outValuesSplit[i])
                {
                    outValues[position++] = item;
                }
            }
        }

        /// <summary>
        /// Scatter an object with large serialized representation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="comm"></param>
        /// <param name="isRoot"></param>
        /// <param name="values"></param>
        /// <param name="root"></param>
        /// <returns></returns>
        public static T ScatterLarge<T>(Communicator comm, bool isRoot, T[] values, int root)
        {
            const int tag = 7;
            if (isRoot)
            {
                var batch = new BatchSendReceive();
                for (int destination = 0; destination < comm.Size; destination++)
                {
                    if (destination == root)
                        continue;
                    batch.ImmediateSend(comm, values[destination], destination, tag);
                }
                batch.WaitAll(comm);
                return values[root];
            }
            else
            {
                T value;
                ReceiveLarge(comm, root, tag, out value);
                return value;
            }
        }

        internal class BatchSendReceive
        {
            int pendingCount;
            Action completed;

            public BatchSendReceive()
            {
                completed = () =>
                {
                    Interlocked.Decrement(ref pendingCount);
                };
            }

            public void ImmediateSend<T>(Communicator comm, T value, int dest, int tag)
            {
                if (comm.Serialization.SendWorkerCount < comm.Serialization.WorkerCount)
                    Serialization.StartSendWorker(comm.Serialization.Sends, comm.Serialization.WorkItems, comm);
                pendingCount++;
                comm.Serialization.Sends.Add(new SerializedSendInfo<T>(value, dest, tag, completed));
            }

            public void ImmediateReceive<T>(Communicator comm, int source, int tag, Action<T> action)
            {
                if (comm.Serialization.ReceiveWorkerCount < comm.Serialization.WorkerCount)
                    Serialization.StartReceiveWorker(comm.Serialization.Receives, comm.Serialization.WorkItems, comm);
                pendingCount++;
                comm.Serialization.Receives.Add(new SerializedReceiveInfo<T>(source, tag, action, completed));
            }

            public void WaitAll(Communicator comm)
            {
                RequestList requests = new RequestList();
                Dictionary<Request, EventWaitHandle> waitHandles = new Dictionary<Request, EventWaitHandle>();
                Dictionary<Request, string> requestTags = new Dictionary<Request, string>();
                DateTime startTime = DateTime.UtcNow;
                int nextMinute = 1;
                var workItems = comm.Serialization.WorkItems;
                while (pendingCount > 0 || workItems.Count > 0 || requests.Count > 0)
                {
                    RequestGenerator workItem;
                    if (workItems.TryDequeue(out workItem))
                    {
                        var request = workItem.generator();
                        requests.Add(request);
                        waitHandles.Add(request, workItem.waitHandle);
                        requestTags.Add(request, workItem.Tag);
                    }
                    else
                    {
                        var request = requests.TestAny();
                        if (request != null)
                        {
                            waitHandles[request].Set();
                            waitHandles.Remove(request);
                            requestTags.Remove(request);
                        }
                    }
                    var elapsed = DateTime.UtcNow - startTime;
                    if (elapsed.TotalMinutes > nextMinute)
                    {
                        StringBuilder sb = new StringBuilder();
                        if (requestTags.Count > 0)
                        {
                            sb.Append(": ");
                            bool firstTime = true;
                            foreach (var tag in requestTags.Values)
                            {
                                if (!firstTime)
                                    sb.Append(",");
                                sb.Append(tag);
                                firstTime = false;
                            }
                        }
                        comm.Serialization.WaitLogger?.Invoke($"Waiting on {pendingCount} pending tasks, {workItems.Count} work items, {requests.Count} requests{sb.ToString()}");
                        nextMinute++;
                    }
                }
                if (waitHandles.Count != 0)
                    throw new Exception("waitHandles.Count != 0");
            }
        }

        internal static void StartSendWorker(BlockingCollection<ISerializedSendInfo> sends, ConcurrentQueue<RequestGenerator> workItems, Communicator comm)
        {
            Thread worker = new Thread(new ThreadStart(() =>
            {
                using (EventWaitHandle waitHandle = new AutoResetEvent(false))
                {
                    bool done = false;
                    int dest = default(int);
                    int tag = default(int);
                    Action flushAction = delegate ()
                    {
                        if (!done)
                        {
                            var workItem = new RequestGenerator(waitHandle, $"Send to {dest}", () =>
                                    comm.ImmediateSend(0, dest, tag)
                            );
                            workItems.Enqueue(workItem);
                            comm.Serialization.SendLogger?.Invoke(4, dest);
                            waitHandle.WaitOne();
                            done = true;
                        }
                    };
                    Action<byte[]> writeAction = delegate (byte[] bytes)
                    {
                        if (bytes.Length < comm.Serialization.BufferSize)
                            done = true; // the receiver knows that this is the last block.
                        var workItem = new RequestGenerator(waitHandle, $"Serialized send to {dest}", () =>
                        {
                            return new SerializedSendRequest(comm, dest, tag, bytes, bytes.Length, bytes.Length);
                        });
                        workItems.Enqueue(workItem);
                        comm.Serialization.SendLogger?.Invoke(bytes.Length, dest);
                        waitHandle.WaitOne();
                    };
                    using (var stream = new BufferedStream(new ActionStream(null, writeAction, flushAction), comm.Serialization.BufferSize))
                    {
                        while (true)
                        {
                            var info = sends.Take();
                            if (info == null)
                                break;
                            done = false;
                            dest = info.Destination;
                            tag = info.Tag;
                            info.Serialize(comm.Serialization.Serializer, stream);
                            stream.Flush();
                            info.Completed();
                        }
                    }
                }
            }));
            worker.IsBackground = true;
            worker.Start();
            comm.Serialization.SendWorkerCount++;
        }

        internal static void StartReceiveWorker(BlockingCollection<ISerializedReceiveInfo> receives, ConcurrentQueue<RequestGenerator> workItems, Communicator comm)
        {
            Thread worker = new Thread(new ThreadStart(() =>
            {
                using (EventWaitHandle waitHandle = new AutoResetEvent(false))
                {
                    bool done = false;
                    int source = default(int);
                    int tag = default(int);
                    Func<byte[], int> readAction = delegate (byte[] bytes)
                    {
                        if (done)
                            return 0;
                        int length = 0;
                        Action<int> setLength = value => { length = value; };
                        var workItem = new RequestGenerator(waitHandle, $"Receive length from {source}", () =>
                            comm.ImmediateReceive(source, tag, setLength)
                        );
                        workItems.Enqueue(workItem);
                        waitHandle.WaitOne();
                        if (length < comm.Serialization.BufferSize)
                            done = true;
                        if (length == 0)
                            return 0;
                        var workItem2 = new RequestGenerator(waitHandle, $"Receive {length} bytes from {source}", () =>
                            comm.ImmediateReceive(source, tag, bytes)
                        );
                        workItems.Enqueue(workItem2);
                        waitHandle.WaitOne();
                        return length;
                    };
                    using (var stream = new BufferedStream(new ActionStream(readAction, null), comm.Serialization.BufferSize))
                    {
                        while (true)
                        {
                            var info = receives.Take();
                            if (info == null)
                                break;
                            done = false;
                            source = info.Source;
                            tag = info.Tag;
                            info.Deserialize(comm.Serialization.Serializer, stream);
                            info.Completed();
                        }
                    }
                }
            }));
            worker.IsBackground = true;
            worker.Start();
            comm.Serialization.ReceiveWorkerCount++;
        }

        internal interface ISerializedSendInfo
        {
            int Destination { get; }
            int Tag { get; }
            Action Completed { get; }
            void Serialize(ISerializer serializer, Stream stream);
        }

        internal class SerializedSendInfo<T> : ISerializedSendInfo
        {
            readonly T value;
            public int Destination { get; }
            public int Tag { get; }
            public Action Completed { get; }

            public SerializedSendInfo(T value, int dest, int tag, Action completed)
            {
                this.value = value;
                this.Destination = dest;
                this.Tag = tag;
                this.Completed = completed;
            }

            public void Serialize(ISerializer serializer, Stream stream)
            {
                serializer.Serialize(stream, value);
            }
        }

        internal interface ISerializedReceiveInfo
        {
            int Source { get; }
            int Tag { get; }
            Action Completed { get; }
            void Deserialize(ISerializer serializer, Stream stream);
        }

        internal class SerializedReceiveInfo<T> : ISerializedReceiveInfo
        {
            Action<T> action;
            public int Source { get; }
            public int Tag { get; }
            public Action Completed { get; }

            public SerializedReceiveInfo(int source, int tag, Action<T> action, Action completed)
            {
                this.Source = source;
                this.Tag = tag;
                this.action = action;
                this.Completed = completed;
            }

            public void Deserialize(ISerializer serializer, Stream stream)
            {
                T value = serializer.Deserialize<T>(stream);
                action(value);
            }
        }
    }

    public interface ISerializer
    {
        void Serialize<T>(Stream stream, T value);
        T Deserialize<T>(Stream stream);
    }

    public class BinaryFormatterSerializer : ISerializer
    {
        public static readonly BinaryFormatterSerializer Default;

        static BinaryFormatterSerializer()
        {
            Default = new BinaryFormatterSerializer();
        }

        public T Deserialize<T>(Stream stream)
        {
            var formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            return (T)formatter.Deserialize(stream);
        }

        public void Serialize<T>(Stream stream, T value)
        {
            var formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
            formatter.Serialize(stream, value);
        }
    }
}
