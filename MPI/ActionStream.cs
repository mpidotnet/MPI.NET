using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MPI
{
    public class ActionStream : Stream
    {
        Func<byte[],int> readAction;
        Action<byte[]> writeAction;
        Action flushAction;

        public ActionStream(Func<byte[],int> readAction, Action<byte[]> writeAction, Action flushAction = null)
        {
            this.readAction = readAction;
            this.writeAction = writeAction;
            this.flushAction = flushAction;
        }

        public override bool CanRead
        {
            get
            {
                return (readAction != null);
            }
        }

        public override bool CanSeek
        {
            get
            {
                return false;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return (writeAction != null);
            }
        }

        public override long Length
        {
            get
            {
                throw new NotImplementedException("get Length");
            }
        }

        public override long Position
        {
            get
            {
                throw new NotImplementedException("get Position");
            }

            set
            {
                throw new NotImplementedException("set Position");
            }
        }

        public override void Flush()
        {
            flushAction?.Invoke();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            //Console.WriteLine($"called Read({offset}, {count})");
            if (offset != 0)
                throw new ArgumentException("offset != 0");
            if (count != buffer.Length)
                throw new ArgumentException("count != buffer.Length");
            return readAction(buffer);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException("Seek");
        }

        public override void SetLength(long value)
        {
        }

        public override void Write(byte[] array, int offset, int count)
        {
            //Console.WriteLine($"called Write({offset}, {count})");
            if (offset != 0) throw new ArgumentException("offset != 0");
            if (count != array.Length)
            {
                writeAction(array.Take(count).ToArray());
            }
            else
            {
                writeAction(array);
            }
        }

        public override void Close()
        {
            Flush();
        }
    }
}
