using System;
using System.Collections.Generic;
using CStore.ReadWriteTypes;

namespace CStore
{
    static class Extenders
    {
        static readonly Dictionary<Type, BaseReaderWriter> readerWriters = new()
        {
            [typeof(byte)]     = new ByteReaderWriter(),
            [typeof(short)]    = new ShortReaderWriter(),
            [typeof(int)]      = new Int32ReaderWriter(),
            [typeof(Int64)]    = new Int64ReaderWriter(),
            [typeof(double)]   = new DoubleReaderWriter(),
            [typeof(TimeSpan)] = new TimeSpanReaderWriter(),
            [typeof(DateTime)] = new DateTimeReaderWriter()
        };

        public static byte[] Pack(this Array a, RangeWithKey range)
        {
            var type = a.GetValue(0)?.GetType();
            if (type == null)
                throw new InvalidOperationException("Type can't be null");

            if (!readerWriters.TryGetValue(type, out var readerWriter))
                throw new NotSupportedException("Not supported type: " + type);

            return readerWriter.Pack(a, range);
        }
    }
}