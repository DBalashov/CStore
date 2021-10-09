using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using CStore.ReadWriteTypes;

namespace CStore
{
    static class Extenders
    {
        static readonly Dictionary<ColumnStoreType, BaseReaderWriter> readerWriters = new()
        {
            [ColumnStoreType.Byte]     = new ByteReaderWriter(),
            [ColumnStoreType.Short]    = new ShortReaderWriter(),
            [ColumnStoreType.Int]      = new Int32ReaderWriter(),
            [ColumnStoreType.Int64]    = new Int64ReaderWriter(),
            [ColumnStoreType.Double]   = new DoubleReaderWriter(),
            [ColumnStoreType.TimeSpan] = new TimeSpanReaderWriter(),
            [ColumnStoreType.DateTime] = new DateTimeReaderWriter(),
            [ColumnStoreType.String]   = new StringReaderWriter()
        };

        internal static ColumnStoreType? DetectDataType(this Array a)
        {
            var type = a.GetValue(0)?.GetType();
            if (type == null)
                throw new InvalidOperationException("Type can't be null");

            if (type == typeof(byte)) return ColumnStoreType.Byte;
            if (type == typeof(short)) return ColumnStoreType.Short;
            if (type == typeof(int)) return ColumnStoreType.Int;
            if (type == typeof(Int64)) return ColumnStoreType.Int64;
            if (type == typeof(double)) return ColumnStoreType.Double;
            if (type == typeof(TimeSpan)) return ColumnStoreType.TimeSpan;
            if (type == typeof(DateTime)) return ColumnStoreType.DateTime;

            return null;
        }

        public static byte[] Pack(this KeyValueArray<CDT> item, Range range)
        {
            // values:
            var columnType = DetectDataType(item.Values);
            if (!columnType.HasValue)
                throw new NotSupportedException("Not supported type: " + item.Values.GetValue(0)?.GetType());

            var readerWriter = readerWriters[columnType.Value];
            var values       = readerWriter.Pack(item.Values, range);

            // keys:
            var keyLengthInBytes = range.Length() * 4;
            var buff             = new byte[(4 + 4) + keyLengthInBytes + values.Length];
            var span             = buff.AsSpan();

            BitConverter.TryWriteBytes(span, range.Length()); // 4 bytes
            span[4] = (byte)columnType.Value;                 // 1 byte
            span[5] = 0;                                      // 1 byte (reserved)
            span[6] = 0;                                      // 1 byte (reserved)
            span[7] = 0;                                      // 1 byte (reserved)

            span = span.Slice(4 + 4);
            MemoryMarshal.Cast<CDT, byte>(item.Keys.AsSpan(range)).CopyTo(span);

            span = span.Slice(keyLengthInBytes);
            values.CopyTo(span);

            return buff;
        }

        public static KeyValueArray<DateTime> Unpack(this byte[] from, DateTimeRange? range = null)
        {
            var span  = from.AsSpan();
            var count = BitConverter.ToInt32(span);
            span = span.Slice(4);

            var dataType = (ColumnStoreType)span[0];
            span = span.Slice(4);

            var keys = (DateTime[])readerWriters[ColumnStoreType.DateTime].Unpack(span.Slice(0, count * 4), new Range(0, count));
            span = span.Slice(count * 4);

            var valueRange = range?.GetRange(keys);
            var values     = readerWriters[dataType].Unpack(span, valueRange ?? new Range(0, count));
            return new KeyValueArray<DateTime>(valueRange == null
                                                   ? keys
                                                   : keys.AsSpan(valueRange.Value).ToArray(),
                                               values);
        }
    }
}