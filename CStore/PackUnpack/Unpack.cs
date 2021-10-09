using System;
using CStore.ReadWriteTypes;

namespace CStore
{
    static partial class PackUnpackExtenders
    {
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