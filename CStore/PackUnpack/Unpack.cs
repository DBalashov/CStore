using System;
using CStore.ReadWriteTypes;

namespace CStore
{
    static partial class PackUnpackExtenders
    {
        public static KeyValueArray Unpack(this byte[] from, DateTimeRange? range = null)
        {
            var span  = from.AsSpan();
            var count = BitConverter.ToInt32(span);
            span = span.Slice(4);

            var dataType = (ColumnStoreType)span[0];
            span = span.Slice(4);

            var keys = span.Slice(0, count * 4).UnpackKeys();
            if (keys.Length != count)
                throw new InvalidOperationException($"Corrupted keys: actual length={keys.Length}, expected={count}");

            var valueRange = range?.GetRange(keys) ?? new Range(0, count);
            var values     = readerWriters[dataType].Unpack(span.Slice(count * 4), valueRange);

            var sliceOfKeys = keys.AsSpan(valueRange).ToArray();

            if (values.Length != sliceOfKeys.Length)
                throw new InvalidOperationException($"Corrupted values: actual length={values.Length}, expected={sliceOfKeys.Length}");

            return new KeyValueArray(sliceOfKeys, values);
        }
    }
}