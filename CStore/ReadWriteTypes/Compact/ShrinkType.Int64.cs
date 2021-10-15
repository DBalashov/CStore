using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    static class ShrinkTypeExtenders
    {
        internal static CompactedResult Compact(this DictionarizeResult<Int64> r)
        {
            var values = MemoryMarshal.Cast<Int64, byte>(r.Values);
            return r.KeyType switch
            {
                CompactType.Byte => new CompactedResult(CompactType.Byte,
                                                        new CompactedResultItem(r.Indexes.CompactToByte(), r.Indexes.Length),
                                                        new CompactedResultItem(values, r.Values.Length)),
                CompactType.Short => new CompactedResult(CompactType.Short,
                                                         new CompactedResultItem(r.Indexes.CompactToShort(), r.Indexes.Length),
                                                         new CompactedResultItem(values, r.Values.Length)),
                CompactType.Int => new CompactedResult(CompactType.Int,
                                                       new CompactedResultItem(r.Indexes.CompactToInt(), r.Indexes.Length),
                                                       new CompactedResultItem(values, r.Values.Length)),
                _ => throw new NotSupportedException(r.KeyType.ToString())
            };
        }

        #region Uncompact (Int64)

        internal static Int64[] UndictionarizeToInt64(this Span<byte> span, Range range)
        {
            var keyType = (CompactType)span[1];
            span = span.Slice(2);

            var keyCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            var valueCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            return keyType switch
            {
                CompactType.Byte => span.uncompactByteToInt64(keyCount, range),
                CompactType.Short => span.uncompactShortToInt64(keyCount, range),
                CompactType.Int => span.uncompactIntToInt64(keyCount, range),
                _ => throw new NotSupportedException(keyType.ToString())
            };
        }

        static Int64[] uncompactByteToInt64(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = span.Slice(0, keyCount);
            var values  = MemoryMarshal.Cast<byte, Int64>(span.Slice(keyCount)).ToArray();

            var r = new Int64[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }

        static Int64[] uncompactShortToInt64(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = MemoryMarshal.Cast<byte, ushort>(span.Slice(0, keyCount * 2)).ToArray();
            var values  = MemoryMarshal.Cast<byte, Int64>(span.Slice(keyCount * 2)).ToArray();

            var r = new Int64[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }

        static Int64[] uncompactIntToInt64(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = MemoryMarshal.Cast<byte, int>(span.Slice(0, keyCount * 4)).ToArray();
            var values  = MemoryMarshal.Cast<byte, Int64>(span.Slice(keyCount * 4)).ToArray();

            var r = new Int64[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }

        #endregion
    }
}