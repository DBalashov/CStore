using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    static class ShrinkInt16Extenders
    {
        internal static CompactedResult Compact(this DictionarizeResult<short> r)
        {
            var values = MemoryMarshal.Cast<short, byte>(r.Values);
            return r.KeyType switch
            {
                CompactType.Byte => new CompactedResult(CompactType.Byte,
                                                          new CompactedResultItem(r.Indexes.CompactToByte(), r.Indexes.Length),
                                                          new CompactedResultItem(values, r.Values.Length)),
                CompactType.Short => new CompactedResult(CompactType.Short,
                                                           new CompactedResultItem(r.Indexes.CompactToShort(), r.Indexes.Length),
                                                           new CompactedResultItem(values, r.Values.Length)),
                _ => throw new NotSupportedException(r.KeyType.ToString())
            };
        }

        internal static short[] UndictionarizeToShort(this Span<byte> span, Range range)
        {
            var keyType = (CompactType)span[1];
            span = span.Slice(2);

            var keyCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            var valueCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            return keyType switch
            {
                CompactType.Byte => span.uncompactByteToShort(keyCount, range),
                CompactType.Short => span.uncompactShortToShort(keyCount, range),
                _ => throw new NotSupportedException(keyType.ToString())
            };
        }

        static short[] uncompactByteToShort(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = span.Slice(0, keyCount);
            var values  = MemoryMarshal.Cast<byte, short>(span.Slice(keyCount)).ToArray();

            var r = new short[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }

        static short[] uncompactShortToShort(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = MemoryMarshal.Cast<byte, ushort>(span.Slice(0, keyCount * 2)).ToArray();
            var values  = MemoryMarshal.Cast<byte, short>(span.Slice(keyCount * 2)).ToArray();

            var r = new short[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }
    }
}