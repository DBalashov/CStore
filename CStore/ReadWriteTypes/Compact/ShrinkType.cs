using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    readonly ref struct CompactedResult
    {
        internal readonly DictionaryKey KeyType;

        internal readonly Span<byte> Indexes;
        internal readonly int        IndexesElements;

        internal readonly Span<byte> Values;
        internal readonly int        ValuesElements;

        internal CompactedResult(DictionaryKey keyType,
                                 Span<byte>    indexes, int indexesElements,
                                 Span<byte>    values,  int valuesElements)
        {
            KeyType = keyType;

            Indexes         = indexes;
            IndexesElements = indexesElements;

            Values         = values;
            ValuesElements = valuesElements;
        }
    }

    static class ShrinkTypeExtenders
    {
        #region Combine

        internal static byte[] Combine(this CompactedResult packed)
        {
            var buff = new byte[(2 + 4 + 4 + packed.Indexes.Length + packed.Values.Length)];
            var span = buff.AsSpan();

            span[0] = (byte)CompactType.Dictionary;
            span[1] = (byte)packed.KeyType;
            span    = span.Slice(2);

            BitConverter.TryWriteBytes(span, packed.IndexesElements);
            span = span.Slice(4);

            BitConverter.TryWriteBytes(span, packed.ValuesElements);
            span = span.Slice(4);

            packed.Indexes.CopyTo(span);
            packed.Values.CopyTo(span.Slice(packed.Indexes.Length));

            return buff;
        }

        #endregion

        #region Compact (Int)

        internal static CompactedResult Compact(this DictionarizeResult<int> r)
        {
            var values = MemoryMarshal.Cast<int, byte>(r.Values);
            return r.KeyType switch
            {
                DictionaryKey.Byte => new CompactedResult(DictionaryKey.Byte,
                                                          r.Indexes.CompactToByte(), r.Indexes.Length,
                                                          values, r.Values.Length),
                DictionaryKey.Short => new CompactedResult(DictionaryKey.Short,
                                                           r.Indexes.CompactToShort(), r.Indexes.Length,
                                                           values, r.Values.Length),
                DictionaryKey.Int => new CompactedResult(DictionaryKey.Int,
                                                         r.Indexes.CompactToInt(), r.Indexes.Length,
                                                         values, r.Values.Length),
                _ => throw new NotSupportedException(r.KeyType.ToString())
            };
        }
        
        #endregion

        #region Compact (Int64)

        internal static CompactedResult Compact(this DictionarizeResult<Int64> r)
        {
            var values = MemoryMarshal.Cast<Int64, byte>(r.Values);
            return r.KeyType switch
            {
                DictionaryKey.Byte => new CompactedResult(DictionaryKey.Byte,
                                                          r.Indexes.CompactToByte(), r.Indexes.Length,
                                                          values, r.Values.Length),
                DictionaryKey.Short => new CompactedResult(DictionaryKey.Short,
                                                           r.Indexes.CompactToShort(), r.Indexes.Length,
                                                           values, r.Values.Length),
                DictionaryKey.Int => new CompactedResult(DictionaryKey.Int,
                                                         r.Indexes.CompactToInt(), r.Indexes.Length,
                                                         values, r.Values.Length),
                _ => throw new NotSupportedException(r.KeyType.ToString())
            };
        }

        #endregion

        #region Compact (Int -> Short, Int -> Byte)

        internal static Span<byte> CompactToShort(this int[] data)
        {
            var target = new ushort[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (ushort)(data[i] & 0xFFFF);
            return MemoryMarshal.Cast<ushort, byte>(target);
        }

        internal static Span<byte> CompactToByte(this int[] data)
        {
            var target = new byte[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (byte)(data[i] & 0xFF);
            return target;
        }

        #endregion

        internal static Span<byte> CompactToInt(this int[] data) => MemoryMarshal.Cast<int, byte>(data);

        #region Uncompact (Int)

        internal static int[] UndictionarizeToInt(this Span<byte> span, Range range)
        {
            var keyType = (DictionaryKey)span[1];
            span = span.Slice(2);

            var keyCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            var valueCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            return keyType switch
            {
                DictionaryKey.Byte => span.UncompactByteToInt(keyCount, range),
                DictionaryKey.Short => span.UncompactShortToInt(keyCount, range),
                DictionaryKey.Int => span.UncompactIntToInt(keyCount, range),
                _ => throw new NotSupportedException(keyType.ToString())
            };
        }

        static int[] UncompactByteToInt(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = span.Slice(0, keyCount);
            var values  = MemoryMarshal.Cast<byte, int>(span.Slice(keyCount)).ToArray();

            var r = new int[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }

        static int[] UncompactShortToInt(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = MemoryMarshal.Cast<byte, ushort>(span.Slice(0, keyCount * 2)).ToArray();
            var values  = MemoryMarshal.Cast<byte, int>(span.Slice(keyCount * 2)).ToArray();

            var r = new int[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }

        static int[] UncompactIntToInt(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = MemoryMarshal.Cast<byte, int>(span.Slice(0, keyCount * 4)).ToArray();
            var values  = MemoryMarshal.Cast<byte, int>(span.Slice(keyCount * 4)).ToArray();

            var r = new int[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }

        #endregion

        #region Uncompact (Int64)

        internal static Int64[] UndictionarizeToInt64(this Span<byte> span, Range range)
        {
            var keyType = (DictionaryKey)span[1];
            span = span.Slice(2);

            var keyCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            var valueCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            return keyType switch
            {
                DictionaryKey.Byte => span.UncompactByteToInt64(keyCount, range),
                DictionaryKey.Short => span.UncompactShortToInt64(keyCount, range),
                DictionaryKey.Int => span.UncompactIntToInt64(keyCount, range),
                _ => throw new NotSupportedException(keyType.ToString())
            };
        }

        static Int64[] UncompactByteToInt64(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = span.Slice(0, keyCount);
            var values  = MemoryMarshal.Cast<byte, Int64>(span.Slice(keyCount)).ToArray();

            var r = new Int64[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }

        static Int64[] UncompactShortToInt64(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = MemoryMarshal.Cast<byte, ushort>(span.Slice(0, keyCount * 2)).ToArray();
            var values  = MemoryMarshal.Cast<byte, Int64>(span.Slice(keyCount * 2)).ToArray();

            var r = new Int64[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];

            return r;
        }

        static Int64[] UncompactIntToInt64(this Span<byte> span, int keyCount, Range range)
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