using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    static class RLElizeExtenders
    {
        #region detectElementSize

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static DictionaryKey detectElementSize(short b) =>
            b switch
            {
                <= 255 => DictionaryKey.Byte,
                _ => DictionaryKey.Short
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static DictionaryKey detectElementSize(int b) =>
            b switch
            {
                <= 255 => DictionaryKey.Byte,
                <= short.MaxValue => DictionaryKey.Short,
                _ => DictionaryKey.Int
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static DictionaryKey detectElementSize(Int64 b) =>
            b switch
            {
                <= 255 => DictionaryKey.Byte,
                <= short.MaxValue => DictionaryKey.Short,
                <= int.MaxValue => DictionaryKey.Int,
                _ => DictionaryKey.Int64
            };

        static DictionaryKey? detectElementSize<T>(this Span<T> values) where T : struct
        {
            if (values.Length == 0) return null;

            if (typeof(T) == typeof(byte)) return DictionaryKey.Byte;

            int elementSize = 1;
            if (typeof(T) == typeof(short))
                foreach (var b in MemoryMarshal.Cast<T, short>(values))
                {
                    elementSize = Math.Max(elementSize, (int)detectElementSize(b));
                    if (elementSize == 8) break;
                }
            else if (typeof(T) == typeof(int))
                foreach (var b in MemoryMarshal.Cast<T, int>(values))
                {
                    elementSize = Math.Max(elementSize, (int)detectElementSize(b));
                    if (elementSize == 8) break;
                }
            else if (typeof(T) == typeof(Int64))
                foreach (var b in MemoryMarshal.Cast<T, Int64>(values))
                {
                    elementSize = Math.Max(elementSize, (int)detectElementSize(b));
                    if (elementSize == 8) break;
                }
            else throw new NotSupportedException(typeof(T).ToString());

            return (DictionaryKey)elementSize;
        }

        #endregion

        #region writeElement<T>

        static void writeElement<T>(this Span<byte> spanOut, T value, int targetElementSize) where T : struct
        {
            switch (targetElementSize)
            {
                case 1:
                    spanOut[0] = Convert.ToByte(value);
                    break;

                case 2:
                    BitConverter.TryWriteBytes(spanOut, Convert.ToInt16(value));
                    break;

                case 4:
                    BitConverter.TryWriteBytes(spanOut, Convert.ToInt32(value));
                    break;

                case 8:
                    BitConverter.TryWriteBytes(spanOut, Convert.ToInt64(value));
                    break;
            }
        }

        #endregion

        #region RLElize<T>

        internal static byte[]? RLElize<T>(this Span<T> values) where T : struct, IComparable
        {
            if (values.Length == 0)
                return null;

            var targetElementSize = (int)(values.detectElementSize() ?? 0);
            if (targetElementSize == 0)
                return null;

            var buffOut = new byte[1 + 1 + 4 + values.Length * (targetElementSize + 1)];
            var spanOut = buffOut.AsSpan(1 + 1 + 4); // количество элементов в оригинальном массиве + размер одного элемента (в байтах - 1/2/4/8)

            var indexOut = 0;
            var indexIn  = 0;

            while (indexIn < values.Length)
            {
                if (indexOut * targetElementSize + 4 >= buffOut.Length)
                    return null; // результат упаковки по размеру получается больше оригинальных данных 

                var value = values[indexIn];

                var count = 0;
                while (indexIn < values.Length && values[indexIn].CompareTo(value) == 0 && count < 255)
                {
                    indexIn++;
                    count++;
                }

                spanOut[indexOut] = (byte)count;
                spanOut.Slice(indexOut + 1).writeElement(value, targetElementSize);
                indexOut += 1 + targetElementSize;
            }

            buffOut[0] = (byte)CompactType.RLE;
            buffOut[1] = (byte)targetElementSize;
            BitConverter.TryWriteBytes(buffOut.AsSpan(1 + 1), values.Length);

            Array.Resize(ref buffOut, 1 + 1 + 4 + indexOut);
            return buffOut;
        }

        #endregion

        #region UnRLElize<T>

        // ReSharper disable once HeapView.BoxingAllocation
        internal static T readElement<T>(this Span<byte> span, DictionaryKey actualElementType) where T : struct =>
            (T)Convert.ChangeType(actualElementType switch
            {
                DictionaryKey.Byte => span[0],
                DictionaryKey.Short => BitConverter.ToInt16(span),
                DictionaryKey.Int => BitConverter.ToInt32(span),
                DictionaryKey.Int64 => BitConverter.ToInt64(span),
                _ => throw new NotSupportedException(actualElementType.ToString())
            }, typeof(T));


        internal static T[] UnRLElize<T>(this Span<byte> span, Range range) where T : struct
        {
            if (span.Length <= 1 + 1 + 4)
                return Array.Empty<T>();

            var compactType       = (CompactType)span[0];
            var actualElementType = (DictionaryKey)span[1];
            var elementCount      = BitConverter.ToInt32(span.Slice(1 + 1));
            if (elementCount == 0)
                return Array.Empty<T>();

            span = span.Slice(1 + 1 + 4);

            var r       = new T[elementCount];
            var spanOut = r.AsSpan();

            while (!spanOut.IsEmpty)
            {
                var count   = span[0];
                var element = readElement<T>(span.Slice(1), actualElementType);
                spanOut.Slice(0, count).Fill(element);

                spanOut = spanOut.Slice(count);
                span    = span.Slice(1 + (int)actualElementType);
            }

            return r.AsSpan(range).ToArray();
        }

        #endregion
    }
}