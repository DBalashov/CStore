using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class Int32ReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            var span = ((int[])a).AsSpan(range);

            var rle = span.RLElize();
            if (rle != null)
                return rle;

            if (span.CanBeDictionarize())
                return a.Dictionarize<int>(range).Compact().Combine();

            var buff = new byte[2 + span.Length * 4];
            buff[0] = (byte)CompactKind.None;
            buff[1] = 0;

            MemoryMarshal.Cast<int, byte>(span).CopyTo(buff.AsSpan(2));
            return buff;
        }

        internal override Array Unpack(Span<byte> from, Range range)
        {
            var compactType = (CompactKind)from[0];
            return compactType switch
            {
                CompactKind.Dictionary => from.UndictionarizeToInt(range),
                CompactKind.RLE => from.UnRLElize<int>(range),
                CompactKind.None => MemoryMarshal.Cast<byte, int>(from.Slice(2 + range.Start.Value * 4, range.Length() * 4)).ToArray(),
                _ => throw new NotSupportedException(compactType.ToString())
            };
        }
    }
}