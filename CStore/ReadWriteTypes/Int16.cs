using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class Int16ReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            var span = ((short[])a).AsSpan(range);

            var rle = span.RLElize();
            if (rle != null)
                return rle;

            if (span.CanBeDictionarize())
                return a.Dictionarize<short>(range).Compact().Combine();

            var buff = new byte[2 + span.Length * 4];
            buff[0] = (byte)CompactKind.None;
            buff[1] = 0;

            MemoryMarshal.Cast<short, byte>(span).CopyTo(buff.AsSpan(2));
            return buff;
        }

        internal override Array Unpack(Span<byte> from, Range range)
        {
            var compactType = (CompactKind)from[0];
            return compactType switch
            {
                CompactKind.Dictionary => from.UndictionarizeToInt(range),
                CompactKind.RLE => from.UnRLElize<short>(range),
                CompactKind.None => MemoryMarshal.Cast<byte, short>(from.Slice(2 + range.Start.Value * 2, range.Length() * 2)).ToArray(),
                _ => throw new NotSupportedException(compactType.ToString())
            };
        }
    }
}