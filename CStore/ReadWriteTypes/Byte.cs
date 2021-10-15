using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class ByteReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            var span = ((byte[])a).AsSpan(range);

            var rle = span.RLElize();
            if (rle != null)
                return rle;

            var buff = new byte[2 + span.Length * 4];
            buff[0] = (byte)CompactKind.None;
            buff[1] = 0;

            span.CopyTo(buff.AsSpan(2));
            return buff;
        }

        internal override Array Unpack(Span<byte> from, Range range)
        {
            var compactType = (CompactKind)from[0];
            return compactType switch
            {
                CompactKind.RLE => from.UnRLElize<int>(range),
                CompactKind.None => MemoryMarshal.Cast<byte, byte>(from.Slice(2))
                                                 .Slice(range.Start.Value, range.Length())
                                                 .ToArray(),
                _ => throw new NotSupportedException(compactType.ToString())
            };
        }
    }
}