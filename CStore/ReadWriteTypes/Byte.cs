using System;

namespace CStore.ReadWriteTypes
{
    sealed class ByteReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range) => 
            ((byte[])a).AsSpan(range).ToArray();

        internal override Array Unpack(Span<byte> from, Range range) => 
            from.Slice(range.Start.Value, range.Length()).ToArray();
    }
}