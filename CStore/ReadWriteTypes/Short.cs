using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{ 
    sealed class ShortReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range) => 
            MemoryMarshal.Cast<short, byte>(((short[])a).AsSpan(range)).ToArray();

        internal override Array Unpack(Span<byte> from, Range range) =>
            MemoryMarshal.Cast<byte, short>(from)
                         .Slice(range.Start.Value, range.Length())
                         .ToArray();
    }
}