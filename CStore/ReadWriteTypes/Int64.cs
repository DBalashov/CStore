using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class Int64ReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range) =>
            MemoryMarshal.Cast<Int64, byte>(((Int64[])a).AsSpan(range)).ToArray();

        internal override Array Unpack(Span<byte> from, Range range) =>
            MemoryMarshal.Cast<byte, Int64>(from)
                         .Slice(range.Start.Value, range.Length())
                         .ToArray();
    }
}