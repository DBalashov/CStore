using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class Int32ReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range) =>
            MemoryMarshal.Cast<int, byte>(((int[])a).AsSpan(range))
                         .ToArray();

        internal override Array Unpack(Span<byte> from, Range range) =>
            MemoryMarshal.Cast<byte, int>(from)
                         .Slice(range.Start.Value, range.Length())
                         .ToArray();
    }
}