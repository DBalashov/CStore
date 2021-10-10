using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class GuidReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range) =>
            MemoryMarshal.Cast<Guid, byte>(((Guid[])a).AsSpan(range)).ToArray();

        internal override Array Unpack(Span<byte> from, Range range) =>
            MemoryMarshal.Cast<byte, Guid>(from)
                         .Slice(range.Start.Value, range.Length())
                         .ToArray();
    }
}