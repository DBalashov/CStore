using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class Int64ReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, RangeWithKey range) =>
            MemoryMarshal.Cast<Int64, byte>(((Int64[])a).AsSpan(range.From, range.Length)).ToArray();
    }
}