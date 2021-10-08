using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class Int32ReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, RangeWithKey range) =>
            MemoryMarshal.Cast<int, byte>(((int[])a).AsSpan(range.From, range.Length)).ToArray();
    }
}