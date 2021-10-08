using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{ 
    sealed class ShortReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, RangeWithKey range) => 
            MemoryMarshal.Cast<short, byte>(((short[])a).AsSpan(range.From, range.Length)).ToArray();
    }
}