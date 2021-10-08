using System;

namespace CStore.ReadWriteTypes
{
    sealed class ByteReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, RangeWithKey range) => 
            ((byte[])a).AsSpan(range.From, range.Length).ToArray();
    }
}