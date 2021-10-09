using System;

namespace CStore.ReadWriteTypes
{ 
    sealed class StringReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            throw new NotImplementedException();
        }

        internal override Array Unpack(Span<byte> from, Range range) => throw new NotImplementedException();
    }
}