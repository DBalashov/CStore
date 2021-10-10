using System;

namespace CStore.ReadWriteTypes
{ 
    sealed class StringReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            var r = a.Dictionarize(range, "");

            return null;
        }

        internal override Array Unpack(Span<byte> from, Range range) => throw new NotImplementedException();
    }
}