using System;

namespace CStore.ReadWriteTypes
{ 
    sealed class StringReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, RangeWithKey range)
        {
            throw new NotImplementedException();
        }
    }
}