using System;

namespace CStore.ReadWriteTypes
{
    abstract class BaseReaderWriter
    {
        internal abstract byte[] Pack(Array a, Range range);

        internal abstract Array Unpack(Span<byte> from, Range range);
    }

    public enum ColumnStoreType
    {
        Bool     = 0,
        Byte     = 1,
        Short    = 2,
        Int      = 3,
        Int64    = 4,
        Double   = 5,
        DateTime = 6,
        TimeSpan = 7,
        String   = 8,
        Guid     = 9,
    }

    public enum CompactType
    {
        None,
        Dictionary,
        RLE
    }

    public enum DictionaryKey
    {
        Byte,
        Short,
        Int
    }
}