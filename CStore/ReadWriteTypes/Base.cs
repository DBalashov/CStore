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
        Byte     = 0,
        Short    = 1,
        Int      = 2,
        Int64    = 3,
        Double   = 4,
        DateTime = 5,
        TimeSpan = 6,
        String   = 7,
        Guid     = 8,
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