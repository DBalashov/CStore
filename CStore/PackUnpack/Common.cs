using System;
using System.Collections.Generic;
using CStore.ReadWriteTypes;

namespace CStore
{
    static partial class PackUnpackExtenders
    {
        static readonly Dictionary<ColumnStoreType, BaseReaderWriter> readerWriters = new()
        {
            [ColumnStoreType.Bool]     = new BoolReaderWriter(),
            [ColumnStoreType.Byte]     = new ByteReaderWriter(),
            [ColumnStoreType.Short]    = new ShortReaderWriter(),
            [ColumnStoreType.Int]      = new Int32ReaderWriter(),
            [ColumnStoreType.Int64]    = new Int64ReaderWriter(),
            [ColumnStoreType.Double]   = new DoubleReaderWriter(),
            [ColumnStoreType.TimeSpan] = new TimeSpanReaderWriter(),
            [ColumnStoreType.DateTime] = new DateTimeReaderWriter(),
            [ColumnStoreType.String]   = new StringReaderWriter(),
            [ColumnStoreType.Guid]     = new GuidReaderWriter()
        };

        internal static ColumnStoreType? DetectDataType(this Array a)
        {
            var type = a.GetElementType();
            
            if (type == typeof(bool)) return ColumnStoreType.Bool;
            if (type == typeof(byte)) return ColumnStoreType.Byte;
            if (type == typeof(short)) return ColumnStoreType.Short;
            if (type == typeof(int)) return ColumnStoreType.Int;
            if (type == typeof(Int64)) return ColumnStoreType.Int64;
            if (type == typeof(double)) return ColumnStoreType.Double;
            if (type == typeof(TimeSpan)) return ColumnStoreType.TimeSpan;
            if (type == typeof(DateTime)) return ColumnStoreType.DateTime;
            if (type == typeof(Guid)) return ColumnStoreType.Guid;
            if (type == typeof(string)) return ColumnStoreType.String;

            return null;
        }
    }
}