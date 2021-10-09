using System;
using System.Runtime.InteropServices;

namespace CStore
{
    static partial class PackUnpackExtenders
    {
        public static byte[] Pack(this KeyValueArray<CDT> item, Range range)
        {
            // values:
            var columnType = DetectDataType(item.Values);
            if (!columnType.HasValue)
                throw new NotSupportedException("Not supported type: " + item.Values.GetValue(0)?.GetType());

            var readerWriter = readerWriters[columnType.Value];
            var values       = readerWriter.Pack(item.Values, range);

            // keys:
            var keyLengthInBytes = range.Length() * 4;
            var buff             = new byte[(4 + 4) + keyLengthInBytes + values.Length];
            var span             = buff.AsSpan();

            BitConverter.TryWriteBytes(span, range.Length()); // 4 bytes
            span[4] = (byte)columnType.Value;                 // 1 byte
            span[5] = 0;                                      // 1 byte (reserved)
            span[6] = 0;                                      // 1 byte (reserved)
            span[7] = 0;                                      // 1 byte (reserved)

            span = span.Slice(4 + 4);
            MemoryMarshal.Cast<CDT, byte>(item.Keys.AsSpan(range)).CopyTo(span);

            span = span.Slice(keyLengthInBytes);
            values.CopyTo(span);

            return buff;
        }
    }
}