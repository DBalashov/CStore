using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class Int64ReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            var span = ((Int64[])a).AsSpan(range);

            if (span.CanBeDictionarize())
                return a.Dictionarize<Int64>(range).Compact().Combine();

            var buff = new byte[2 + span.Length * 8];
            buff[0] = (byte)CompactType.None;
            buff[1] = 0;

            MemoryMarshal.Cast<Int64, byte>(span).CopyTo(buff.AsSpan(2));
            return buff;
        }

        internal override Array Unpack(Span<byte> from, Range range)
        {
            var compactType = (CompactType)from[0];
            return compactType switch
            {
                CompactType.Dictionary => from.UndictionarizeToInt64(range),
                _ => MemoryMarshal.Cast<byte, Int64>(from.Slice(2))
                                  .Slice(range.Start.Value, range.Length())
                                  .ToArray()
            };
        }
    }
}