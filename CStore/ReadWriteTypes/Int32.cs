using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed partial class Int32ReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            var span = ((int[])a).AsSpan(range);

            if (span.CanBeDictionarize())
                return packAsDictionary(a, range);

            var buff = new byte[2 + span.Length * 4];
            buff[0] = (byte)CompactType.None;
            buff[1] = 0;

            MemoryMarshal.Cast<int, byte>(span).CopyTo(buff.AsSpan(2));
            return buff;
        }

        internal override Array Unpack(Span<byte> from, Range range)
        {
            var compactType = (CompactType)from[0];
            return compactType switch
            {
                CompactType.Dictionary => unpackAsDictionary(from, range),
                _ => MemoryMarshal.Cast<byte, int>(from.Slice(2)).Slice(range.Start.Value, range.Length()).ToArray()
            };
        }
    }
}