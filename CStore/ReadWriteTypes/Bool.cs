using System;

namespace CStore.ReadWriteTypes
{
    sealed class BoolReaderWriter : BaseReaderWriter
    {
        static readonly byte[] byteMasks =
        {
            0b1111_1110,
            0b1111_1101,
            0b1111_1011,
            0b1111_0111,
            0b1110_1111,
            0b1101_1111,
            0b1011_1111,
            0b0111_1111,
        };

        internal override byte[] Pack(Array a, Range range)
        {
            var byteCount = range.Length() / 8 + (range.Length() % 8 > 0 ? 1 : 0);
            var buff      = new byte[byteCount];
            var bools     = (bool[])a;

            for (int i = range.Start.Value, index = 0; i < range.End.Value; i++, index++)
            {
                var byteIndex = index >> 3; // /8
                var bitIndex  = index % 8;

                var byteValue = (byte)(buff[byteIndex] & byteMasks[bitIndex]);

                if (bools[i])
                    byteValue |= (byte)(1 << bitIndex);

                buff[byteIndex] = byteValue;
            }

            return buff;
        }

        internal override Array Unpack(Span<byte> from, Range range)
        {
            var values = new bool[from.Length * 8];

            for (var i = 0; i < values.Length; i++)
                values[i] = (from[(i >> 3)] & (1 << (i % 8))) > 0;

            return values.AsSpan(range.Start.Value, range.Length()).ToArray();
        }
    }
}