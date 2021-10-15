using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class DoubleReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            var values = (double[])a;
            var data   = ArrayPool<float>.Shared.Rent(range.Length());

            for (int i = range.Start.Value, index = 0; i < range.End.Value; i++, index++)
                data[index] = (float)values[i];

            var r = MemoryMarshal.Cast<float, byte>(data.AsSpan(0, range.Length())).ToArray();
            ArrayPool<float>.Shared.Return(data);
            return r;
        }

        internal override Array Unpack(Span<byte> from, Range range)
        {
            var values = MemoryMarshal.Cast<byte, float>(from).Slice(range.Start.Value, range.Length());
            var dt     = new double[values.Length];
            for (var i = 0; i < values.Length; i++)
                dt[i] = values[i];
            return dt;
        }
    }
}