using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class TimeSpanReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            var values = (TimeSpan[])a;
            var data   = ArrayPool<int>.Shared.Rent(range.Length());

            for (int i = range.Start.Value, index = 0; i < range.End.Value; i++, index++)
                data[index] = (int)values[i].TotalSeconds;

            var r = MemoryMarshal.Cast<int, byte>(data.AsSpan(0, range.Length())).ToArray();
            ArrayPool<int>.Shared.Return(data);
            return r;
        }

        internal override Array Unpack(Span<byte> from, Range range)
        {
            var values = MemoryMarshal.Cast<byte, int>(from).Slice(range.Start.Value, range.Length());
            var dt     = new TimeSpan[values.Length];
            for (var i = 0; i < values.Length; i++)
                dt[i] = TimeSpan.FromSeconds(values[i]);
            return dt;
        }
    }
}