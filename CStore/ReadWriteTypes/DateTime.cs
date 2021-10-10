using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class DateTimeReaderWriter : BaseReaderWriter
    {
        static readonly ArrayPool<CDT> pool = ArrayPool<CDT>.Shared;

        internal override byte[] Pack(Array a, Range range)
        {
            var values = (DateTime[])a;
            var data   = pool.Rent(range.Length());
            
            for (int i = range.Start.Value, index = 0; i < range.End.Value; i++, index++)
                data[index] = values[i];

            var r = MemoryMarshal.Cast<CDT, byte>(data.AsSpan(0, range.Length())).ToArray();
            pool.Return(data);
            return r;
        }

        internal override Array Unpack(Span<byte> from, Range range)
        {
            var values = MemoryMarshal.Cast<byte, CDT>(from).Slice(range.Start.Value, range.Length());
            var dt     = new DateTime[values.Length];
            for (var i = 0; i < values.Length; i++)
                dt[i] = values[i];
            return dt;
        }
    }
}