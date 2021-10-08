using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class TimeSpanReaderWriter : BaseReaderWriter
    {
        static readonly ArrayPool<int> pool = ArrayPool<int>.Shared;

        internal override byte[] Pack(Array a, RangeWithKey range)
        {
            var values = (TimeSpan[])a;
            var data   = pool.Rent(range.Length);

            for (int i = range.From, index = 0; i < range.To; i++, index++)
                data[index] = (int)values[i].TotalSeconds;

            var r = MemoryMarshal.Cast<int, byte>(data.AsSpan(0, range.Length)).ToArray();
            pool.Return(data);
            return r;
        }
    }
}