using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class DateTimeReaderWriter : BaseReaderWriter
    {
        static readonly ArrayPool<CDT> pool = ArrayPool<CDT>.Shared;

        internal override byte[] Pack(Array a, RangeWithKey range)
        {
            var values = (DateTime[])a;

            var data = pool.Rent(range.Length);
            for (int i = range.From, index = 0; i < range.To; i++, index++)
                data[index] = values[i];

            var r = MemoryMarshal.Cast<CDT, byte>(data.AsSpan(0, range.Length)).ToArray();
            pool.Return(data);
            return r;
        }
    }
}