using System;
using System.Buffers;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class DoubleReaderWriter : BaseReaderWriter
    {
        static readonly ArrayPool<float> pool = ArrayPool<float>.Shared;

        internal override byte[] Pack(Array a, RangeWithKey range)
        {
            var values = (double[])a;
            var data   = pool.Rent(range.Length);

            for (int i = range.From, index = 0; i < range.To; i++, index++)
                data[index] = (float)values[i];

            var r = MemoryMarshal.Cast<float, byte>(data.AsSpan(0, range.Length)).ToArray();
            pool.Return(data);
            return r;
        }
    }
}