using System;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    static class ShrinkTypeExtenders
    {
        #region Short -> Byte
        
        internal static Span<byte> CompactToByte(this ushort[] data)
        {
            var target = new byte[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (byte)(data[i] & 0xFF);
            return target.AsSpan();
        }
        
        #endregion
        
        #region Int -> Short/Byte

        internal static Span<byte> CompactToShort(this int[] data)
        {
            var target = new ushort[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (ushort)(data[i] & 0xFFFF);
            return MemoryMarshal.Cast<ushort, byte>(target);
        }

        internal static Span<byte> CompactToByte(this int[] data)
        {
            var target = new byte[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (byte)(data[i] & 0xFF);
            return target.AsSpan();
        }

        #endregion

        #region Int64 -> Int/Short/Byte

        internal static Span<byte> CompactToInt(this Int64[] data)
        {
            var target = new uint[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (uint)(data[i] & 0xFFFFFFFF);
            return MemoryMarshal.Cast<uint, byte>(target);
        }

        internal static Span<byte> CompactToShort(this Int64[] data)
        {
            var target = new ushort[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (ushort)(data[i] & 0xFFFF);
            return MemoryMarshal.Cast<ushort, byte>(target);
        }

        internal static Span<byte> CompactToByte(this Int64[] data)
        {
            var target = new byte[data.Length];
            for (var i = 0; i < data.Length; i++)
                target[i] = (byte)(data[i] & 0xFFFF);
            return target.AsSpan();
        }

        #endregion

        internal static Span<byte> CompactToInt(this int[] data) => MemoryMarshal.Cast<int, byte>(data);

        internal static int[] UncompactByteToInt(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = span.Slice(0, keyCount);
            var values = span.Slice(keyCount);

            var r = new int[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];
            
            return r;
        }
        
        internal static int[] UncompactShortToInt(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = MemoryMarshal.Cast<byte, ushort>(span.Slice(0, keyCount * 2)).ToArray();
            span = span.Slice(keyCount * 2);

            var values = MemoryMarshal.Cast<byte, ushort>(span).ToArray();

            var r = new int[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];
            
            return r;
        }
        
        internal static int[] UncompactIntToInt(this Span<byte> span, int keyCount, Range range)
        {
            var indexes = MemoryMarshal.Cast<byte, int>(span.Slice(0, keyCount * 4)).ToArray();
            span = span.Slice(keyCount * 4);

            var values = MemoryMarshal.Cast<byte, int>(span).ToArray();

            var r = new int[range.Length()];
            for (int i = range.Start.Value, offset = 0; i < range.End.Value; i++, offset++)
                r[offset] = values[indexes[i]];
            
            return r;
        }
    }
}