using System;
using System.Buffers;
using System.IO;
using System.Text;

namespace CStore.ReadWriteTypes
{
    static class Helpers
    {
        internal static int WriteStrings(this BinaryWriter bw, Span<string> items)
        {
            int length = 0;
            bw.Write(items.Length);

            var buff = ArrayPool<byte>.Shared.Rent(ushort.MaxValue);
            var span = buff.AsSpan(0, ushort.MaxValue);
            foreach (var s in items)
            {
                var bytesLength = Encoding.UTF8.GetBytes(s, span);

                bw.Write((ushort)bytesLength);
                bw.Write(span.Slice(0, bytesLength));
                length += bytesLength + 2;
            }

            ArrayPool<byte>.Shared.Return(buff);

            return length + 4;
        }

        internal static string[] ReadStrings(this Span<byte> span)
        {
            var count = BitConverter.ToInt32(span);
            span = span.Slice(4);

            var keys = new string[count];
            for (var i = 0; i < keys.Length; i++)
            {
                var length = BitConverter.ToUInt16(span);
                keys[i] = Encoding.UTF8.GetString(span.Slice(2, length));
                span    = span.Slice(2 + length);
            }

            return keys;
        }
    }
}