using System;
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
            
            foreach (var s in items)
            {
                var bytes = Encoding.UTF8.GetBytes(s);
                bw.Write((ushort)bytes.Length);
                bw.Write(bytes);
                length += bytes.Length + 2;
            }

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