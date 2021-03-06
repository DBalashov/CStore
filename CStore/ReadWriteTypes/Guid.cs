using System;
using System.IO;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed class GuidReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            using var stm = new MemoryStream();
            using var bw  = new BinaryWriter(stm);

            var span = ((Guid[])a).AsSpan(range);
            if (!span.CanBeDictionarize())
            {
                bw.Write((byte)CompactKind.None);
                bw.Write(span.Length);
                bw.Write(MemoryMarshal.Cast<Guid, byte>(span));
            }
            else
            {
                var r = a.Dictionarize<Guid>(range);

                bw.Write((byte)CompactKind.Dictionary);
                bw.Write((byte)r.KeyType);

                bw.Write(r.Indexes.Length);
                var indexData = r.CompactIndexes();
                bw.Write(indexData);

                bw.Write(r.Values.Length);
                bw.Write(MemoryMarshal.Cast<Guid, byte>(r.Values));
            }

            bw.Flush();
            return stm.ToArray();
        }

        internal override Array Unpack(Span<byte> span, Range range)
        {
            var compactType = (CompactKind)span[0];
            switch (compactType)
            {
                case CompactKind.None:
                {
                    var keyCount = BitConverter.ToInt32(span.Slice(1));
                    return MemoryMarshal.Cast<byte, Guid>(span.Slice(1 + 4 + range.Start.Value * 16, range.Length() * 16)).ToArray();
                }

                case CompactKind.Dictionary:
                {
                    var keyType  = (CompactType)span[1];
                    var keyCount = BitConverter.ToInt32(span.Slice(2));
                    var r        = span.Slice(2 + 4).UncompactIndexes(keyType, keyCount, range);

                    var count = BitConverter.ToInt32(r.Span);
                    var keys  = MemoryMarshal.Cast<byte, Guid>(r.Span.Slice(4, count * 16));

                    var result = new Guid[r.Indexes.Length];
                    for (var i = 0; i < r.Indexes.Length; i++)
                        result[i] = keys[r.Indexes[i]];
                    return result;
                }

                default: throw new NotSupportedException(compactType.ToString());
            }
        }
    }
}