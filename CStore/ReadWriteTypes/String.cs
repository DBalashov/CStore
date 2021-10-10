using System;
using System.IO;

namespace CStore.ReadWriteTypes
{
    sealed class StringReaderWriter : BaseReaderWriter
    {
        internal override byte[] Pack(Array a, Range range)
        {
            using var stm = new MemoryStream();
            using var bw  = new BinaryWriter(stm);

            var span = ((string[])a).AsSpan(range);
            if (!span.CanBeDictionarize())
            {
                bw.Write((byte)CompactType.None);
                bw.WriteStrings(span);
            }
            else
            {
                var r = a.Dictionarize(range, "");

                bw.Write((byte)CompactType.Dictionary);
                bw.Write((byte)r.KeyType);

                bw.Write(r.Indexes.Length);
                var indexData = r.CompactIndexes();
                bw.Write(indexData);

                bw.WriteStrings(r.Values);
            }

            bw.Flush();

            return stm.ToArray();
        }

        internal override Array Unpack(Span<byte> span, Range range)
        {
            var compactType = (CompactType)span[0];
            switch (compactType)
            {
                case CompactType.None:
                    return span.Slice(1).ReadStrings()
                               .AsSpan(range)
                               .ToArray();

                case CompactType.Dictionary:
                {
                    var keyType  = (DictionaryKey)span[1];
                    var keyCount = BitConverter.ToInt32(span.Slice(2));

                    var r    = span.Slice(2 + 4).UncompactIndexes(keyType, keyCount, range);
                    var keys = r.Span.ReadStrings();

                    var result = new string[r.Indexes.Length];
                    for (var i = 0; i < r.Indexes.Length; i++)
                        result[i] = keys[r.Indexes[i]];
                    return result;
                }

                default: throw new NotSupportedException(compactType.ToString());
            }
        }
    }
}