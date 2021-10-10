using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    sealed partial class Int32ReaderWriter
    {
        #region packAsDictionary

        byte[] packAsDictionary(Array a, Range range)
        {
            var dictionarized = a.Dictionarize<int>(range);

            Span<byte> indexData;
            Span<byte> valueData;
            switch (dictionarized.KeyType)
            {
                case DictionaryKey.Byte:
                {
                    indexData = dictionarized.Indexes.CompactToByte();
                    valueData = dictionarized.Values.CompactToByte();
                    break;
                }

                case DictionaryKey.Short:
                {
                    indexData = dictionarized.Indexes.CompactToShort();
                    valueData = dictionarized.Values.CompactToShort();
                    break;
                }

                case DictionaryKey.Int:
                {
                    indexData = dictionarized.Indexes.CompactToInt();
                    valueData = dictionarized.Values.CompactToInt();
                    break;
                }

                default:
                    throw new NotSupportedException(dictionarized.KeyType.ToString());
            }

            var buff = new byte[(2 + 4 + 4 + indexData.Length + valueData.Length)];
            var span = buff.AsSpan();

            span[0] = (byte)CompactType.Dictionary;
            span[1] = (byte)dictionarized.KeyType;
            span    = span.Slice(2);

            BitConverter.TryWriteBytes(span, dictionarized.Indexes.Length);
            span = span.Slice(4);

            BitConverter.TryWriteBytes(span, dictionarized.Values.Length);
            span = span.Slice(4);

            indexData.CopyTo(span);
            valueData.CopyTo(span.Slice(indexData.Length));
            return buff;
        }

        #endregion

        #region unpackAsDictionary

        int[] unpackAsDictionary(Span<byte> span, Range range)
        {
            var keyType = (DictionaryKey)span[1];
            span = span.Slice(2);

            var keyCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            var valueCount = BitConverter.ToInt32(span);
            span = span.Slice(4);

            return keyType switch
            {
                DictionaryKey.Byte => span.UncompactByteToInt(keyCount, range),
                DictionaryKey.Short => span.UncompactShortToInt(keyCount, range),
                DictionaryKey.Int => span.UncompactIntToInt(keyCount, range),
                _ => throw new NotSupportedException(keyType.ToString())
            };
        }

        #endregion
    }
}