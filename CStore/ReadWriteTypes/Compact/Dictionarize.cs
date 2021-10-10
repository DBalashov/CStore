﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace CStore.ReadWriteTypes
{
    static class DictionarizeExtenders
    {
        internal static bool CanBeDictionarize<T>(this Span<T> data)
        {
            if (data.Length < 32) return false; // маленькие массивы не имеет смысла превращать в dictionary

            var checkFirstN = data.Length / 3; // проверяем не все, а только первую треть элементов (этого достаточно для получения результата можно или нет сделать словарь) 

            var hs = new HashSet<T>(checkFirstN);
            for (var i = 0; i < checkFirstN; i++)
                hs.Add(data[i]);

            return hs.Count < checkFirstN / 2; // true, если после dictionarize получается по меньшей мере вдвое меньше элементов
        }

        internal static DictionarizeResult<T> Dictionarize<T>(this Array values, Range range, T nullReplaceTo = default!) where T : notnull
        {
            var v       = (T[])values;
            var r       = new Dictionary<T, int>(); // value : index
            var indexes = new int[range.Length()];
            for (var i = range.Start.Value; i < range.End.Value; i++)
            {
                var item = v[i] ?? nullReplaceTo;
                if (!r.TryGetValue(item, out var index))
                {
                    index = r.Count;
                    r.Add(item, index);
                }

                indexes[i - range.Start.Value] = index;
            }

            return new DictionarizeResult<T>(indexes, r.OrderBy(p => p.Value).Select(p => p.Key).ToArray());
        }

        internal static UncompactIndexesResult UncompactIndexes(this Span<byte> span, DictionaryKey keyType, int keyCount, Range range)
        {
            switch (keyType)
            {
                case DictionaryKey.Byte:
                {
                    var storedIndexes = span.Slice(0, keyCount)
                                            .Slice(range.Start.Value, range.End.Value);
                    var indexes = new int[storedIndexes.Length];
                    for (var i = 0; i < storedIndexes.Length; i++)
                        indexes[i] = storedIndexes[i];

                    return new UncompactIndexesResult(span.Slice(keyCount), indexes);
                }

                case DictionaryKey.Short:
                {
                    var storedIndexes = MemoryMarshal.Cast<byte, ushort>(span.Slice(0, keyCount * 2))
                                                     .Slice(range.Start.Value, range.End.Value);
                    var indexes = new int[storedIndexes.Length];
                    for (var i = 0; i < storedIndexes.Length; i++)
                        indexes[i] = storedIndexes[i];
                    return new UncompactIndexesResult(span.Slice(keyCount * 2), indexes);
                }

                case DictionaryKey.Int:
                {
                    var indexes = MemoryMarshal.Cast<byte, int>(span.Slice(0, keyCount * 4))
                                               .Slice(range.Start.Value, range.End.Value);
                    return new UncompactIndexesResult(span.Slice(keyCount * 4), indexes);
                }

                default: throw new NotSupportedException(keyType.ToString());
            }
        }
        
        internal static Span<byte> CompactIndexes<T>(this DictionarizeResult<T> r) =>
            r.KeyType switch
            {
                DictionaryKey.Byte => r.Indexes.CompactToByte(),
                DictionaryKey.Short => r.Indexes.CompactToShort(),
                DictionaryKey.Int => r.Indexes.CompactToInt(),
                _ => throw new NotSupportedException(r.KeyType.ToString())
            };
    }

    readonly struct DictionarizeResult<T>
    {
        public readonly int[]         Indexes;
        public readonly T[]           Values;
        public readonly DictionaryKey KeyType;

        internal DictionarizeResult(int[] indexes, T[] values)
        {
            Indexes = indexes;
            Values  = values;
            KeyType = indexes.Length switch
            {
                > 0 and <= 255 => DictionaryKey.Byte,
                >= 255 and <= 65535 => DictionaryKey.Short,
                _ => DictionaryKey.Int
            };
        }

        public override string ToString() => $"{Indexes.Length} -> {Values.Length} ({KeyType})";
    }

    readonly ref struct UncompactIndexesResult
    {
        public readonly Span<byte> Span;
        public readonly Span<int>  Indexes;

        internal UncompactIndexesResult(Span<byte> span, Span<int> indexes)
        {
            Span    = span;
            Indexes = indexes;
        }
    }
}