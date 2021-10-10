using System;
using System.Collections.Generic;
using System.Linq;

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
}