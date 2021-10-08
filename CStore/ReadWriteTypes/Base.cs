using System;
using System.Collections.Generic;

namespace CStore.ReadWriteTypes
{
    abstract class BaseReaderWriter
    {
        internal abstract byte[] Pack(Array a, RangeWithKey range);

        protected bool CanBeDictionarize<T>(Span<T> data)
        {
            if (data.Length < 32) return false; // маленькие массивы не имеет смысла превращать в dictionary

            var checkFirstN = data.Length / 3; // проверяем не все, а только первую треть элементов (этого достаточно для получения результата можно или нет сделать словарь) 

            var hs = new HashSet<T>(checkFirstN);
            for (var i = 0; i < checkFirstN; i++)
                hs.Add(data[i]);

            return hs.Count < data.Length / 2; // true, если после dictionarize получается по меньшей мере вдвое меньше элементов
        }
    }

    public enum ColumnStoreType
    {
        Byte     = 0,
        Short    = 1,
        Int      = 2,
        Int64    = 3,
        Double   = 4,
        DateTime = 5,
        TimeSpan = 6,
        String   = 7
    }
}