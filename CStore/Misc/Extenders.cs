using System;
using System.Collections.Generic;
using System.Linq;

namespace CStore
{
    static class Extenders
    {
        internal static string FormatPartitionName(this CDT key, string prefix, string columnName) => $"{prefix}/{columnName}/{key.ToString()}";

        internal static string FormatAllPartitions(this string prefix, string columnName) => $"{prefix}/{columnName}/*";
        
        internal static string FormatAllColumnsAndPartitions(this string prefix) => $"{prefix}/*";

        internal static Type GetElementType(this Array a) =>
            a.Length == 0 ? throw new ArgumentException("Array is empty & element type unknown") : a.GetValue(0)!.GetType();

        internal static byte[] Merge(this KeyValueArray newData, Range newDataRange, KeyValueArray existingData)
        {
            var newDataKeys = newData.Keys.AsSpan(newDataRange);
            var firstDTNew  = newDataKeys[0];
            var lastDTNew   = newDataKeys[^1];

            byte[] result;

            if (existingData.Keys[0] > lastDTNew) // все существующие данные позже новых -> дописываем существующие данные в конец
            {
                var kva = merge(newData, newDataRange,
                                existingData, new Range(0, existingData.Keys.Length));
                result = kva.Pack(new Range(0, kva.Keys.Length));
            }
            else if (existingData.Keys[^1] < firstDTNew) // все существующие данные раньше новых -> дописываем существующие данные в начало
            {
                var kva = merge(existingData, new Range(0, existingData.Keys.Length),
                                newData, newDataRange);
                result = kva.Pack(new Range(0, kva.Keys.Length));
            }
            else
            {
                // todo optimize
                var r = new Dictionary<CDT, object>();

                for (var kindex = 0; kindex < existingData.Keys.Length; kindex++)
                    r[existingData.Keys[kindex]] = existingData.Values.GetValue(kindex)!;
                    
                for (var kindex = 0; kindex < newDataKeys.Length; kindex++)
                    r[newDataKeys[kindex]] = newData.Values.GetValue(newDataRange.Start.Value + kindex)!;
                
                var finalValues = Array.CreateInstance(newData.Values.GetElementType(), r.Count);
                Array.Copy(r.Values.ToArray(), finalValues, finalValues.Length);

                result = new KeyValueArray(r.Keys.ToArray(), finalValues).Pack(new Range(0, r.Keys.Count));
            }

            return result;
        }

        static KeyValueArray merge(KeyValueArray newData,      Range newRange,
                                   KeyValueArray existingData, Range existingRange)
        {
            var finalKeys   = new CDT[newRange.Length() + existingRange.Length()];
            var finalValues = Array.CreateInstance(existingData.Values.GetElementType(), finalKeys.Length);

            newData.Keys.AsSpan(newRange).CopyTo(finalKeys);
            existingData.Keys.AsSpan(existingRange).CopyTo(finalKeys.AsSpan(newRange.Start.Value));

            Array.Copy(newData.Values, newRange.Start.Value,
                       finalValues, 0, newRange.Length());
            Array.Copy(existingData.Values, existingRange.Start.Value,
                       finalValues, newRange.Length(), existingRange.Length());

            return new KeyValueArray(finalKeys, finalValues);
        }
    }
}