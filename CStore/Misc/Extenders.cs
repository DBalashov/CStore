using System;

namespace CStore
{
    static class Extenders
    {
        internal static string FormatPartitionName(this CDT key, string prefix, string columnName) => $"{prefix}/{columnName}/{key.ToString()}";

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
                var (keys, values) = merge(newData.Keys, newData.Values, newDataRange,
                                           existingData.Keys, existingData.Values, new Range(0, existingData.Keys.Length));
                result = new KeyValueArray(keys, values).Pack(new Range(0, keys.Length));
            }
            else if (existingData.Keys[^1] < firstDTNew) // все существующие данные раньше новых -> дописываем существующие данные в начало
            {
                var (keys, values) = merge(existingData.Keys, existingData.Values, new Range(0, existingData.Keys.Length),
                                           newData.Keys, newData.Values, newDataRange);
                result = new KeyValueArray(keys, values).Pack(new Range(0, keys.Length));
            }
            else
            {
                var (keys, values) = merge(newData.Keys, newData.Values, newDataRange,
                                           existingData.Keys, existingData.Values, new Range(0, existingData.Keys.Length));
                Array.Sort(keys, values);
                result = new KeyValueArray(keys, values).Pack(new Range(0, keys.Length));
            }

            return result;
        }

        static (CDT[] Keys, Array Values) merge(CDT[] newKeys,      Array newValues,      Range newRange,
                                                CDT[] existingKeys, Array existingValues, Range existingRange)
        {
            var finalKeys   = new CDT[newRange.Length() + existingRange.Length()];
            var finalValues = Array.CreateInstance(existingValues.GetElementType(), finalKeys.Length);

            newKeys.AsSpan(newRange).CopyTo(finalKeys);
            existingKeys.AsSpan(existingRange).CopyTo(finalKeys.AsSpan(newRange.Start.Value));

            Array.Copy(newValues, newRange.Start.Value,
                       finalValues, 0, newRange.Length());
            Array.Copy(existingValues, existingRange.Start.Value,
                       finalValues, newRange.Length(), existingRange.Length());

            return (finalKeys, finalValues);
        }
    }
}