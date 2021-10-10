using System;
using System.Collections.Generic;
using System.Linq;

namespace CStore
{
    sealed class KeyValueArrayAccumulator
    {
        int                          elementCount = 0;
        readonly List<KeyValueArray> items        = new(12);

        public void Add(KeyValueArray item)
        {
            if (item.Keys.Length == 0) return;

            if (items.Count > 0)
            {
                var collectedType = items[0].Values.GetElementType();
                var newItemType   = item.Values.GetElementType();
                if (collectedType != newItemType)
                    throw new ArrayTypeMismatchException($"Can't add different type (owned type: {collectedType}, added type: {newItemType}");
            }

            items.Add(item);
            elementCount += item.Keys.Length;
        }

        public KeyValueArray Merge()
        {
            if (!items.Any())
                return new KeyValueArray(Array.Empty<CDT>(), Array.Empty<object>());

            var keys   = new CDT[elementCount];
            var values = Array.CreateInstance(items[0].Values.GetElementType(), elementCount);

            int offset = 0;
            foreach (var item in items)
            {
                item.Keys.CopyTo(keys, offset);
                item.Values.CopyTo(values, offset);
                offset += item.Keys.Length;
            }

            return new KeyValueArray(keys, values);
        }

        public override string ToString() => $"Elements={elementCount}";
    }
}