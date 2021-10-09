using System;
using System.Collections.Generic;
using System.Linq;

namespace CStore
{
    sealed class KeyValueArrayAccumulator<K>
    {
        int                             elementCount = 0;
        readonly List<KeyValueArray<K>> items        = new(12);

        public void Add(KeyValueArray<K> item)
        {
            if (item.Keys.Length == 0) return;

            if (items.Count > 0)
            {
                var collectedType = items[0].Values.GetValue(0)!.GetType();
                var newItemType   = item.Values.GetValue(0)!.GetType();
                if (collectedType != newItemType)
                    throw new ArrayTypeMismatchException($"Can't add different type (owned type: {collectedType}, added type: {newItemType}");
            }

            items.Add(item);
            elementCount += item.Keys.Length;
        }

        public KeyValueArray<K> Merge()
        {
            if (!items.Any())
                return new KeyValueArray<K>(Array.Empty<K>(), Array.Empty<object>());

            var keys   = new K[elementCount];
            var values = Array.CreateInstance(items[0].Values.GetValue(0)!.GetType(), elementCount);

            int offset = 0;
            foreach (var item in items)
            {
                item.Keys.CopyTo(keys, offset);
                item.Values.CopyTo(values, offset);
                offset += item.Keys.Length;
            }

            return new KeyValueArray<K>(keys, values);
        }

        public override string ToString() => $"Elements={elementCount}";
    }
}