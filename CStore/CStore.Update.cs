﻿using System;

namespace CStore
{
    public partial class ColumnStore
    {
        public void Update(string prefix, ColumnBatch item, PartitionUpdateMode mode = PartitionUpdateMode.Merge)
        {
            foreach (var col in item.Columns)
            {
                var values = new KeyValueArray(item.Keys, item[col]);
                foreach (var part in values.Keys.GetRange(unit))
                {
                    var partitionName = part.Key.FormatPartitionName(prefix, col);

                    var existingPartitionData = c.Get(partitionName);
                    var data = existingPartitionData == null || mode == PartitionUpdateMode.Replace
                        ? values.Pack(part.Range)
                        : values.Merge(part.Range, existingPartitionData.Unpack());

                    c.Put(partitionName, data);
                }
            }
        }
    }

    public enum PartitionUpdateMode
    {
        Merge,
        Replace
    }
}