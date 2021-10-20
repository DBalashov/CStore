using System;
using System.Collections.Generic;
using System.Linq;

namespace CStore
{
    public partial class ColumnStore
    {
        public void Delete(string[] prefixes, string[]? columnNames = null, DateTimeRange? range = null)
        {
            prefixes    = prefixes.Distinct(StringComparer.InvariantCultureIgnoreCase).ToArray();
            columnNames = columnNames?.Distinct(StringComparer.InvariantCultureIgnoreCase).ToArray();

            string[] partitionKeys;

            if (range == null)
                partitionKeys = (columnNames == null
                    ? prefixes.Select(prefix => prefix.FormatAllColumnsAndPartitions())
                    : prefixes.SelectMany(prefix => columnNames.Select(prefix.FormatAllPartitions))).ToArray();
            else
            {
                if (columnNames == null)
                    partitionKeys = range.Value.GetKeyInRanges(unit) // todo +partial remove
                                         .SelectMany(r => prefixes.Select(prefix => r.Key.FormatPartitionName(prefix, "*")))
                                         .ToArray();
                else
                {
                    var l = new List<string>();
                    foreach (var r in range.Value.GetKeyInRanges(unit)) // todo +partial remove
                    foreach (var prefix in prefixes)
                        l.AddRange(columnNames.Select(columnName => r.Key.FormatPartitionName(prefix, columnName)));
                    partitionKeys = l.ToArray();
                }
            }

            c.Delete(partitionKeys);
        }
    }
}