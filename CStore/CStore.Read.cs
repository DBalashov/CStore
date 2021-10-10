using System;
using System.Collections.Generic;
using System.Linq;

namespace CStore
{
    public partial class ColumnStore
    {
        public Dictionary<string, Dictionary<string, KeyValueArray>> Read(string[] prefixes, string[] columnNames, DateTimeRange? range = null)
        {
            var r = new Dictionary<string, Dictionary<string, KeyValueArray>>(StringComparer.InvariantCultureIgnoreCase);
            columnNames = columnNames.Distinct(StringComparer.InvariantCultureIgnoreCase).ToArray();

            range ??= new DateTimeRange(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                                        new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            
            foreach (var prefix in prefixes.Distinct(StringComparer.InvariantCultureIgnoreCase))
            {
                var partitionKeys = range.Value.GetKeyInRanges(unit).ToArray();
                var prefixColumns = new Dictionary<string, KeyValueArray>();
                foreach (var columnName in columnNames)
                {
                    var accum = new KeyValueArrayAccumulator();
                    foreach (var part in partitionKeys)
                    {
                        var partitionName = part.Key.FormatPartitionName(prefix, columnName);

                        var partitionData = c.Get(partitionName);
                        if (partitionData == null) continue; // no partition

                        accum.Add(partitionData.Unpack(part.Range));
                    }

                    prefixColumns.Add(columnName, accum.Merge());
                }

                r.Add(prefix, prefixColumns);
            }

            return r;
        }
    }
}