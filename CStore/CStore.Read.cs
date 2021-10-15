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

            foreach (var prefix in prefixes.Distinct(StringComparer.InvariantCultureIgnoreCase))
            {
                var prefixColumns = range.HasValue
                    ? readWithRange(columnNames, prefix, range.Value)
                    : readAllPartitions(columnNames, prefix);

                r.Add(prefix, prefixColumns);
            }

            return r;
        }

        #region readWithRange

        Dictionary<string, KeyValueArray> readWithRange(string[] columnNames, string prefix, DateTimeRange range)
        {
            var prefixColumns = new Dictionary<string, KeyValueArray>();

            var partitionKeys = range.GetKeyInRanges(unit).ToArray();
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

            return prefixColumns;
        }

        #endregion

        #region readAllPartitions

        Dictionary<string, KeyValueArray> readAllPartitions(string[] columnNames, string prefix)
        {
            var prefixColumns = new Dictionary<string, KeyValueArray>();

            foreach (var columnName in columnNames)
            {
                var accum      = new KeyValueArrayAccumulator();
                var partitions = c.Find(prefix.FormatAllPartitions(columnName));
                foreach (var part in partitions.OrderBy(p => p.Name))
                {
                    var partitionData = c.Get(part.Name);
                    if (partitionData == null) continue; // no partition

                    accum.Add(partitionData.Unpack());
                }

                prefixColumns.Add(columnName, accum.Merge());
            }

            return prefixColumns;
        }

        #endregion
    }
}