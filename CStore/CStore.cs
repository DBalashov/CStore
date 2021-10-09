using System;
using System.Collections.Generic;
using System.Linq;
using FileContainer;

namespace CStore
{
    public class ColumnStore : IDisposable
    {
        readonly PagedContainerAbstract c;
        readonly CDTUnit                unit;

        public ColumnStore(PagedContainerAbstract c, CDTUnit unit = CDTUnit.Month)
        {
            this.c    = c;
            this.unit = unit;
        }

        public void Dispose()
        {
            c.Dispose();
        }

        public void Update(string prefix, ColumnBatch item)
        {
            foreach (var col in item.Columns)
            {
                var values = new KeyValueArray<CDT>(item.Keys, item[col]);
                foreach (var part in values.Keys.GetRange(unit))
                {
                    var data = values.Pack(part.Range);

                    var partitionName = buildPartitionName(prefix, part.Key, col);
                    c.Put(partitionName, data);
                }
            }
        }

        public Dictionary<string, Dictionary<string, KeyValueArray<DateTime>>> Read(string[] prefixes, string[] columnNames, DateTimeRange range)
        {
            var r = new Dictionary<string, Dictionary<string, KeyValueArray<DateTime>>>(StringComparer.InvariantCultureIgnoreCase);
            columnNames = columnNames.Distinct(StringComparer.InvariantCultureIgnoreCase).ToArray();

            foreach (var prefix in prefixes.Distinct(StringComparer.InvariantCultureIgnoreCase))
            {
                var partitionKeys = range.GetKeyInRanges(unit).ToArray();
                var prefixColumns = new Dictionary<string, KeyValueArray<DateTime>>();
                foreach (var columnName in columnNames)
                {
                    var accum = new KeyValueArrayAccumulator<DateTime>();
                    foreach (var part in partitionKeys)
                    {
                        var partitionName = buildPartitionName(prefix, part.Key, columnName);

                        var partitionData = c.Get(partitionName);
                        if (partitionData == null) continue; // no partition

                        accum.Add(partitionData.Unpack(part.Range));
                    }

                    var a = accum.Merge();
                    prefixColumns.Add(columnName, a);
                }

                r.Add(prefix, prefixColumns);
            }

            return r;
        }

        string buildPartitionName(string prefix, CDT key, string col) => $"{prefix}/{col}/{key.ToString()}";
    }
}