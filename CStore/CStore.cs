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
            foreach (var part in item.DT.GetRange(unit))
            {
                foreach (var col in item.Columns)
                {
                    var values      = new KeyValueArray<CDT>(item.DT, item[col]).Pack(part.Range);
                    var sectionName = buildSectionName(prefix, part.Key, col);
                    c.Put(sectionName, values);
                }
            }
        }

        public Dictionary<string, ColumnBatch> Read(string[] prefixes, string[] columnNames, DateTime sd, DateTime ed)
        {
            var r = new Dictionary<string, Dictionary<string, List<KeyValueArray<DateTime>>>>();
            foreach (var prefix in prefixes)
            {
                var parts = ((CDT)sd).GetKeyInRanges(ed, unit).ToArray();
                foreach (var columnName in columnNames)
                {
                    foreach (var part in parts)
                    {
                        var sectionName = buildSectionName(prefix, part.Key, columnName);
                        var sectionData = c.Get(sectionName);
                        if (sectionData == null) continue;

                        var unpacked = sectionData.Unpack(part.Range);

                        if (!r.TryGetValue(prefix, out var byPrefixes))
                            r.Add(prefix, byPrefixes = new Dictionary<string, List<KeyValueArray<DateTime>>>(StringComparer.InvariantCultureIgnoreCase));

                        if (!byPrefixes.TryGetValue(columnName, out var byColumns))
                            byPrefixes.Add(columnName, byColumns = new List<KeyValueArray<DateTime>>());

                        byColumns.Add(unpacked);
                    }
                }
            }

            return null;
        }

        string buildSectionName(string prefix, CDT key, string col) => $"{prefix}/{col}/{key.ToString()}";
    }
}