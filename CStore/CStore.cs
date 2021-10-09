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

        public Dictionary<string, Dictionary<string, KeyValueArray<DateTime>>> Read(string[] prefixes, string[] columnNames, DateTime sd, DateTime ed)
        {
            var r = new Dictionary<string, Dictionary<string, KeyValueArray<DateTime>>>(StringComparer.InvariantCultureIgnoreCase);
            columnNames = columnNames.Distinct(StringComparer.InvariantCultureIgnoreCase).ToArray();
            
            foreach (var prefix in prefixes.Distinct(StringComparer.InvariantCultureIgnoreCase))
            {
                var parts = ((CDT)sd).GetKeyInRanges(ed, unit).ToArray();

                var prefixColumns = new Dictionary<string, KeyValueArray<DateTime>>();
                foreach (var columnName in columnNames)
                {
                    var accum = new KeyValueArrayAccumulator<DateTime>();
                    foreach (var part in parts)
                    {
                        var sectionName = buildSectionName(prefix, part.Key, columnName);
                        var sectionData = c.Get(sectionName);
                        if (sectionData == null) continue;

                        var unpacked = sectionData.Unpack(part.Range);
                        accum.Add(unpacked);
                    }

                    var a = accum.Merge();
                    prefixColumns.Add(columnName, a);
                }
                
                r.Add(prefix, prefixColumns);
            }

            return r;
        }

        string buildSectionName(string prefix, CDT key, string col) => $"{prefix}/{col}/{key.ToString()}";
    }
}