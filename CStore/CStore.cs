using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using FileContainer;

namespace CStore
{
    public class ColumnStore : IDisposable
    {
        readonly PagedContainerAbstract c;

        public ColumnStore(PagedContainerAbstract c) => this.c = c;

        public void Dispose()
        {
            c.Dispose();
        }

        public void Update(string prefix, ColumnBatch item)
        {
            foreach (var range in item.DT.GetRange(CDTUnit.Month))
            {
                foreach (var col in item.Columns)
                {
                    var values = item[col].Pack(range);

                    var keyLengthInBytes = range.Length * 4;
                    var buff             = new byte[4 + keyLengthInBytes + values.Length];
                    var span             = buff.AsSpan();

                    BitConverter.TryWriteBytes(span, range.Length);
                    MemoryMarshal.Cast<CDT, byte>(item.DT.AsSpan(range.From, range.Length)).CopyTo(span.Slice(4));

                    values.CopyTo(span.Slice(4 + keyLengthInBytes));

                    var sectionName = buildSectionName(prefix, range.Key, col);
                    c.Put(sectionName, buff);
                }
            }
        }

        string buildSectionName(string prefix, CDT key, string col) => $"{prefix}/{col}/{key.ToString()}";
    }
}