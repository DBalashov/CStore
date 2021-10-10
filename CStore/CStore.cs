using System;
using FileContainer;

namespace CStore
{
    public partial class ColumnStore : IDisposable
    {
        internal readonly PagedContainerAbstract c;
        internal readonly CDTUnit                unit;

        public ColumnStore(PagedContainerAbstract c, CDTUnit unit = CDTUnit.Month)
        {
            this.c    = c;
            this.unit = unit;
        }

        public void Dispose() => c.Dispose();
    }
}