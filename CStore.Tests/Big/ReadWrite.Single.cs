using System;

namespace CStore.Tests
{
    public class ReadWriteSingleBig : ReadWriteSingle
    {
        protected override DateTime[] GetKeys(int everyMinute) => base.GetKeys(5);
    }
}