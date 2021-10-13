using System;

namespace CStore.Tests
{
    public class ReadWriteSingleSmall : ReadWriteSingle
    {
        protected override DateTime[] GetKeys(int everyMinute) => base.GetKeys(60);
    }
}