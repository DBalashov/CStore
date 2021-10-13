using System;
using System.Collections.Generic;
using System.Linq;
using FileContainer;
using NUnit.Framework;

namespace CStore.Tests
{
    public abstract class Base
    {
        // protected readonly Random r = new(Guid.Empty.GetHashCode());

        protected DateTime SD = new(2021, 1, 1);
        protected DateTime ED = new(2022, 1, 1);

        protected ColumnStore GetStore() => new(new InMemoryContainer(new PersistentContainerSettings(256)), CDTUnit.Month);

        protected virtual DateTime[] GetKeys(int everyMinute = 10)
        {
            var keys = new List<DateTime>();

            var dt = SD;
            while (dt < ED)
            {
                keys.Add(dt);
                dt = dt.AddMinutes(everyMinute);
            }

            return keys.ToArray();
        }

        protected void CheckSingle(string prefix, ColumnBatch batch, string columnName, Dictionary<string, Dictionary<string, KeyValueArray>> r)
        {
            var data = r[prefix][columnName];
            Assert.IsNotNull(data, columnName);
            Assert.IsNotNull(data.Keys, columnName);
            Assert.IsNotNull(data.Values, columnName);
            Assert.IsTrue(data.Values.Length == data.Keys.Length, "Values/Keys: {0} / {1}", data.Values.Length, data.Keys.Length);

            CDT prev = default;
            for (var i = 0; i < data.Keys.Length; i++)
            {
                Assert.AreEqual(data.Keys[i], batch.Keys[i],
                                "{0} keys [{1}]: expected {2}, but {3}", columnName, i, batch.Keys[i], data.Keys[i]);

                if (i > 0)
                    Assert.Greater(data.Keys[i], prev, "{0} keys: prev {1}, current {2}", i, prev, data.Keys[i]);

                prev = data.Keys[i];
            }

            var original = batch[columnName];

            if (data.Values.GetValue(0) is double)
                for (var i = 0; i < data.Values.Length; i++)
                    Assert.AreEqual((double)original.GetValue(i)!, (double)data.Values.GetValue(i)!, 0.001,
                                    "{0} values [{1}]: expected {2}, but {3}", columnName, i, original.GetValue(i), data.Values.GetValue(i));
            else
                for (var i = 0; i < data.Values.Length; i++)
                    Assert.AreEqual(original.GetValue(i), data.Values.GetValue(i),
                                    "{0} values [{1}]: expected {2}, but {3}", columnName, i, original.GetValue(i), data.Values.GetValue(i));
        }
    }
}