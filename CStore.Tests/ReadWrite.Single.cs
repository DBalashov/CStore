using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace CStore.Tests
{
    public class ReadWriteSingle : Base
    {
        const string PREFIX = "/test";

        [SetUp]
        public void Setup()
        {
        }

        void executeSingle(ColumnBatch batch)
        {
            using var store = GetStore();

            store.Update(PREFIX, batch);

            TestContext.WriteLine("Keys / Columns       : {0} / {1}", batch.Keys.Length, string.Join(',', batch.Columns));
            TestContext.WriteLine("Length/Pages/PageSize: {0} / {1} / {2}\n", store.c.Length, store.c.TotalPages, store.c.PageSize);

            var r = store.Read(new[] { PREFIX }, batch.Columns);

            Assert.IsNotNull(r);
            Assert.IsNotEmpty(r);
            Assert.IsTrue(r.ContainsKey(PREFIX) && r.Count == 1);

            Assert.IsEmpty(r[PREFIX].Keys.Except(batch.Columns));

            CheckSingle(PREFIX, batch, batch.Columns[0], r);
        }

        [Test]
        public void WriteBools()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddBools());
            executeSingle(new ColumnBatch(GetKeys()).AddBools_HighEntropy());
        }
        
        [Test]
        public void WriteBytes()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddBytes());
            executeSingle(new ColumnBatch(GetKeys()).AddBytes_HighEntropy());
        }

        [Test]
        public void WriteInt16s()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddInt16s());
            executeSingle(new ColumnBatch(GetKeys()).AddInt16s_HighEntropy());
        }

        [Test]
        public void WriteInt32s()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddInt32s());
            executeSingle(new ColumnBatch(GetKeys()).AddInt32s_HighEntropy());
        }

        [Test]
        public void WriteInt64s()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddInt64s());
            executeSingle(new ColumnBatch(GetKeys()).AddInt64s_HighEntropy());
        }

        [Test]
        public void WriteDoubles()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddDoubles());
            executeSingle(new ColumnBatch(GetKeys()).AddDoubles_HighEntropy());
        }

        [Test]
        public void WriteTimeSpans()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddTimeSpans());
            executeSingle(new ColumnBatch(GetKeys()).AddTimeSpans_HighEntropy());
        }

        [Test]
        public void WriteStrings()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddStrings());
            executeSingle(new ColumnBatch(GetKeys()).AddStrings_HighEntropy());
        }

        [Test]
        public void WriteGuids()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddGuids());
            executeSingle(new ColumnBatch(GetKeys()).AddGuids_HighEntropy());
        }

        [Test]
        public void WriteDateTimes()
        {
            executeSingle(new ColumnBatch(GetKeys()).AddDateTimes());
            executeSingle(new ColumnBatch(GetKeys()).AddDateTimes_HighEntropy());
        }
    }
}