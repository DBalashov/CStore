using System;
using System.Linq;
using NUnit.Framework;

namespace CStore.Tests
{
    public abstract class ReadWriteSingle : Base
    {
        const string PREFIX = "/test";

        [SetUp]
        public void Setup()
        {
        }

        void executeSingle(ColumnStore store, ColumnBatch batch, bool withWrite = true)
        {
            if (withWrite)
                store.Update(PREFIX, batch);

            TestContext.WriteLine("Keys / Columns       : {0} / {1}", batch.Keys.Length, string.Join(',', batch.Columns));
            TestContext.WriteLine("Length/Pages/PageSize: {0} / {1} / {2}\n", store.c.Length, store.c.TotalPages, store.c.PageSize);

            var checkRange = new DateTimeRange(batch.Keys[0], batch.Keys[^1]);

            var r = store.Read(new[] { PREFIX }, batch.Columns, checkRange);

            Assert.IsNotNull(r);
            Assert.IsNotEmpty(r);
            Assert.IsTrue(r.ContainsKey(PREFIX) && r.Count == 1);

            Assert.IsEmpty(r[PREFIX].Keys.Except(batch.Columns));

            CheckSingle(PREFIX, batch, batch.Columns[0], r);
        }

        ColumnBatch modifyBatchRandom(ColumnBatch batch)
        {
            var offset = rand.Next(batch.Keys.Length / 2) + 1;
            var length = rand.Next(batch.Keys.Length / 3 + 1);
            var b      = new ColumnBatch(batch.Keys.Skip(offset).Take(length).Select(p => (DateTime)p).ToArray());
            foreach (var col in batch.Columns)
            {
                var a = Array.CreateInstance(batch[col].GetElementType(), length);
                Array.Copy(batch[col], offset, a, 0, length);
                b.Add(col, a);
            }

            return b;
        }

        #region WriteBools

        [Test]
        public void WriteBools()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddBools();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddBools_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteBools_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddBools());
                executeSingle(store, new ColumnBatch(keys).AddBools());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddBools_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddBools_HighEntropy());
            }
        }

        #endregion

        #region WriteBytes

        [Test]
        public void WriteBytes()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddBytes();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddBytes_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteBytes_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddBytes());
                executeSingle(store, new ColumnBatch(keys).AddBytes());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddBytes_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddBytes_HighEntropy());
            }
        }

        #endregion

        #region WriteInt16s

        [Test]
        public void WriteInt16s()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddInt16s();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddInt16s_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteInt16s_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddInt16s());
                executeSingle(store, new ColumnBatch(keys).AddInt16s());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddInt16s_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddInt16s_HighEntropy());
            }
        }

        #endregion

        #region WriteInt32s

        [Test]
        public void WriteInt32s()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddInt32s();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddInt32s_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteInt32s_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddInt32s());
                executeSingle(store, new ColumnBatch(keys).AddInt32s());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddInt32s_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddInt32s_HighEntropy());
            }
        }

        #endregion

        #region WriteInt64s

        [Test]
        public void WriteInt64s()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddInt64s();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddInt64s_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteInt64s_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddInt64s());
                executeSingle(store, new ColumnBatch(keys).AddInt64s());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddInt64s_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddInt64s_HighEntropy());
            }
        }

        #endregion

        #region WriteDoubles

        [Test]
        public void WriteDoubles()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddDoubles();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddDoubles_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteDoubles_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddDoubles());
                executeSingle(store, new ColumnBatch(keys).AddDoubles());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddDoubles_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddDoubles_HighEntropy());
            }
        }

        #endregion

        #region WriteTimeSpans

        [Test]
        public void WriteTimeSpans()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddTimeSpans();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddTimeSpans_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteTimeSpans_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddTimeSpans());
                executeSingle(store, new ColumnBatch(keys).AddTimeSpans());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddTimeSpans_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddTimeSpans_HighEntropy());
            }
        }

        #endregion

        #region WriteStrings

        [Test]
        public void WriteStrings()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddStrings();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddStrings_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteStrings_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddStrings());
                executeSingle(store, new ColumnBatch(keys).AddStrings());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddStrings_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddStrings_HighEntropy());
            }
        }

        #endregion

        #region WriteGuids

        [Test]
        public void WriteGuids()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddGuids();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddGuids_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteGuids_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddGuids());
                executeSingle(store, new ColumnBatch(keys).AddGuids());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddGuids_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddGuids_HighEntropy());
            }
        }

        #endregion

        #region WriteDateTimes

        [Test]
        public void WriteDateTimes()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddDateTimes();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }

            using (var store = GetStore())
            {
                var batch = new ColumnBatch(keys).AddDateTimes_HighEntropy();
                executeSingle(store, batch);
                executeSingle(store, modifyBatchRandom(batch), false);
            }
        }

        [Test]
        public void WriteDateTimes_Merge()
        {
            var keys = GetKeys();
            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddDateTimes());
                executeSingle(store, new ColumnBatch(keys).AddDateTimes());
            }

            using (var store = GetStore())
            {
                executeSingle(store, new ColumnBatch(keys).AddDateTimes_HighEntropy());
                executeSingle(store, new ColumnBatch(keys).AddDateTimes_HighEntropy());
            }
        }

        #endregion
    }
}