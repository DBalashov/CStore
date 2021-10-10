using System;
using System.Linq;

namespace CStore.Tests
{
    static class Extenders
    {
        static readonly Random r = new(Guid.NewGuid().GetHashCode());

        public static DateTime[] Convert(this CDT[] source) => source.Select(p => (DateTime)p).ToArray();

        internal static ColumnBatch AddBools(this ColumnBatch batch, string name = "bools") =>
            batch.Add(name, batch.Keys.Convert().Select(p => (p.Minute + p.Second + p.Day) % p.Day > 0).ToArray());

        internal static ColumnBatch AddBools_HighEntropy(this ColumnBatch batch, string name = "bools") =>
            batch.Add(name, batch.Keys.Convert().Select(p => (r.Next(batch.Keys.Length) % 2) > 0).ToArray());


        internal static ColumnBatch AddBytes(this ColumnBatch batch, string name = "bytes") =>
            batch.Add(name, batch.Keys.Convert().Select(p => (byte)(p.Minute + p.Second + p.Day)).ToArray());

        internal static ColumnBatch AddBytes_HighEntropy(this ColumnBatch batch, string name = "bytes") =>
            batch.Add(name, batch.Keys.Convert().Select(p => (byte)r.Next(0xFF)).ToArray());


        internal static ColumnBatch AddInt16s(this ColumnBatch batch, string name = "int16s") =>
            batch.Add(name, batch.Keys.Convert().Select(p => (short)(p.Minute + p.Second + p.Day)).ToArray());

        internal static ColumnBatch AddInt16s_HighEntropy(this ColumnBatch batch, string name = "int16s") =>
            batch.Add(name, batch.Keys.Convert().Select(p => (short)r.Next(0xFFFF)).ToArray());


        internal static ColumnBatch AddInt32s(this ColumnBatch batch, string name = "int32s") =>
            batch.Add(name, batch.Keys.Convert().Select(p => p.Minute + p.Second + p.Day).ToArray());

        internal static ColumnBatch AddInt32s_HighEntropy(this ColumnBatch batch, string name = "int32s") =>
            batch.Add(name, batch.Keys.Convert().Select(p => r.Next(batch.Keys.Length)).ToArray());


        internal static ColumnBatch AddInt64s(this ColumnBatch batch, string name = "int64s") =>
            batch.Add(name, batch.Keys.Convert()
                                 .Select(p =>
                                 {
                                     Int64 x = p.Minute + p.Second + p.Day + 1;
                                     return x | ((x + 2) << 32);
                                 })
                                 .ToArray());

        internal static ColumnBatch AddInt64s_HighEntropy(this ColumnBatch batch, string name = "int64s") =>
            batch.Add(name, batch.Keys.Convert()
                                 .Select(p => ((Int64)r.Next(int.MaxValue)) | (((Int64)r.Next(int.MaxValue)) << 32))
                                 .ToArray());


        internal static ColumnBatch AddDoubles(this ColumnBatch batch, string name = "doubles") =>
            batch.Add(name, batch.Keys.Convert().Select(p => p.TimeOfDay.TotalMilliseconds).ToArray());

        internal static ColumnBatch AddDoubles_HighEntropy(this ColumnBatch batch, string name = "doubles") =>
            batch.Add(name, batch.Keys.Convert().Select(p => batch.Keys.Length * 2.0 / (r.Next(batch.Keys.Length) + 1)).ToArray());


        internal static ColumnBatch AddTimeSpans(this ColumnBatch batch, string name = "timespans") =>
            batch.Add(name, batch.Keys.Convert().Select(p => p.TimeOfDay).ToArray());

        internal static ColumnBatch AddTimeSpans_HighEntropy(this ColumnBatch batch, string name = "timespans") =>
            batch.Add(name, batch.Keys.Convert().Select(p => TimeSpan.FromSeconds(r.Next(batch.Keys.Length))).ToArray());


        internal static ColumnBatch AddStrings(this ColumnBatch batch, string name = "strings") =>
            batch.Add(name, batch.Keys.Convert().Select(p => "Item Address " + p.ToString("yyyyMMdd") + "/" + p.Month + "/" + p.Minute + "/" + p.Day).ToArray());

        internal static ColumnBatch AddStrings_HighEntropy(this ColumnBatch batch, string name = "strings") =>
            batch.Add(name, batch.Keys.Convert().Select(p => "Item " + r.Next(batch.Keys.Length)).ToArray());


        internal static ColumnBatch AddGuids(this ColumnBatch batch, string name = "guids") =>
            batch.Add(name, batch.Keys.Convert().Select(p => new Guid((uint)p.Year, 0, 0, (byte)p.Year, (byte)p.Month, (byte)p.Day, (byte)p.Hour, 0, 0, 0, 0)).ToArray());

        internal static ColumnBatch AddGuids_HighEntropy(this ColumnBatch batch, string name = "guids") =>
            batch.Add(name, batch.Keys.Convert().Select(p => Guid.NewGuid()).ToArray());


        internal static ColumnBatch AddDateTimes(this ColumnBatch batch, string name = "datetimes") =>
            batch.Add(name, batch.Keys.Convert().Select(p => p).ToArray());

        internal static ColumnBatch AddDateTimes_HighEntropy(this ColumnBatch batch, string name = "datetimes") =>
            batch.Add(name, batch.Keys.Convert().Select(p => p.AddSeconds(r.Next(batch.Keys.Length) - batch.Keys.Length / 2)).ToArray());
    }
}