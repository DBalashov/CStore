using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using CStore;
using FileContainer;

namespace CStoreDev1
{
    class Program
    {
        static readonly Random r = new(Guid.NewGuid().GetHashCode());

        static void Main(string[] args)
        {
            var fileName = @"D:\1.bbb";
            if (File.Exists(fileName))
                File.Delete(fileName);

            using var fs = new PersistentContainer(fileName, new PersistentContainerSettings(512));

            var startFrom = new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var end       = new DateTime(2022, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            var dt = Enumerable.Range(0, (int)(end - startFrom).TotalHours)
                               .Select(p => startFrom.AddHours(p))
                               .ToArray();

            var cb = new ColumnBatch(dt)
                     //.Add("str", Enumerable.Range(0, dt.Length).Select(p => "Item_" + (p % 10)).ToArray())
                     .Add("dbl", Enumerable.Range(0, dt.Length).Select(p => (double)p / (1 + p) + (p % 10)).ToArray())
                     .Add("int32", Enumerable.Range(0, dt.Length).Select(p => p).ToArray())
                     .Add("int33", Enumerable.Range(0, dt.Length).Select(p => p % 30).ToArray())
                     .Add("ts", Enumerable.Range(0, dt.Length).Select(p => TimeSpan.FromSeconds(r.Next(3600))).ToArray())
                     .Add("by", Enumerable.Range(0, dt.Length).Select(p => (byte)((p / 25) % 8)).ToArray())
                     .Add("lat", Enumerable.Range(0, dt.Length).Select(p => (p % 80) + r.Next(1000) / 1000.0).ToArray())
                     .Add("lng", Enumerable.Range(0, dt.Length).Select(p => (p % 80) + r.Next(1000) / 1000.0).ToArray());

            const string prefix = "/x/123";

            using var cs = new ColumnStore(fs);
            var       sw = Stopwatch.StartNew();
            cs.Update(prefix, cb);
            Console.WriteLine("{0} items => {1}", dt.Length, sw.ElapsedMilliseconds + " ms");

            var x = cs.Read(new[] { prefix }, new[] { "int32", "lat", "lng" },
                            new DateTimeRange(new DateTime(2021, 3, 15, 0, 0, 0, DateTimeKind.Utc),
                                              new DateTime(2021, 6, 11, 0, 0, 0, DateTimeKind.Utc)));
        }
    }
}