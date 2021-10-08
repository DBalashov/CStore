using System;
using System.Collections.Generic;
using System.Linq;

namespace CStore
{
    public sealed class ColumnBatch
    {
        public readonly CDT[] DT;

        readonly Dictionary<string, Array> values = new(StringComparer.InvariantCultureIgnoreCase);

        public Array this[string name] => values[name];

        public string[] Columns => values.Keys.ToArray();

        public ColumnBatch(DateTime[] dt) => DT = checkAndConvert(dt);

        CDT[] checkAndConvert(DateTime[] dt)
        {
            if (dt == null)
                throw new ArgumentNullException(nameof(dt));

            if (dt.Length == 0)
                throw new ArgumentException("Must not be empty", nameof(dt));

            var r = new CDT[dt.Length];
            r[0] = dt[0];

            for (var i = 1; i < dt.Length; i++)
            {
                r[i] = dt[i];
                if (r[i] <= r[i - 1])
                    throw new ArgumentException("All date/time must be unique and sorted by ascending", nameof(dt));
            }

            return r;
        }

        public ColumnBatch Add(string columnName, Array columnValues, bool withReplaceExisting = false)
        {
            if (columnValues == null)
                throw new ArgumentNullException(nameof(columnValues));
            if (columnValues.Length != DT.Length)
                throw new ArgumentException($"'{columnName}' must be exactly the same length as date/time ({DT.Length})", nameof(columnValues));

            if (string.IsNullOrEmpty(columnName))
                throw new ArgumentNullException(nameof(columnName));

            if (!withReplaceExisting)
                if (values.ContainsKey(columnName))
                    throw new ArgumentException($"'{columnName}' already exists in collection", nameof(columnName));

            if (columnValues.Rank > 1)
                throw new ArgumentException($"Values can't be multidimensional ({columnValues.Rank})", nameof(columnValues));

            values[columnName] = columnValues;

            return this;
        }

        public override string ToString() => $"{DT[0]} - ${DT[^1]}: {DT.Length} => {values.Count} columns";
    }
}