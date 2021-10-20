using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace CStore
{
    public sealed class ColumnBatch
    {
        internal readonly Dictionary<string, Array> values = new(StringComparer.InvariantCultureIgnoreCase);

        public readonly CDT[] Keys;

        public Array this[string name] => values[name];

        public string[] Columns => values.Keys.ToArray();

        public ColumnBatch(DateTime[] dt)
        {
            if (dt == null)
                throw new ArgumentNullException(nameof(dt));

            if (dt.Length == 0)
                throw new ArgumentException("Must not be empty", nameof(dt));

            Keys    = new CDT[dt.Length];
            Keys[0] = dt[0];

            for (var i = 1; i < dt.Length; i++)
            {
                Keys[i] = dt[i];
                if (Keys[i] <= Keys[i - 1])
                    throw new ArgumentException("All date/time must be unique and sorted by ascending", nameof(Keys));
            }
        }

        public ColumnBatch Add(string columnName, Array columnValues, bool withReplaceExisting = false)
        {
            if (columnValues == null)
                throw new ArgumentNullException(nameof(columnValues));
            if (columnValues.Length != Keys.Length)
                throw new ArgumentException($"'{columnName}' must be exactly the same length as date/time ({Keys.Length})", nameof(columnValues));

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

        [ExcludeFromCodeCoverage]
        public override string ToString() => $"{Keys[0]} - {Keys[^1]}: {Keys.Length} => {values.Count} columns";
    }
}