using System;

namespace CStore
{
    public readonly struct KeyValueArray<K>
    {
        public readonly K[]   Keys;
        public readonly Array Values;

        internal KeyValueArray(K[] dt, Array values)
        {
            if (dt.Length != values.Length)
                throw new InvalidOperationException($"DT.Length != Values.Length ({dt.Length} vs {values.Length})");

            Keys   = dt ?? throw new ArgumentNullException(nameof(dt));
            Values = values ?? throw new ArgumentNullException(nameof(values));
        }

        public override string ToString() => $"{Keys[0]} - {Keys[^1]} ({Keys.Length}) {Values.GetValue(0)?.GetType()}";
    }
}