using System;
using System.Diagnostics.CodeAnalysis;

namespace CStore
{
    public readonly struct KeyValueArray
    {
        public readonly CDT[] Keys;
        public readonly Array Values;

        internal KeyValueArray(CDT[] dt, Array values)
        {
            if (dt.Length != values.Length)
                throw new InvalidOperationException($"DT.Length != Values.Length ({dt.Length} vs {values.Length})");

            Keys   = dt ?? throw new ArgumentNullException(nameof(dt));
            Values = values ?? throw new ArgumentNullException(nameof(values));
        }

        [ExcludeFromCodeCoverage]
        public override string ToString() => $"{Keys[0]} - {Keys[^1]} ({Keys.Length}) {Values.GetElementType()}";
    }
}