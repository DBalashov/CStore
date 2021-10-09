using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace CStore
{
    static class CDTRangeIndexExtenders
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Length(this Range r) => r.End.Value - r.Start.Value;

        internal static IEnumerable<RangeWithKey> GetRange(this CDT[] values, CDTUnit unit)
        {
            if (!values.Any())
                yield break;

            var key        = values[0].Trunc(unit);
            var startIndex = 0;
            for (var i = 1; i < values.Length; i++)
            {
                var currentKey = values[i].Trunc(unit);
                if (currentKey != key)
                {
                    yield return new RangeWithKey(key, new Range(startIndex, i));
                    key        = currentKey;
                    startIndex = i;
                }
            }

            yield return new RangeWithKey(key, new Range(startIndex, values.Length));
        }
    }
}