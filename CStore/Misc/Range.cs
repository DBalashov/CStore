using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace CStore
{
    readonly struct RangeWithKey
    {
        public readonly CDT Key;
        public readonly int From;
        public readonly int To;

        public int Length => To - From;

        /// <param name="key"></param>
        /// <param name="from">included</param>
        /// <param name="to">not included</param>
        internal RangeWithKey(CDT key, int from, int to)
        {
            Key  = key;
            From = from;
            To   = to;
        }

        public override string ToString() => $"{Key}: {From}-{To} ({To - From} item(s))";
    }

    static class CDTRangeIndexExtenders
    {
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
                    yield return new RangeWithKey(key, startIndex, i);
                    key        = currentKey;
                    startIndex = i;
                }
            }

            yield return new RangeWithKey(key, startIndex, values.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Length(this Range r) => r.End.Value - r.Start.Value;
    }
}