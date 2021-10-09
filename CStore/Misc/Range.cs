using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace CStore
{
    readonly struct DateTimeRange
    {
        public readonly DateTime From;
        public readonly DateTime To;

        public DateTimeRange(DateTime from, DateTime to)
        {
            From = from;
            To   = to;
        }

        /// <summary> Возвращает индексы элементов From/To в keys </summary>
        public Range GetRange(DateTime[] keys)
        {
            if (keys.Length == 0) return new Range(0, 0);

            var idxFrom = Array.BinarySearch(keys, From);
            var idxTo   = Array.BinarySearch(keys, To);

            return new Range(idxFrom < 0 ? ~idxFrom : idxFrom,
                             idxTo < 0 ? ~idxTo : idxTo);
        }

        public override string ToString() => $"{From} - {To}";
    }

    readonly struct RangeWithKey
    {
        public readonly CDT   Key;
        public readonly Range Range;

        internal RangeWithKey(CDT key, Range range)
        {
            Key   = key;
            Range = range;
        }

        public override string ToString() => $"{Key}: {Range.Start}-{Range.End} ({Range.Length()} item(s))";
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
                    yield return new RangeWithKey(key, new Range(startIndex, i));
                    key        = currentKey;
                    startIndex = i;
                }
            }

            yield return new RangeWithKey(key, new Range(startIndex, values.Length));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Length(this Range r) => r.End.Value - r.Start.Value;

        public static IEnumerable<DateTimeRangeWithKey> GetKeyInRanges(this CDT from, CDT to, CDTUnit unit)
        {
            var start = from.Trunc(unit);

            yield return new DateTimeRangeWithKey(start,
                                                  start != from
                                                      ? new DateTimeRange(from, from.NextNearest(unit))
                                                      : null);

            while (true)
            {
                var next = start.NextNearest(unit);
                if (next > to) break;

                yield return new DateTimeRangeWithKey(next,
                                                      next <= to && to < next.NextNearest(unit)
                                                          ? new DateTimeRange(next, to)
                                                          : null);
                start = next;
            }
        }
    }

    readonly struct DateTimeRangeWithKey
    {
        public readonly CDT            Key;
        public readonly DateTimeRange? Range;

        internal DateTimeRangeWithKey(CDT key, DateTimeRange? range)
        {
            Key   = key;
            Range = range;
        }

        public override string ToString() => $"{Key}: {Range?.From}-{Range?.To}";
    }
}