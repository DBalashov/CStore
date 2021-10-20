using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace CStore
{
    readonly struct RangeWithKey
    {
        public readonly CDT   Key;
        public readonly Range Range;

        internal RangeWithKey(CDT key, Range range) => (Key, Range) = (key, range);

        [ExcludeFromCodeCoverage]
        public override string ToString() => $"{Key}: {Range.Start}-{Range.End} ({Range.Length()} item(s))";
    }

    #region DateTimeRange

    public readonly struct DateTimeRange
    {
        public readonly DateTime From;
        public readonly DateTime To;

        public DateTimeRange(DateTime from, DateTime to) => (From, To) = (from, to);

        /// <summary> Возвращает индексы элементов From/To в keys </summary>
        public Range GetRange(CDT[] keys)
        {
            if (keys.Length == 0) return new Range(0, 0);

            var idxFrom = Array.BinarySearch(keys, From);
            var idxTo   = Array.BinarySearch(keys, To);

            return new Range(idxFrom < 0 ? ~idxFrom : idxFrom,
                             idxTo < 0 ? ~idxTo : idxTo + 1);
        }

        internal IEnumerable<DateTimeRangeWithKey> GetKeyInRanges(CDTUnit unit)
        {
            var from = (CDT)From;
            var to   = (CDT)To;

            var start = from.Trunc(unit);

            if (start == from)
            {
                yield return new DateTimeRangeWithKey(start, null); // первый парт целиком
            }
            else
            {
                if (from.Trunc(unit) == to.Trunc(unit)) // запрошенный range укладывается целиком в первый парт - возвращаем и прекращаем
                {
                    yield return new DateTimeRangeWithKey(start, new DateTimeRange(from, to));
                    yield break;
                }

                yield return new DateTimeRangeWithKey(start, new DateTimeRange(from, from.NextNearest(unit)));
            }

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

        [ExcludeFromCodeCoverage]
        public override string ToString() => $"{From} - {To}";
    }

    #endregion

    #region DateTimeRangeWithKey

    readonly struct DateTimeRangeWithKey
    {
        public readonly CDT            Key;
        public readonly DateTimeRange? Range;

        internal DateTimeRangeWithKey(CDT key, DateTimeRange? range) => (Key, Range) = (key, range);

        [ExcludeFromCodeCoverage]
        public override string ToString() => $"{Key}: {Range?.From}-{Range?.To}";
    }

    #endregion
}