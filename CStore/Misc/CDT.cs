using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace CStore
{
    public enum CDTUnit
    {
        Year,
        Month,
        Day,
        Hour,
        Minute
    }

    /// <summary>
    /// CDT = CompactDateTime - 4-bytes (int32) date-time (used as key)
    ///
    /// 1-second precision
    /// 
    /// min value: 2000.1.1
    /// max value: 2068.1.1
    /// 
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public readonly struct CDT : IComparable<CDT>, IComparable
    {
        const int    MIN_YEAR = 2000;
        const int    MAX_YEAR = 2068;
        const string FMT      = "yyyy-MM-dd HH:mm:ss";

        static readonly DateTime startDT = new(MIN_YEAR, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        /// <summary> seconds since (2000,1,1) </summary>
        public readonly int Value;

        public CDT(string from) : this(DateTime.ParseExact(from, FMT, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal))
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CDT(DateTime dt) => Value = dt.Year is < MIN_YEAR or > MAX_YEAR ? 0 : (int)dt.Subtract(startDT).TotalSeconds;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal CDT(int value) => Value = value;

        internal CDT(ushort date, int hour = 0, int minute = 0, int seconds = 0) :
            this(new DateTime(MIN_YEAR + ((date >> 9) & 0b1_111_111),
                              ((date >> 5) & 0b11_111) + 1,
                              (date & 0b1111) + 1, hour, minute, seconds))
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CDT(int year, int month, int day) =>
            Value = (int)new DateTime(year, month, day, 0, 0, 0, 0, DateTimeKind.Utc).Subtract(startDT).TotalSeconds;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CDT Trunc(CDTUnit to)
        {
            if (Value == 0)
                return new CDT(0);

            var dt = (DateTime)this;
            return to switch
            {
                CDTUnit.Year => new CDT(dt.Year, 1, 1),
                CDTUnit.Month => new CDT(dt.Year, dt.Month, 1),
                CDTUnit.Day => new CDT(dt.Year, dt.Month, dt.Day),
                CDTUnit.Hour => new CDT(new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, DateTimeKind.Utc)),
                CDTUnit.Minute => new CDT(new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0, DateTimeKind.Utc)),
                _ => throw new NotSupportedException(to.ToString())
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CDT Add(TimeSpan ts) => new(Value + (int)ts.TotalSeconds);

        /// <summary> return next time unit from current (next minute, hour, month, ...) </summary>
        public CDT NextNearest(CDTUnit to)
        {
            var dt = (DateTime)Trunc(to);
            return to switch
            {
                CDTUnit.Minute => dt.AddMinutes(1),
                CDTUnit.Hour => dt.AddHours(1),
                CDTUnit.Day => dt.AddDays(1),
                CDTUnit.Month => dt.AddMonths(1),
                CDTUnit.Year => dt.AddYears(1),
                _ => throw new NotSupportedException(to.ToString())
            };
        }

        /// <summary>
        /// component:  Y  M  D
        /// bits       [7][5][4]
        /// M - zero-based
        /// D - zero-based
        /// Y - year-MIN_YEAR
        /// </summary>
        public ushort Date
        {
            get
            {
                DateTime dt = this;

                var y = dt.Year - MIN_YEAR;
                var m = dt.Month - 1;
                var d = dt.Day - 1;

                return (ushort)(d & (m << 5) & (y << 9));
            }
        }

        #region DateTime <-> CDT, TimeSpan <- CDT

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator CDT(DateTime dt) => new(dt);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator DateTime(CDT dt) => dt.Value == 0
            ? DateTime.MinValue
            : startDT.AddSeconds(dt.Value);

        // [MethodImpl(MethodImplOptions.AggressiveInlining)]
        // public static explicit operator TimeSpan(CDT dt) => dt.Value == 0
        //     ? TimeSpan.Zero
        //     : ((DateTime) dt).TimeOfDay;

        #endregion

        #region +  -  ==  !=

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        // public static CDT operator +(CDT to, int seconds) => new CDT(to.Value + seconds);
        //
        // public static CDT operator -(CDT to, int seconds) => new CDT(to.Value - seconds);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator ==(CDT x, CDT y) => x.Value == y.Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator !=(CDT x, CDT y) => x.Value != y.Value;

        #endregion

        #region >  <  >=  <=

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator <(CDT x, CDT y) => x.Value < y.Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator >(CDT x, CDT y) => x.Value > y.Value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator <=(CDT x, CDT y) => x.Value <= y.Value;

        public static bool operator >=(CDT x, CDT y) => x.Value >= y.Value;

        #endregion

        [ExcludeFromCodeCoverage]
        public override bool Equals(object? obj) => obj is CDT cdt && cdt.Value == Value;

        [ExcludeFromCodeCoverage]
        public override int GetHashCode() => Value;

        public int CompareTo(object? obj)   => obj is CDT cdt ? Value.CompareTo(cdt.Value) : -1;
        public int CompareTo(CDT     other) => Value.CompareTo(other.Value);

        public override string ToString() => ((DateTime)this).ToString(FMT);
    }
}