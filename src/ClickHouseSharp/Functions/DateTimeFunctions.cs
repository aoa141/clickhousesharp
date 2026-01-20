using System.Globalization;
using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class DateTimeFunctions
{
    public class NowFunction : ScalarFunction
    {
        public override string Name => "now";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new DateTimeValue(DateTime.UtcNow);
        }
    }

    public class TodayFunction : ScalarFunction
    {
        public override string Name => "today";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new DateValue(DateOnly.FromDateTime(DateTime.UtcNow));
        }
    }

    public class YesterdayFunction : ScalarFunction
    {
        public override string Name => "yesterday";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            return new DateValue(DateOnly.FromDateTime(DateTime.UtcNow.AddDays(-1)));
        }
    }

    public class ToYearFunction : ScalarFunction
    {
        public override string Name => "toYear";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            return new Int64Value(dt.Year);
        }
    }

    public class ToMonthFunction : ScalarFunction
    {
        public override string Name => "toMonth";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            return new Int64Value(dt.Month);
        }
    }

    public class ToDayOfMonthFunction : ScalarFunction
    {
        public override string Name => "toDayOfMonth";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            return new Int64Value(dt.Day);
        }
    }

    public class ToHourFunction : ScalarFunction
    {
        public override string Name => "toHour";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            return new Int64Value(dt.Hour);
        }
    }

    public class ToMinuteFunction : ScalarFunction
    {
        public override string Name => "toMinute";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            return new Int64Value(dt.Minute);
        }
    }

    public class ToSecondFunction : ScalarFunction
    {
        public override string Name => "toSecond";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            return new Int64Value(dt.Second);
        }
    }

    public class ToDayOfWeekFunction : ScalarFunction
    {
        public override string Name => "toDayOfWeek";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            // ClickHouse: Monday = 1, Sunday = 7
            var dow = (int)dt.DayOfWeek;
            return new Int64Value(dow == 0 ? 7 : dow);
        }
    }

    public class ToDayOfYearFunction : ScalarFunction
    {
        public override string Name => "toDayOfYear";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            return new Int64Value(dt.DayOfYear);
        }
    }

    public class ToQuarterFunction : ScalarFunction
    {
        public override string Name => "toQuarter";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            return new Int64Value((dt.Month - 1) / 3 + 1);
        }
    }

    public class ToDateFunction : ScalarFunction
    {
        public override string Name => "toDate";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return args[0] switch
            {
                DateValue d => d,
                DateTimeValue dt => new DateValue(DateOnly.FromDateTime(dt.Value)),
                StringValue s => new DateValue(DateOnly.Parse(s.Value)),
                _ => new DateValue(DateOnly.FromDateTime(GetDateTime(args[0])))
            };
        }
    }

    public class ToDateTimeFunction : ScalarFunction
    {
        public override string Name => "toDateTime";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return args[0] switch
            {
                DateTimeValue dt => dt,
                DateValue d => new DateTimeValue(d.Value.ToDateTime(TimeOnly.MinValue)),
                StringValue s => new DateTimeValue(DateTime.Parse(s.Value)),
                Int64Value i => new DateTimeValue(DateTimeOffset.FromUnixTimeSeconds(i.Value).DateTime),
                _ => throw new InvalidOperationException($"Cannot convert {args[0].Type} to DateTime")
            };
        }
    }

    public class ToUnixTimestampFunction : ScalarFunction
    {
        public override string Name => "toUnixTimestamp";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            return new Int64Value(new DateTimeOffset(dt, TimeSpan.Zero).ToUnixTimeSeconds());
        }
    }

    public class FromUnixTimestampFunction : ScalarFunction
    {
        public override string Name => "fromUnixTimestamp";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var ts = args[0].AsInt64();
            return new DateTimeValue(DateTimeOffset.FromUnixTimeSeconds(ts).DateTime);
        }
    }

    public class DateAddFunction : ScalarFunction
    {
        public override string Name => "dateAdd";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull || args[2].IsNull) return NullValue.Instance;
            var unit = args[0].AsString().ToLowerInvariant();
            var value = (int)args[1].AsInt64();
            var dt = GetDateTime(args[2]);

            var result = unit switch
            {
                "year" or "years" => dt.AddYears(value),
                "month" or "months" => dt.AddMonths(value),
                "week" or "weeks" => dt.AddDays(value * 7),
                "day" or "days" => dt.AddDays(value),
                "hour" or "hours" => dt.AddHours(value),
                "minute" or "minutes" => dt.AddMinutes(value),
                "second" or "seconds" => dt.AddSeconds(value),
                _ => throw new InvalidOperationException($"Unknown date unit: {unit}")
            };

            return new DateTimeValue(result);
        }
    }

    public class DateSubFunction : ScalarFunction
    {
        public override string Name => "dateSub";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull || args[2].IsNull) return NullValue.Instance;
            var unit = args[0].AsString().ToLowerInvariant();
            var value = -(int)args[1].AsInt64();
            var dt = GetDateTime(args[2]);

            var result = unit switch
            {
                "year" or "years" => dt.AddYears(value),
                "month" or "months" => dt.AddMonths(value),
                "week" or "weeks" => dt.AddDays(value * 7),
                "day" or "days" => dt.AddDays(value),
                "hour" or "hours" => dt.AddHours(value),
                "minute" or "minutes" => dt.AddMinutes(value),
                "second" or "seconds" => dt.AddSeconds(value),
                _ => throw new InvalidOperationException($"Unknown date unit: {unit}")
            };

            return new DateTimeValue(result);
        }
    }

    public class DateDiffFunction : ScalarFunction
    {
        public override string Name => "dateDiff";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull || args[2].IsNull) return NullValue.Instance;
            var unit = args[0].AsString().ToLowerInvariant();
            var dt1 = GetDateTime(args[1]);
            var dt2 = GetDateTime(args[2]);
            var diff = dt2 - dt1;

            var result = unit switch
            {
                "year" or "years" => (dt2.Year - dt1.Year),
                "month" or "months" => (dt2.Year - dt1.Year) * 12 + (dt2.Month - dt1.Month),
                "week" or "weeks" => (long)(diff.TotalDays / 7),
                "day" or "days" => (long)diff.TotalDays,
                "hour" or "hours" => (long)diff.TotalHours,
                "minute" or "minutes" => (long)diff.TotalMinutes,
                "second" or "seconds" => (long)diff.TotalSeconds,
                _ => throw new InvalidOperationException($"Unknown date unit: {unit}")
            };

            return new Int64Value(result);
        }
    }

    public class FormatDateTimeFunction : ScalarFunction
    {
        public override string Name => "formatDateTime";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            var dt = GetDateTime(args[0]);
            var format = ConvertClickHouseFormat(args[1].AsString());
            return new StringValue(dt.ToString(format, CultureInfo.InvariantCulture));
        }

        private static string ConvertClickHouseFormat(string chFormat)
        {
            // Convert ClickHouse format to .NET format
            return chFormat
                .Replace("%Y", "yyyy")
                .Replace("%m", "MM")
                .Replace("%d", "dd")
                .Replace("%H", "HH")
                .Replace("%M", "mm")
                .Replace("%S", "ss")
                .Replace("%F", "yyyy-MM-dd")
                .Replace("%T", "HH:mm:ss");
        }
    }

    private static DateTime GetDateTime(ClickHouseValue val)
    {
        return val switch
        {
            DateTimeValue dt => dt.Value,
            DateValue d => d.Value.ToDateTime(TimeOnly.MinValue),
            StringValue s => DateTime.Parse(s.Value),
            Int64Value i => DateTimeOffset.FromUnixTimeSeconds(i.Value).DateTime,
            _ => throw new InvalidOperationException($"Cannot convert {val.Type} to DateTime")
        };
    }
}
