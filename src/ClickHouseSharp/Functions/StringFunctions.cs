using System.Text;
using ClickHouseSharp.Types;

namespace ClickHouseSharp.Functions;

public static class StringFunctions
{
    public class LengthFunction : ScalarFunction
    {
        public override string Name => "length";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new Int64Value(args[0].AsString().Length);
        }
    }

    public class LowerFunction : ScalarFunction
    {
        public override string Name => "lower";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new StringValue(args[0].AsString().ToLowerInvariant());
        }
    }

    public class UpperFunction : ScalarFunction
    {
        public override string Name => "upper";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new StringValue(args[0].AsString().ToUpperInvariant());
        }
    }

    public class TrimFunction : ScalarFunction
    {
        public override string Name => "trim";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new StringValue(args[0].AsString().Trim());
        }
    }

    public class LTrimFunction : ScalarFunction
    {
        public override string Name => "trimLeft";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new StringValue(args[0].AsString().TrimStart());
        }
    }

    public class RTrimFunction : ScalarFunction
    {
        public override string Name => "trimRight";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new StringValue(args[0].AsString().TrimEnd());
        }
    }

    public class SubstringFunction : ScalarFunction
    {
        public override string Name => "substring";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var str = args[0].AsString();
            var start = (int)args[1].AsInt64() - 1; // ClickHouse uses 1-based indexing
            if (start < 0) start = 0;
            if (start >= str.Length) return new StringValue("");

            if (args.Count > 2 && !args[2].IsNull)
            {
                var length = (int)args[2].AsInt64();
                if (length <= 0) return new StringValue("");
                return new StringValue(str.Substring(start, Math.Min(length, str.Length - start)));
            }
            return new StringValue(str[start..]);
        }
    }

    public class ConcatFunction : ScalarFunction
    {
        public override string Name => "concat";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            var sb = new StringBuilder();
            foreach (var arg in args)
            {
                if (!arg.IsNull)
                    sb.Append(arg.AsString());
            }
            return new StringValue(sb.ToString());
        }
    }

    public class ReplaceFunction : ScalarFunction
    {
        public override string Name => "replace";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var str = args[0].AsString();
            var from = args[1].AsString();
            var to = args[2].AsString();
            return new StringValue(str.Replace(from, to));
        }
    }

    public class ReverseFunction : ScalarFunction
    {
        public override string Name => "reverse";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var chars = args[0].AsString().ToCharArray();
            Array.Reverse(chars);
            return new StringValue(new string(chars));
        }
    }

    public class PositionFunction : ScalarFunction
    {
        public override string Name => "position";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            var haystack = args[0].AsString();
            var needle = args[1].AsString();
            var pos = haystack.IndexOf(needle, StringComparison.Ordinal);
            return new Int64Value(pos + 1); // 1-based, 0 if not found
        }
    }

    public class StartsWithFunction : ScalarFunction
    {
        public override string Name => "startsWith";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            return new BoolValue(args[0].AsString().StartsWith(args[1].AsString()));
        }
    }

    public class EndsWithFunction : ScalarFunction
    {
        public override string Name => "endsWith";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            return new BoolValue(args[0].AsString().EndsWith(args[1].AsString()));
        }
    }

    public class RepeatFunction : ScalarFunction
    {
        public override string Name => "repeat";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            var str = args[0].AsString();
            var count = (int)args[1].AsInt64();
            if (count <= 0) return new StringValue("");
            return new StringValue(string.Concat(Enumerable.Repeat(str, count)));
        }
    }

    public class LeftFunction : ScalarFunction
    {
        public override string Name => "left";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            var str = args[0].AsString();
            var len = (int)args[1].AsInt64();
            if (len <= 0) return new StringValue("");
            return new StringValue(str[..Math.Min(len, str.Length)]);
        }
    }

    public class RightFunction : ScalarFunction
    {
        public override string Name => "right";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            var str = args[0].AsString();
            var len = (int)args[1].AsInt64();
            if (len <= 0) return new StringValue("");
            var start = Math.Max(0, str.Length - len);
            return new StringValue(str[start..]);
        }
    }

    public class SplitByCharFunction : ScalarFunction
    {
        public override string Name => "splitByChar";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            var sep = args[0].AsString();
            var str = args[1].AsString();
            var parts = str.Split(sep.Length > 0 ? sep[0] : '\0');
            var elements = parts.Select(p => (ClickHouseValue)new StringValue(p)).ToList();
            return new ArrayValue(elements, new ArrayType(StringType.Instance));
        }
    }

    public class SplitByStringFunction : ScalarFunction
    {
        public override string Name => "splitByString";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull || args[1].IsNull) return NullValue.Instance;
            var sep = args[0].AsString();
            var str = args[1].AsString();
            var parts = string.IsNullOrEmpty(sep)
                ? [str]
                : str.Split(sep);
            var elements = parts.Select(p => (ClickHouseValue)new StringValue(p)).ToList();
            return new ArrayValue(elements, new ArrayType(StringType.Instance));
        }
    }

    public class ArrayStringConcatFunction : ScalarFunction
    {
        public override string Name => "arrayStringConcat";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            if (args[0] is not ArrayValue arr) throw new InvalidOperationException("Expected array argument");
            var sep = args.Count > 1 && !args[1].IsNull ? args[1].AsString() : "";
            var joined = string.Join(sep, arr.Elements.Select(e => e.AsString()));
            return new StringValue(joined);
        }
    }

    public class FormatFunction : ScalarFunction
    {
        public override string Name => "format";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            var format = args[0].AsString();
            var formatArgs = args.Skip(1).Select(a => a.RawValue).ToArray();
            try
            {
                return new StringValue(string.Format(format, formatArgs));
            }
            catch
            {
                return new StringValue(format);
            }
        }
    }

    public class ToStringFunction : ScalarFunction
    {
        public override string Name => "toString";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return NullValue.Instance;
            return new StringValue(args[0].RawValue?.ToString() ?? "");
        }
    }

    public class EmptyFunction : ScalarFunction
    {
        public override string Name => "empty";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return new BoolValue(true);
            var val = args[0];
            if (val is StringValue s) return new BoolValue(string.IsNullOrEmpty(s.Value));
            if (val is ArrayValue a) return new BoolValue(a.Elements.Count == 0);
            return new BoolValue(false);
        }
    }

    public class NotEmptyFunction : ScalarFunction
    {
        public override string Name => "notEmpty";
        public override ClickHouseValue Execute(IReadOnlyList<ClickHouseValue> args, bool distinct = false)
        {
            if (args[0].IsNull) return new BoolValue(false);
            var val = args[0];
            if (val is StringValue s) return new BoolValue(!string.IsNullOrEmpty(s.Value));
            if (val is ArrayValue a) return new BoolValue(a.Elements.Count > 0);
            return new BoolValue(true);
        }
    }
}
