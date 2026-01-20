using ClickHouseSharp;

namespace ClickHouseSharp.Tests;

public class FunctionTests
{
    [Fact]
    public void MathFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                abs(-5) AS abs_val,
                ceil(3.2) AS ceil_val,
                floor(3.8) AS floor_val,
                round(3.567, 2) AS round_val,
                sqrt(16) AS sqrt_val,
                pow(2, 10) AS pow_val
        ");

        Assert.Single(result.Rows);
        Assert.Equal(5.0, result.Rows[0][0].AsFloat64());
        Assert.Equal(4.0, result.Rows[0][1].AsFloat64());
        Assert.Equal(3.0, result.Rows[0][2].AsFloat64());
        Assert.Equal(3.57, result.Rows[0][3].AsFloat64());
        Assert.Equal(4.0, result.Rows[0][4].AsFloat64());
        Assert.Equal(1024.0, result.Rows[0][5].AsFloat64());
    }

    [Fact]
    public void TrigFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT sin(0), cos(0), tan(0)");

        Assert.Single(result.Rows);
        Assert.Equal(0.0, result.Rows[0][0].AsFloat64(), 10);
        Assert.Equal(1.0, result.Rows[0][1].AsFloat64(), 10);
        Assert.Equal(0.0, result.Rows[0][2].AsFloat64(), 10);
    }

    [Fact]
    public void StringFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                length('hello') AS len,
                lower('HELLO') AS lower_val,
                upper('hello') AS upper_val,
                trim('  hello  ') AS trim_val,
                concat('hello', ' ', 'world') AS concat_val,
                substring('hello', 2, 3) AS substr_val
        ");

        Assert.Single(result.Rows);
        Assert.Equal(5L, result.Rows[0][0].AsInt64());
        Assert.Equal("hello", result.Rows[0][1].AsString());
        Assert.Equal("HELLO", result.Rows[0][2].AsString());
        Assert.Equal("hello", result.Rows[0][3].AsString());
        Assert.Equal("hello world", result.Rows[0][4].AsString());
        Assert.Equal("ell", result.Rows[0][5].AsString());
    }

    [Fact]
    public void StringSearchFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                position('hello world', 'world') AS pos,
                startsWith('hello', 'he') AS starts,
                endsWith('hello', 'lo') AS ends,
                replace('hello world', 'world', 'there') AS replaced
        ");

        Assert.Single(result.Rows);
        Assert.Equal(7L, result.Rows[0][0].AsInt64());
        Assert.True(result.Rows[0][1].AsBool());
        Assert.True(result.Rows[0][2].AsBool());
        Assert.Equal("hello there", result.Rows[0][3].AsString());
    }

    [Fact]
    public void DateFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                toYear(toDate('2024-06-15')) AS year,
                toMonth(toDate('2024-06-15')) AS month,
                toDayOfMonth(toDate('2024-06-15')) AS day,
                toDayOfWeek(toDate('2024-06-15')) AS dow
        ");

        Assert.Single(result.Rows);
        Assert.Equal(2024L, result.Rows[0][0].AsInt64());
        Assert.Equal(6L, result.Rows[0][1].AsInt64());
        Assert.Equal(15L, result.Rows[0][2].AsInt64());
    }

    [Fact]
    public void ConditionalFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                if(1 = 1, 'yes', 'no') AS if_val,
                ifNull(NULL, 'default') AS ifnull_val,
                nullIf(1, 1) AS nullif_val,
                coalesce(NULL, NULL, 'found', 'notused') AS coalesce_val
        ");

        Assert.Single(result.Rows);
        Assert.Equal("yes", result.Rows[0][0].AsString());
        Assert.Equal("default", result.Rows[0][1].AsString());
        Assert.True(result.Rows[0][2].IsNull);
        Assert.Equal("found", result.Rows[0][3].AsString());
    }

    [Fact]
    public void ArrayFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                array(1, 2, 3) AS arr,
                arrayLength(array(1, 2, 3, 4, 5)) AS len,
                has(array(1, 2, 3), 2) AS has_2,
                indexOf(array('a', 'b', 'c'), 'b') AS index,
                arrayConcat(array(1, 2), array(3, 4)) AS concat_arr
        ");

        Assert.Single(result.Rows);
        Assert.Equal(5L, result.Rows[0][1].AsInt64());
        Assert.True(result.Rows[0][2].AsBool());
        Assert.Equal(2L, result.Rows[0][3].AsInt64());
    }

    [Fact]
    public void RangeFunction()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute("SELECT range(5)");

        Assert.Single(result.Rows);
        var arr = result.Rows[0][0] as ClickHouseSharp.Types.ArrayValue;
        Assert.NotNull(arr);
        Assert.Equal(5, arr.Elements.Count);
    }

    [Fact]
    public void TypeConversionFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                toInt64('123') AS int_val,
                toFloat64('3.14') AS float_val,
                toString(123) AS str_val
        ");

        Assert.Single(result.Rows);
        Assert.Equal(123L, result.Rows[0][0].AsInt64());
        Assert.Equal(3.14, result.Rows[0][1].AsFloat64());
        Assert.Equal("123", result.Rows[0][2].AsString());
    }

    [Fact]
    public void GreatestLeastFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                greatest(1, 5, 3, 9, 2) AS max_val,
                least(1, 5, 3, 9, 2) AS min_val
        ");

        Assert.Single(result.Rows);
        Assert.Equal(9L, result.Rows[0][0].AsInt64());
        Assert.Equal(1L, result.Rows[0][1].AsInt64());
    }

    [Fact]
    public void MultiIfFunction()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                multiIf(1 = 0, 'a', 2 = 0, 'b', 'c') AS result
        ");

        Assert.Single(result.Rows);
        Assert.Equal("c", result.Rows[0][0].AsString());
    }

    [Fact]
    public void SplitFunctions()
    {
        using var ch = new ClickHouseLocal();

        var result = ch.Execute(@"
            SELECT
                splitByChar(',', 'a,b,c') AS split_char,
                splitByString(', ', 'a, b, c') AS split_str,
                arrayStringConcat(array('a', 'b', 'c'), '-') AS joined
        ");

        Assert.Single(result.Rows);
        Assert.Equal("a-b-c", result.Rows[0][2].AsString());
    }
}
