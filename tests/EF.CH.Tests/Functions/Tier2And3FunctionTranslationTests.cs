using EF.CH.Extensions;
using EF.CH.Tests.Sql;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.Tests.Functions;

/// <summary>
/// Translation tests for Tiers 2 and 3 of the missing-CH-functions rollout:
///   2a — bit functions
///   2b-step-1 — non-lambda array helpers
///   2c — JSON typed extraction
///   2d — random functions
///   2e — server / session metadata
///   3a — tuple ops
///   3b — IPv6 + IP additions
///   3c — UUID v7 helpers
///   3d — math specials
///   3e — string extras
/// </summary>
public class Tier2And3FunctionTranslationTests
{
    // ---- 2a — Bit functions ----

    [Theory]
    [InlineData("BitAnd", "bitAnd(")]
    [InlineData("BitOr", "bitOr(")]
    [InlineData("BitXor", "bitXor(")]
    [InlineData("BitShiftLeft", "bitShiftLeft(")]
    [InlineData("BitShiftRight", "bitShiftRight(")]
    [InlineData("BitRotateLeft", "bitRotateLeft(")]
    [InlineData("BitRotateRight", "bitRotateRight(")]
    [InlineData("BitHammingDistance", "bitHammingDistance(")]
    public void Bit_BinaryOps_EmitChFunction(string method, string expected)
    {
        using var ctx = CreateContext();
        var sql = method switch
        {
            "BitAnd" => ctx.TestEntities.Select(e => EfClass.Functions.BitAnd(e.Value, 7L)).ToQueryString(),
            "BitOr" => ctx.TestEntities.Select(e => EfClass.Functions.BitOr(e.Value, 7L)).ToQueryString(),
            "BitXor" => ctx.TestEntities.Select(e => EfClass.Functions.BitXor(e.Value, 7L)).ToQueryString(),
            "BitShiftLeft" => ctx.TestEntities.Select(e => EfClass.Functions.BitShiftLeft(e.Value, 2)).ToQueryString(),
            "BitShiftRight" => ctx.TestEntities.Select(e => EfClass.Functions.BitShiftRight(e.Value, 2)).ToQueryString(),
            "BitRotateLeft" => ctx.TestEntities.Select(e => EfClass.Functions.BitRotateLeft(e.Value, 2)).ToQueryString(),
            "BitRotateRight" => ctx.TestEntities.Select(e => EfClass.Functions.BitRotateRight(e.Value, 2)).ToQueryString(),
            "BitHammingDistance" => ctx.TestEntities.Select(e => EfClass.Functions.BitHammingDistance(e.Value, 7L)).ToQueryString(),
            _ => throw new InvalidOperationException()
        };
        Assert.Contains(expected, sql);
    }

    [Fact] public void BitNot_Emits_bitNot() { using var ctx = CreateContext(); Assert.Contains("bitNot(", ctx.TestEntities.Select(e => EfClass.Functions.BitNot(e.Value)).ToQueryString()); }
    [Fact] public void BitCount_Emits_bitCount() { using var ctx = CreateContext(); Assert.Contains("bitCount(", ctx.TestEntities.Select(e => EfClass.Functions.BitCount(e.Value)).ToQueryString()); }
    [Fact] public void BitTest_Emits_bitTest() { using var ctx = CreateContext(); Assert.Contains("bitTest(", ctx.TestEntities.Select(e => EfClass.Functions.BitTest(e.Value, 3)).ToQueryString()); }
    [Fact] public void BitSlice_Emits_bitSlice() { using var ctx = CreateContext(); Assert.Contains("bitSlice(", ctx.TestEntities.Select(e => EfClass.Functions.BitSlice(e.Value, 0, 8)).ToQueryString()); }

    // ---- 2c — JSON typed extraction ----

    [Theory]
    [InlineData("Int", "JSONExtractInt(")]
    [InlineData("Float", "JSONExtractFloat(")]
    [InlineData("Bool", "JSONExtractBool(")]
    [InlineData("String", "JSONExtractString(")]
    [InlineData("Raw", "JSONExtractRaw(")]
    [InlineData("Has", "JSONHas(")]
    [InlineData("Length", "JSONLength(")]
    [InlineData("Type", "JSONType(")]
    public void JsonExtract_Variants_EmitChFunction(string variant, string expected)
    {
        using var ctx = CreateContext();
        var sql = variant switch
        {
            "Int" => ctx.TestEntities.Select(e => EfClass.Functions.JSONExtractInt(e.Name, "$.x")).ToQueryString(),
            "Float" => ctx.TestEntities.Select(e => EfClass.Functions.JSONExtractFloat(e.Name, "$.x")).ToQueryString(),
            "Bool" => ctx.TestEntities.Select(e => EfClass.Functions.JSONExtractBool(e.Name, "$.x")).ToQueryString(),
            "String" => ctx.TestEntities.Select(e => EfClass.Functions.JSONExtractString(e.Name, "$.x")).ToQueryString(),
            "Raw" => ctx.TestEntities.Select(e => EfClass.Functions.JSONExtractRaw(e.Name, "$.x")).ToQueryString(),
            "Has" => ctx.TestEntities.Select(e => EfClass.Functions.JSONHas(e.Name, "$.x")).ToQueryString(),
            "Length" => ctx.TestEntities.Select(e => EfClass.Functions.JSONLength(e.Name, "$.x")).ToQueryString(),
            "Type" => ctx.TestEntities.Select(e => EfClass.Functions.JSONType(e.Name, "$.x")).ToQueryString(),
            _ => throw new InvalidOperationException()
        };
        Assert.Contains(expected, sql);
    }

    [Fact]
    public void IsValidJSON_Emits_isValidJSON()
    {
        using var ctx = CreateContext();
        var sql = ctx.TestEntities.Where(e => EfClass.Functions.IsValidJSON(e.Name)).ToQueryString();
        Assert.Contains("isValidJSON(", sql);
    }

    // ---- 2d — Random ----

    [Theory]
    [InlineData("Rand", "rand()")]
    [InlineData("Rand64", "rand64()")]
    [InlineData("RandCanonical", "randCanonical()")]
    public void Random_NoArg_EmitChFunction(string variant, string expected)
    {
        using var ctx = CreateContext();
        var sql = variant switch
        {
            "Rand" => ctx.TestEntities.Select(e => EfClass.Functions.Rand()).ToQueryString(),
            "Rand64" => ctx.TestEntities.Select(e => EfClass.Functions.Rand64()).ToQueryString(),
            "RandCanonical" => ctx.TestEntities.Select(e => EfClass.Functions.RandCanonical()).ToQueryString(),
            _ => throw new InvalidOperationException()
        };
        Assert.Contains(expected, sql);
    }

    [Fact] public void RandomString_Emits_randomString() { using var ctx = CreateContext(); Assert.Contains("randomString(", ctx.TestEntities.Select(e => EfClass.Functions.RandomString(8)).ToQueryString()); }
    [Fact] public void RandUniform_Emits_randUniform() { using var ctx = CreateContext(); Assert.Contains("randUniform(", ctx.TestEntities.Select(e => EfClass.Functions.RandUniform(0.0, 1.0)).ToQueryString()); }
    [Fact] public void RandNormal_Emits_randNormal() { using var ctx = CreateContext(); Assert.Contains("randNormal(", ctx.TestEntities.Select(e => EfClass.Functions.RandNormal(0.0, 1.0)).ToQueryString()); }

    // ---- 2e — Server metadata ----

    [Theory]
    [InlineData("Version", "version()")]
    [InlineData("HostName", "hostName()")]
    [InlineData("CurrentDatabase", "currentDatabase()")]
    [InlineData("CurrentUser", "currentUser()")]
    [InlineData("ServerTimezone", "serverTimezone()")]
    [InlineData("ServerUUID", "serverUUID()")]
    [InlineData("Uptime", "uptime()")]
    public void Server_Metadata_EmitChFunction(string variant, string expected)
    {
        using var ctx = CreateContext();
        var sql = variant switch
        {
            "Version" => ctx.TestEntities.Select(e => EfClass.Functions.Version()).ToQueryString(),
            "HostName" => ctx.TestEntities.Select(e => EfClass.Functions.HostName()).ToQueryString(),
            "CurrentDatabase" => ctx.TestEntities.Select(e => EfClass.Functions.CurrentDatabase()).ToQueryString(),
            "CurrentUser" => ctx.TestEntities.Select(e => EfClass.Functions.CurrentUser()).ToQueryString(),
            "ServerTimezone" => ctx.TestEntities.Select(e => EfClass.Functions.ServerTimezone()).ToQueryString(),
            "ServerUUID" => ctx.TestEntities.Select(e => EfClass.Functions.ServerUUID()).ToQueryString(),
            "Uptime" => ctx.TestEntities.Select(e => EfClass.Functions.Uptime()).ToQueryString(),
            _ => throw new InvalidOperationException()
        };
        Assert.Contains(expected, sql);
    }

    // ---- 3a — Tuple ops ----

    [Fact] public void DotProduct_Emits_dotProduct() { using var ctx = CreateContext(); Assert.Contains("dotProduct(", ctx.TestEntities.Select(e => EfClass.Functions.DotProduct(e.Name, e.Name)).ToQueryString()); }
    [Fact] public void TupleHammingDistance_Emits_tupleHammingDistance() { using var ctx = CreateContext(); Assert.Contains("tupleHammingDistance(", ctx.TestEntities.Select(e => EfClass.Functions.TupleHammingDistance(e.Name, e.Name)).ToQueryString()); }
    // TuplePlus / TupleMinus / TupleMultiply / TupleDivide / TupleNegate / FlattenTuple all
    // return `object` (CH tuple type — no clean .NET equivalent). EF Core can't infer a type
    // mapping for an `object`-typed return on a synthetic test column, so the call gets
    // collapsed before reaching the translator. They're still wired in the dictionary; a
    // real-tuple integration test against a CH server would exercise the path properly.

    // ---- 3b — IPv6 ----

    [Theory]
    [InlineData("IPv6StringToNum", "IPv6StringToNum(")]
    [InlineData("ToIPv4", "toIPv4(")]
    [InlineData("ToIPv6", "toIPv6(")]
    public void Ipv6_StringInput_EmitChFunction(string variant, string expected)
    {
        using var ctx = CreateContext();
        var sql = variant switch
        {
            "IPv6StringToNum" => ctx.TestEntities.Select(e => EfClass.Functions.IPv6StringToNum(e.Name)).ToQueryString(),
            "ToIPv4" => ctx.TestEntities.Select(e => EfClass.Functions.ToIPv4(e.Name)).ToQueryString(),
            "ToIPv6" => ctx.TestEntities.Select(e => EfClass.Functions.ToIPv6(e.Name)).ToQueryString(),
            _ => throw new InvalidOperationException()
        };
        Assert.Contains(expected, sql);
    }

    // ---- 3c — UUID v7 helpers ----

    [Fact] public void ToUUIDOrZero_Emits_toUUIDOrZero() { using var ctx = CreateContext(); Assert.Contains("toUUIDOrZero(", ctx.TestEntities.Select(e => EfClass.Functions.ToUUIDOrZero(e.Name)).ToQueryString()); }
    [Fact]
    public void DateTimeToUUIDv7_EvaluatesClientSide_AndBindsAsParameter()
    {
        using var ctx = CreateContext();
        // Client-side evaluation requires a constant DateTime (not a column reference)
        // so EF Core can fold the call into a parameter. Use a fixed instant.
        var fixedDt = new DateTime(2025, 6, 15, 12, 30, 45, DateTimeKind.Utc);
        var sql = ctx.TestEntities
            .Select(e => EfClass.Functions.DateTimeToUUIDv7(fixedDt))
            .ToQueryString();
        // The function should not appear in the emitted SQL — it ran client-side
        // and the resulting Guid lands as a parameter.
        Assert.DoesNotContain("dateTimeToUUIDv7(", sql);
    }
    [Fact] public void UUIDv7ToDateTime_Emits_UUIDv7ToDateTime() { using var ctx = CreateContext(); Assert.Contains("UUIDv7ToDateTime(", ctx.TestEntities.Select(e => EfClass.Functions.UUIDv7ToDateTime(e.Id)).ToQueryString()); }

    // ---- 3d — Math specials ----

    [Theory]
    [InlineData("Pi", "pi()")]
    [InlineData("E", "e()")]
    public void Math_NoArg_Constants_EmitChFunction(string variant, string expected)
    {
        using var ctx = CreateContext();
        var sql = variant switch
        {
            "Pi" => ctx.TestEntities.Select(e => EfClass.Functions.Pi()).ToQueryString(),
            "E" => ctx.TestEntities.Select(e => EfClass.Functions.E()).ToQueryString(),
            _ => throw new InvalidOperationException()
        };
        Assert.Contains(expected, sql);
    }

    [Theory]
    [InlineData("Degrees", "degrees(")]
    [InlineData("Radians", "radians(")]
    [InlineData("Erf", "erf(")]
    [InlineData("Erfc", "erfc(")]
    [InlineData("Lgamma", "lgamma(")]
    [InlineData("Tgamma", "tgamma(")]
    [InlineData("Sigmoid", "sigmoid(")]
    [InlineData("Log1P", "log1p(")]
    public void Math_UnaryDouble_EmitChFunction(string variant, string expected)
    {
        using var ctx = CreateContext();
        var sql = variant switch
        {
            "Degrees" => ctx.TestEntities.Select(e => EfClass.Functions.Degrees(e.DoubleValue)).ToQueryString(),
            "Radians" => ctx.TestEntities.Select(e => EfClass.Functions.Radians(e.DoubleValue)).ToQueryString(),
            "Erf" => ctx.TestEntities.Select(e => EfClass.Functions.Erf(e.DoubleValue)).ToQueryString(),
            "Erfc" => ctx.TestEntities.Select(e => EfClass.Functions.Erfc(e.DoubleValue)).ToQueryString(),
            "Lgamma" => ctx.TestEntities.Select(e => EfClass.Functions.Lgamma(e.DoubleValue)).ToQueryString(),
            "Tgamma" => ctx.TestEntities.Select(e => EfClass.Functions.Tgamma(e.DoubleValue)).ToQueryString(),
            "Sigmoid" => ctx.TestEntities.Select(e => EfClass.Functions.Sigmoid(e.DoubleValue)).ToQueryString(),
            "Log1P" => ctx.TestEntities.Select(e => EfClass.Functions.Log1P(e.DoubleValue)).ToQueryString(),
            _ => throw new InvalidOperationException()
        };
        Assert.Contains(expected, sql);
    }

    [Fact] public void Factorial_Emits_factorial() { using var ctx = CreateContext(); Assert.Contains("factorial(", ctx.TestEntities.Select(e => EfClass.Functions.Factorial(e.Value)).ToQueryString()); }
    [Fact] public void RoundBankers_Emits_roundBankers() { using var ctx = CreateContext(); Assert.Contains("roundBankers(", ctx.TestEntities.Select(e => EfClass.Functions.RoundBankers(e.DoubleValue, 2)).ToQueryString()); }
    [Fact] public void WidthBucket_Emits_widthBucket() { using var ctx = CreateContext(); Assert.Contains("widthBucket(", ctx.TestEntities.Select(e => EfClass.Functions.WidthBucket(e.DoubleValue, 0.0, 100.0, 10)).ToQueryString()); }
    [Fact] public void Hypot_Emits_hypot() { using var ctx = CreateContext(); Assert.Contains("hypot(", ctx.TestEntities.Select(e => EfClass.Functions.Hypot(e.DoubleValue, e.DoubleValue)).ToQueryString()); }

    // ---- 3e — String extras ----

    [Theory]
    [InlineData("Left", "leftUTF8(")]
    [InlineData("Right", "rightUTF8(")]
    [InlineData("Repeat", "repeat(")]
    [InlineData("Reverse", "reverseUTF8(")]
    [InlineData("InitCap", "initcapUTF8(")]
    public void String_Extras_EmitChFunction(string variant, string expected)
    {
        using var ctx = CreateContext();
        var sql = variant switch
        {
            "Left" => ctx.TestEntities.Select(e => EfClass.Functions.Left(e.Name, 5)).ToQueryString(),
            "Right" => ctx.TestEntities.Select(e => EfClass.Functions.Right(e.Name, 5)).ToQueryString(),
            "Repeat" => ctx.TestEntities.Select(e => EfClass.Functions.Repeat(e.Name, 3)).ToQueryString(),
            "Reverse" => ctx.TestEntities.Select(e => EfClass.Functions.Reverse(e.Name)).ToQueryString(),
            "InitCap" => ctx.TestEntities.Select(e => EfClass.Functions.InitCap(e.Name)).ToQueryString(),
            _ => throw new InvalidOperationException()
        };
        Assert.Contains(expected, sql);
    }

    [Fact] public void LeftPad_Emits_leftPad() { using var ctx = CreateContext(); Assert.Contains("leftPad(", ctx.TestEntities.Select(e => EfClass.Functions.LeftPad(e.Name, 10, "*")).ToQueryString()); }
    [Fact] public void RightPad_Emits_rightPad() { using var ctx = CreateContext(); Assert.Contains("rightPad(", ctx.TestEntities.Select(e => EfClass.Functions.RightPad(e.Name, 10, "*")).ToQueryString()); }
    [Fact] public void Space_Emits_space() { using var ctx = CreateContext(); Assert.Contains("space(", ctx.TestEntities.Select(e => EfClass.Functions.Space(e.Value)).ToQueryString()); }

    // ---- 2b-step-1 — Array helpers ----

    [Theory]
    [InlineData("ArrayDistinct", "arrayDistinct(")]
    [InlineData("ArrayUniq", "arrayUniq(")]
    [InlineData("ArrayCompact", "arrayCompact(")]
    [InlineData("ArraySort", "arraySort(")]
    [InlineData("ArrayReverseSort", "arrayReverseSort(")]
    [InlineData("ArrayReverse", "arrayReverse(")]
    [InlineData("ArrayPopBack", "arrayPopBack(")]
    [InlineData("ArrayPopFront", "arrayPopFront(")]
    [InlineData("ArrayEnumerate", "arrayEnumerate(")]
    public void Array_UnaryHelpers_EmitChFunction(string variant, string expected)
    {
        using var ctx = CreateContext();
        var arr = new[] { 1, 2, 3 };
        var sql = variant switch
        {
            "ArrayDistinct" => ctx.TestEntities.Select(e => EfClass.Functions.ArrayDistinct(arr)).ToQueryString(),
            "ArrayUniq" => ctx.TestEntities.Select(e => EfClass.Functions.ArrayUniq(arr)).ToQueryString(),
            "ArrayCompact" => ctx.TestEntities.Select(e => EfClass.Functions.ArrayCompact(arr)).ToQueryString(),
            "ArraySort" => ctx.TestEntities.Select(e => EfClass.Functions.ArraySort(arr)).ToQueryString(),
            "ArrayReverseSort" => ctx.TestEntities.Select(e => EfClass.Functions.ArrayReverseSort(arr)).ToQueryString(),
            "ArrayReverse" => ctx.TestEntities.Select(e => EfClass.Functions.ArrayReverse(arr)).ToQueryString(),
            "ArrayPopBack" => ctx.TestEntities.Select(e => EfClass.Functions.ArrayPopBack(arr)).ToQueryString(),
            "ArrayPopFront" => ctx.TestEntities.Select(e => EfClass.Functions.ArrayPopFront(arr)).ToQueryString(),
            "ArrayEnumerate" => ctx.TestEntities.Select(e => EfClass.Functions.ArrayEnumerate(arr)).ToQueryString(),
            _ => throw new InvalidOperationException()
        };
        Assert.Contains(expected, sql);
    }

    [Fact]
    public void ArrayConcat_Emits_arrayConcat()
    {
        using var ctx = CreateContext();
        var a = new[] { 1, 2 }; var b = new[] { 3, 4 };
        Assert.Contains("arrayConcat(", ctx.TestEntities.Select(e => EfClass.Functions.ArrayConcat(a, b)).ToQueryString());
    }

    [Fact]
    public void ArraySlice_Emits_arraySlice()
    {
        using var ctx = CreateContext();
        var a = new[] { 1, 2, 3, 4 };
        Assert.Contains("arraySlice(", ctx.TestEntities.Select(e => EfClass.Functions.ArraySlice(a, 2, 2)).ToQueryString());
    }

    [Fact]
    public void IndexOf_Emits_indexOf()
    {
        using var ctx = CreateContext();
        var a = new[] { 1, 2, 3 };
        Assert.Contains("indexOf(", ctx.TestEntities.Select(e => EfClass.Functions.IndexOf(a, 2)).ToQueryString());
    }

    [Fact]
    public void ArrayElement_Emits_arrayElement()
    {
        using var ctx = CreateContext();
        var a = new[] { 1, 2, 3 };
        Assert.Contains("arrayElement(", ctx.TestEntities.Select(e => EfClass.Functions.ArrayElement(a, 2)).ToQueryString());
    }

    [Fact]
    public void ArrayPushBack_Emits_arrayPushBack()
    {
        using var ctx = CreateContext();
        var a = new[] { 1, 2 };
        Assert.Contains("arrayPushBack(", ctx.TestEntities.Select(e => EfClass.Functions.ArrayPushBack(a, 3)).ToQueryString());
    }

    [Fact]
    public void ArrayResize_Emits_arrayResize()
    {
        using var ctx = CreateContext();
        var a = new[] { 1, 2 };
        Assert.Contains("arrayResize(", ctx.TestEntities.Select(e => EfClass.Functions.ArrayResize(a, 5, 0)).ToQueryString());
    }

    [Fact]
    public void ArrayFlatten_Emits_arrayFlatten()
    {
        using var ctx = CreateContext();
        var a = new[] { new[] { 1, 2 }, new[] { 3 } };
        Assert.Contains("arrayFlatten(", ctx.TestEntities.Select(e => EfClass.Functions.ArrayFlatten(a)).ToQueryString());
    }

    [Fact]
    public void ArrayIntersect_Emits_arrayIntersect()
    {
        using var ctx = CreateContext();
        var a = new[] { 1, 2 }; var b = new[] { 2, 3 };
        Assert.Contains("arrayIntersect(", ctx.TestEntities.Select(e => EfClass.Functions.ArrayIntersect(a, b)).ToQueryString());
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return new TestDbContext(options);
    }
}
