using System.Reflection;
using EF.CH.Extensions;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Pins the <c>SqlLiteral</c> helper used by <c>UsePostgreSqlEngine</c>,
/// <c>UseMySqlEngine</c>, <c>UseRedisEngine</c>, and <c>UseOdbcEngine</c> when
/// building the engine-args annotation that ends up emitted verbatim in the
/// CREATE TABLE DDL. The previous implementation used the <c>''</c> doubled-
/// quote form for apostrophe escaping but did not escape <c>\</c> first — a
/// value ending in <c>\</c> would escape the closing quote and break out of
/// the literal because ClickHouse interprets <c>\</c> as a C-style escape
/// inside <c>'…'</c>.
/// </summary>
public class EntityBuilderSqlLiteralEscapeTests
{
    private static readonly MethodInfo SqlLiteral =
        typeof(ClickHouseEntityTypeBuilderExtensions).GetMethod("SqlLiteral", BindingFlags.Static | BindingFlags.NonPublic)!;

    private static string Quote(string value) => (string)SqlLiteral.Invoke(null, [value])!;

    [Theory]
    [InlineData("plain", "'plain'")]
    [InlineData("Bob's", "'Bob''s'")]
    [InlineData(@"trailing\", @"'trailing\\'")]
    [InlineData(@"a\'b", @"'a\\''b'")]
    [InlineData(@"a'\b", @"'a''\\b'")]
    public void SqlLiteral_EscapesBackslashThenApostrophe(string raw, string expected)
    {
        Assert.Equal(expected, Quote(raw));
    }
}
