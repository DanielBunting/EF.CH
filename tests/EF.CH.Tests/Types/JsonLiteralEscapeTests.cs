using System.Reflection;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EF.CH.Tests.Types;

/// <summary>
/// Pins the JSON-literal escape used by <c>ClickHouseJsonTypeMapping</c> when
/// embedding a JSON string into a single-quoted SQL literal. The escape must
/// handle control characters (tab, newline, carriage return, etc.) — otherwise
/// a hand-built JSON value containing a literal control char produces a
/// malformed SQL literal that ClickHouse rejects (or worse, parses unexpectedly).
/// </summary>
public class JsonLiteralEscapeTests
{
    private static readonly MethodInfo EscapeJsonString =
        typeof(ClickHouseJsonTypeMapping).GetMethod("EscapeJsonString", BindingFlags.Static | BindingFlags.NonPublic)!;

    private static string Escape(string raw) => (string)EscapeJsonString.Invoke(null, [raw])!;

    [Fact]
    public void Tab_IsEscapedAsBackslashT()
    {
        Assert.Equal(@"a\tb", Escape("a\tb"));
    }

    [Fact]
    public void Newline_IsEscapedAsBackslashN()
    {
        Assert.Equal(@"a\nb", Escape("a\nb"));
    }

    [Fact]
    public void CarriageReturn_IsEscapedAsBackslashR()
    {
        Assert.Equal(@"a\rb", Escape("a\rb"));
    }

    [Fact]
    public void Backspace_IsEscapedAsBackslashB()
    {
        Assert.Equal(@"a\bb", Escape("a\bb"));
    }

    [Fact]
    public void FormFeed_IsEscapedAsBackslashF()
    {
        Assert.Equal(@"a\fb", Escape("a\fb"));
    }

    [Fact]
    public void NullByte_IsEscapedAsBackslashZero()
    {
        Assert.Equal(@"a\0b", Escape("a\0b"));
    }

    [Fact]
    public void Apostrophe_AndBackslash_StillEscape()
    {
        Assert.Equal(@"a\'b", Escape("a'b"));
        Assert.Equal(@"a\\b", Escape("a\\b"));
    }

    [Fact]
    public void RegularCharacters_PassThroughUnchanged()
    {
        Assert.Equal("simple json", Escape("simple json"));
    }
}
