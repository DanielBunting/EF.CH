using System.Reflection;
using EF.CH.Dictionaries;
using EF.CH.External;
using Xunit;

namespace EF.CH.Tests.External;

/// <summary>
/// Pins the SQL-literal escape used by <c>ExternalConfigResolver</c> and
/// <c>DictionaryConfigResolver</c> when interpolating user-supplied values into
/// the <c>ENGINE = postgresql('host', 'db', 'table', 'user', 'password', 'schema')</c>
/// table-function call and the dictionary <c>SOURCE(...)</c> clause.
///
/// The escape must handle BOTH:
/// <list type="bullet">
///   <item><description>Apostrophe — naively, <c>'</c> closes the literal.</description></item>
///   <item><description>Backslash — ClickHouse interprets <c>\</c> as a C-style escape inside a single-quoted literal, so a value ending in <c>\</c> would escape the closing quote and let the next chunk become syntactically active.</description></item>
/// </list>
///
/// The previous implementation only escaped <c>'</c> → <c>\'</c>, which left the
/// backslash injection vector wide open.
/// </summary>
public class SqlLiteralEscapeTests
{
    private static readonly MethodInfo ExternalEscape =
        typeof(ExternalConfigResolver).GetMethod("Escape", BindingFlags.Static | BindingFlags.NonPublic)!;

    private static readonly MethodInfo DictionaryEscape =
        typeof(DictionaryConfigResolver).GetMethod("EscapeSql", BindingFlags.Static | BindingFlags.NonPublic)!;

    private static string ExternalEscapeOf(string value) =>
        (string)ExternalEscape.Invoke(null, [value])!;

    private static string DictionaryEscapeOf(string value) =>
        (string)DictionaryEscape.Invoke(null, [value])!;

    public static IEnumerable<object[]> InjectionVectors() =>
    [
        // Plain apostrophe — must be escaped.
        ["Bob's", @"Bob\'s"],
        // Trailing backslash — without escaping the backslash, the next char
        // (the closing quote) would be itself escaped.
        [@"password\", @"password\\"],
        // Backslash followed by apostrophe — the original code emitted `\\'`
        // which ClickHouse reads as escaped-backslash followed by terminator.
        [@"a\'b", @"a\\\'b"],
        // Apostrophe followed by backslash.
        [@"a'\b", @"a\'\\b"],
        // Combined: literal newline isn't relevant to the apostrophe/backslash
        // injection class, so no assertion here — covered by the JSON test.
    ];

    [Theory]
    [MemberData(nameof(InjectionVectors))]
    public void ExternalConfigResolver_Escape_HandlesBackslashAndApostrophe(string raw, string expected)
    {
        Assert.Equal(expected, ExternalEscapeOf(raw));
    }

    [Theory]
    [MemberData(nameof(InjectionVectors))]
    public void DictionaryConfigResolver_EscapeSql_HandlesBackslashAndApostrophe(string raw, string expected)
    {
        Assert.Equal(expected, DictionaryEscapeOf(raw));
    }
}
