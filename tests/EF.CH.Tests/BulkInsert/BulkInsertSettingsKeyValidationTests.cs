using System.Reflection;
using EF.CH.BulkInsert.Internal;
using Xunit;

namespace EF.CH.Tests.BulkInsert;

/// <summary>
/// Pins SETTINGS-key validation in <see cref="BulkInsertSqlBuilder"/>.
/// Setting keys are emitted raw into <c>INSERT INTO … SETTINGS &lt;key&gt; = …</c>;
/// without validation, a key like <c>"foo, x = (SELECT 1) --"</c> injects
/// arbitrary SQL into the bulk insert. Keys must be plain identifiers.
/// </summary>
public class BulkInsertSettingsKeyValidationTests
{
    private static readonly MethodInfo AppendSettings =
        typeof(BulkInsertSqlBuilder).GetMethod("AppendSettings", BindingFlags.Static | BindingFlags.NonPublic)!;

    private static void Append(System.Text.StringBuilder sb, Dictionary<string, object> settings) =>
        AppendSettings.Invoke(null, [sb, settings]);

    [Theory]
    [InlineData("max_threads")]
    [InlineData("optimize_read_in_order")]
    [InlineData("_leading_underscore")]
    [InlineData("digits_after_123")]
    public void ValidIdentifierKeys_AreAccepted(string key)
    {
        var sb = new System.Text.StringBuilder();
        var settings = new Dictionary<string, object> { [key] = 4 };
        Append(sb, settings);
        Assert.Contains($"{key} = ", sb.ToString());
    }

    [Theory]
    [InlineData("foo, x = (SELECT 1) --")]
    [InlineData("with space")]
    [InlineData("1starts_with_digit")]
    [InlineData("hyphen-key")]
    [InlineData("bad'quote")]
    [InlineData("")]
    public void InvalidKeys_ThrowArgumentException(string key)
    {
        var sb = new System.Text.StringBuilder();
        var settings = new Dictionary<string, object> { [key] = 4 };
        var ex = Assert.Throws<TargetInvocationException>(() => Append(sb, settings));
        Assert.IsType<ArgumentException>(ex.InnerException);
    }
}
