using EF.CH.Metadata;
using Xunit;

namespace EF.CH.Tests.Cluster;

/// <summary>
/// <see cref="ClickHouseClusterMacros.ContainsMacro"/> tells the DDL emitter
/// whether to wrap a cluster name in single quotes (server-side macro) or in
/// backticks (literal identifier). The detection currently fires on any
/// <c>{…}</c> substring; that misclassifies literal cluster names that happen
/// to contain braces, and accepts non-identifier content like <c>{a-b}</c> that
/// the server would reject. Tightening to <c>\{[a-zA-Z_]\w*\}</c> matches
/// every standard ClickHouse macro (<c>{cluster}</c>, <c>{shard}</c>, <c>{replica}</c>,
/// <c>{database}</c>, <c>{table}</c>, <c>{uuid}</c>) and rejects the rest.
/// </summary>
public class ClusterMacroDetectionTests
{
    [Theory]
    [InlineData("{cluster}")]
    [InlineData("{shard}")]
    [InlineData("{replica}")]
    [InlineData("{database}")]
    [InlineData("{table}")]
    [InlineData("{uuid}")]
    [InlineData("prefix_{cluster}_suffix")] // embedded macro inside a templated name
    [InlineData("/clickhouse/tables/{shard}/test")] // typical ZK path
    public void ContainsMacro_ReturnsTrueForRealMacros(string value)
    {
        Assert.True(ClickHouseClusterMacros.ContainsMacro(value), $"expected '{value}' to be detected as a macro");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("my_cluster")]
    [InlineData("primary")]
    [InlineData("cluster-east-1")]
    [InlineData("my{bad-name}cluster")] // hyphen is not part of identifier
    [InlineData("my{1cluster}name")]    // identifiers cannot start with a digit
    [InlineData("{}")]                  // empty braces
    [InlineData("{ space }")]           // whitespace inside
    public void ContainsMacro_ReturnsFalseForLiteralOrInvalid(string? value)
    {
        Assert.False(ClickHouseClusterMacros.ContainsMacro(value), $"expected '{value}' to NOT be detected as a macro");
    }
}
