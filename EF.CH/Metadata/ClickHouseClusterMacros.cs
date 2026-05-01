using System.Text.RegularExpressions;

namespace EF.CH.Metadata;

/// <summary>
/// Standard ClickHouse server-side macros that can be used in place of literal
/// cluster names, ZooKeeper paths, and replica names. Macros are defined in the
/// ClickHouse server's <c>&lt;macros&gt;</c> config and expanded at DDL execution time.
/// </summary>
/// <remarks>
/// When one of these tokens appears in an <c>ON CLUSTER</c> clause, EF.CH emits
/// it as a single-quoted string literal (e.g. <c>ON CLUSTER '{cluster}'</c>) so
/// the server performs macro substitution. Literal cluster names are still emitted
/// as delimited identifiers.
/// </remarks>
public static class ClickHouseClusterMacros
{
    /// <summary>
    /// The <c>{cluster}</c> macro — resolved server-side from the node's
    /// <c>&lt;macros&gt;&lt;cluster&gt;...&lt;/cluster&gt;&lt;/macros&gt;</c>
    /// config entry. Lets the same DDL run against different clusters without
    /// hardcoding a cluster name.
    /// </summary>
    public const string Cluster = "{cluster}";

    /// <summary>
    /// The <c>{shard}</c> macro — typically used inside replicated engine paths.
    /// </summary>
    public const string Shard = "{shard}";

    /// <summary>
    /// The <c>{replica}</c> macro — typically used as the replica-name argument
    /// of <c>ReplicatedMergeTree</c>.
    /// </summary>
    public const string Replica = "{replica}";

    /// <summary>
    /// Returns true when <paramref name="value"/> contains a ClickHouse server-side
    /// macro of the form <c>{name}</c> where <c>name</c> is a valid identifier
    /// (alphanumeric or underscore, not starting with a digit). Macro-containing
    /// names must be emitted as string literals so the server performs substitution;
    /// literal cluster names — including names that happen to contain unrelated
    /// braces like <c>my{bad-name}cluster</c> — must stay as backticked identifiers.
    /// </summary>
    public static bool ContainsMacro(string? value)
    {
        if (string.IsNullOrEmpty(value)) return false;
        return MacroPattern.IsMatch(value);
    }

    private static readonly Regex MacroPattern = new(
        @"\{[A-Za-z_][A-Za-z0-9_]*\}",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Formats a cluster name for emission inside an <c>ON CLUSTER …</c> clause.
    /// Macros (anything matching <see cref="ContainsMacro"/>) are emitted as
    /// single-quoted string literals so the server performs substitution; literal
    /// names are emitted as backtick-quoted identifiers. Returns the empty string
    /// when <paramref name="clusterName"/> is null or empty so callers can
    /// concatenate unconditionally.
    /// </summary>
    public static string FormatOnClusterClause(string? clusterName)
    {
        if (string.IsNullOrEmpty(clusterName)) return string.Empty;
        var formatted = ContainsMacro(clusterName)
            ? $"'{clusterName.Replace("'", "''")}'"
            : $"`{clusterName.Replace("`", "``")}`";
        return $" ON CLUSTER {formatted}";
    }
}
