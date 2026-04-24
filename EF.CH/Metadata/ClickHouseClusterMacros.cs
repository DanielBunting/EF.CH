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
    /// Returns true when <paramref name="value"/> contains a ClickHouse macro
    /// (any <c>{name}</c> token). Macro-containing names must be emitted as
    /// string literals, not backticked identifiers.
    /// </summary>
    public static bool ContainsMacro(string? value)
    {
        if (string.IsNullOrEmpty(value)) return false;
        var open = value.IndexOf('{');
        if (open < 0) return false;
        var close = value.IndexOf('}', open + 1);
        return close > open + 1;
    }
}
