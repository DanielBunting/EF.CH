namespace EF.CH.Metadata.Internal;

/// <summary>
/// ClickHouse table engine name constants used in migrations, annotation code generation,
/// fluent API configuration, and scaffolding.
/// </summary>
/// <remarks>
/// Replication is a property of the engine — call
/// <c>WithReplication(...)</c> on any MergeTree-family builder to mark the
/// table replicated. The <c>Replicated</c> prefix is applied at SQL-generation
/// time based on the <c>ClickHouseAnnotationNames.IsReplicated</c> annotation,
/// so this class only declares the base engine names.
/// </remarks>
public static class ClickHouseEngineNames
{
    public const string MergeTree = "MergeTree";
    public const string ReplacingMergeTree = "ReplacingMergeTree";
    public const string SummingMergeTree = "SummingMergeTree";
    public const string AggregatingMergeTree = "AggregatingMergeTree";
    public const string CollapsingMergeTree = "CollapsingMergeTree";
    public const string VersionedCollapsingMergeTree = "VersionedCollapsingMergeTree";
    public const string GraphiteMergeTree = "GraphiteMergeTree";

    // Non-MergeTree engines
    public const string Null = "Null";
    public const string Memory = "Memory";
    public const string Log = "Log";
    public const string TinyLog = "TinyLog";
    public const string StripeLog = "StripeLog";
    public const string Distributed = "Distributed";
    public const string KeeperMap = "KeeperMap";

    // External / integration engines.
    public const string PostgreSQL = "PostgreSQL";
    public const string MySQL = "MySQL";
    public const string Redis = "Redis";
    public const string ODBC = "ODBC";

    public static bool IsMergeTreeFamily(string engine)
        => engine.EndsWith(MergeTree, StringComparison.OrdinalIgnoreCase);
}
