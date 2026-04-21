namespace EF.CH.Metadata.Internal;

/// <summary>
/// ClickHouse table engine name constants used in migrations, annotation code generation,
/// fluent API configuration, and scaffolding.
/// </summary>
public static class ClickHouseEngineNames
{
    public const string MergeTree = "MergeTree";
    public const string ReplacingMergeTree = "ReplacingMergeTree";
    public const string SummingMergeTree = "SummingMergeTree";
    public const string AggregatingMergeTree = "AggregatingMergeTree";
    public const string CollapsingMergeTree = "CollapsingMergeTree";
    public const string VersionedCollapsingMergeTree = "VersionedCollapsingMergeTree";
    public const string GraphiteMergeTree = "GraphiteMergeTree";

    public const string ReplicatedMergeTree = "ReplicatedMergeTree";
    public const string ReplicatedReplacingMergeTree = "ReplicatedReplacingMergeTree";
    public const string ReplicatedSummingMergeTree = "ReplicatedSummingMergeTree";
    public const string ReplicatedAggregatingMergeTree = "ReplicatedAggregatingMergeTree";
    public const string ReplicatedCollapsingMergeTree = "ReplicatedCollapsingMergeTree";
    public const string ReplicatedVersionedCollapsingMergeTree = "ReplicatedVersionedCollapsingMergeTree";

    // Non-MergeTree engines
    public const string Null = "Null";
    public const string Memory = "Memory";
    public const string Log = "Log";
    public const string TinyLog = "TinyLog";
    public const string StripeLog = "StripeLog";
    public const string Distributed = "Distributed";
    public const string KeeperMap = "KeeperMap";

    public const string MergeTreeSuffix = "MergeTree";
}
