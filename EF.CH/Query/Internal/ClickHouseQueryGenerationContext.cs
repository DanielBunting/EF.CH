using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Per-query state passed from <see cref="ClickHouseQueryTranslationPostprocessor"/>
/// to <see cref="ClickHouseQuerySqlGenerator"/> via <see cref="ClickHouseQueryStateRegistry"/>.
/// Holds ClickHouse-specific query options (SETTINGS, PREWHERE, CTEs, LIMIT BY,
/// etc.) plus the model's EPHEMERAL column set. Each query gets its own
/// instance — no reuse, no Reset.
/// </summary>
internal sealed class ClickHouseQueryGenerationContext
{
    public Dictionary<string, object>? QuerySettings;
    public ClickHouseQueryCompilationContextOptions? WithFillOptions;
    public SqlExpression? PreWhereExpression;
    public int? LimitByLimit;
    public int? LimitByOffset;
    public List<SqlExpression>? LimitByExpressions;
    public GroupByModifier GroupByModifier;
    public string? RawFilter;
    public List<CteDefinition>? CteDefinitions;
    public List<ArrayJoinSpec>? ArrayJoinSpecs;
    public AsofJoinInfo? AsofJoin;
    public HashSet<string>? EphemeralColumns;
}
