using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Thread-local state passed from the query postprocessor to the SQL generator.
/// Holds ClickHouse-specific query options (SETTINGS, PREWHERE, CTEs, LIMIT BY, etc.).
/// Fields are populated by <see cref="ClickHouseQueryTranslationPostprocessor"/> and
/// consumed (read-and-cleared) by <see cref="ClickHouseQuerySqlGenerator"/> during generation.
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
}
