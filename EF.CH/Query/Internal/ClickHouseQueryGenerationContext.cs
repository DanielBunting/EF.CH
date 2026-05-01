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

    /// <summary>
    /// Clears every field. Called at the start of each query's postprocessor pass so
    /// state from a prior query that wasn't fully consumed (e.g. translation threw
    /// before the SQL generator reached the consumption site) cannot leak forward.
    /// </summary>
    public void Reset()
    {
        QuerySettings = null;
        WithFillOptions = null;
        PreWhereExpression = null;
        LimitByLimit = null;
        LimitByOffset = null;
        LimitByExpressions = null;
        GroupByModifier = GroupByModifier.None;
        RawFilter = null;
        CteDefinitions = null;
        ArrayJoinSpecs = null;
        AsofJoin = null;
    }
}
