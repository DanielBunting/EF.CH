using System.Linq.Expressions;
using System.Text;
using EF.CH.External;
using EF.CH.Metadata;
using EF.CH.Metadata.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Post-processes translated queries to apply ClickHouse-specific modifiers (FINAL, SAMPLE, SETTINGS)
/// and to rewrite external entity tables to use table functions.
/// Runs after query translation but before SQL generation.
/// </summary>
public class ClickHouseQueryTranslationPostprocessor : RelationalQueryTranslationPostprocessor
{
    private readonly RelationalQueryCompilationContext _queryCompilationContext;
    private readonly IExternalConfigResolver _externalConfigResolver;

    public ClickHouseQueryTranslationPostprocessor(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext,
        IExternalConfigResolver externalConfigResolver)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _queryCompilationContext = queryCompilationContext;
        _externalConfigResolver = externalConfigResolver;
    }

    public override Expression Process(Expression query)
    {
        // Set up default-for-null mappings for null comparison rewriting in SQL nullability processor
        ClickHouseSqlNullabilityProcessor.SetDefaultForNullMappings(_queryCompilationContext.Model);

        // Build a per-query state container. Anything the SQL generator needs
        // (SETTINGS, PREWHERE, CTEs, LIMIT BY, ARRAY JOIN, ASOF JOIN, ephemeral
        // columns, ...) flows through this. Attached to the outer SelectExpression
        // via QueryCompilationContext.Tags + ClickHouseQueryStateRegistry below.
        var ctx = new ClickHouseQueryGenerationContext
        {
            EphemeralColumns = CollectEphemeralColumns(_queryCompilationContext.Model),
        };

        string? tag = null;
        try
        {
            // Let base class do standard processing first
            query = base.Process(query);

            // Get ClickHouse options set during translation phase
            var options = _queryCompilationContext.QueryCompilationContextOptions();

            // Apply dictionary table function rewrites FIRST
            query = new ClickHouseDictionaryTableFunctionVisitor(
                _queryCompilationContext.Model).Visit(query);

            // Apply external table function rewrites (before FINAL/SAMPLE, which don't apply to external tables)
            query = new ClickHouseExternalTableFunctionVisitor(
                _queryCompilationContext.Model,
                _externalConfigResolver).Visit(query);

            // Apply FINAL/SAMPLE modifiers to table expressions (only applies to native ClickHouse tables)
            if (options.UseFinal || options.SampleFraction.HasValue)
            {
                query = new ClickHouseTableModifierApplyingVisitor(options, _queryCompilationContext.Model).Visit(query);
            }

            if (options.QuerySettings.Count > 0)
            {
                ctx.QuerySettings = new Dictionary<string, object>(options.QuerySettings);
            }

            if (options.HasWithFill || options.HasInterpolate)
            {
                ctx.WithFillOptions = options;
            }

            if (options.PreWhereExpression != null)
            {
                ctx.PreWhereExpression = options.PreWhereExpression;
            }

            if (options.HasLimitBy)
            {
                ctx.LimitByLimit = options.LimitByLimit!.Value;
                ctx.LimitByOffset = options.LimitByOffset;
                ctx.LimitByExpressions = options.LimitByExpressions!;
            }

            if (options.GroupByModifier != GroupByModifier.None)
            {
                ctx.GroupByModifier = options.GroupByModifier;
            }

            if (options.RawFilterSql != null)
            {
                ctx.RawFilter = options.RawFilterSql;
            }

            // Extract CTE if AsCte() was called — must happen before reading CteDefinitions.
            if (options.PendingCteName != null)
            {
                query = new ClickHouseCteExtractionVisitor(options).Visit(query);
            }

            if (options.HasCtes)
            {
                ctx.CteDefinitions = new List<CteDefinition>(options.CteDefinitions);
            }

            if (options.HasArrayJoin)
            {
                ctx.ArrayJoinSpecs = new List<ArrayJoinSpec>(options.ArrayJoinSpecs);
            }

            if (options.AsofJoin != null)
            {
                ctx.AsofJoin = options.AsofJoin;
            }

            // Attach the state-pointer tag to QueryCompilationContext.Tags. EF Core's
            // RelationalShapedQueryCompilingExpressionVisitor.ShaperProcessingExpressionVisitor
            // copies queryCompilationContext.Tags onto the outer SelectExpression
            // (via ApplyTags), so the tag rides through to the SQL generator. Adding
            // directly to outerSelect.Tags would be clobbered by that ApplyTags overwrite.
            tag = ClickHouseQueryStateRegistry.Register(ctx);
            _queryCompilationContext.AddTag(tag);
            tag = null; // ownership passed; clear so the catch below doesn't double-discard.

            return query;
        }
        catch
        {
            // Translation threw after we registered. Free the registry entry so it
            // doesn't leak for the lifetime of the process.
            if (tag is not null)
            {
                ClickHouseQueryStateRegistry.Discard(tag);
            }
            throw;
        }
    }

    /// <summary>
    /// Walks the model and collects column names for every property that
    /// carries the EPHEMERAL annotation. The resulting set is passed to the
    /// SQL generator which rewrites matching column references to <c>NULL</c>.
    /// </summary>
    private static HashSet<string>? CollectEphemeralColumns(IModel model)
    {
        HashSet<string>? result = null;

        foreach (var entityType in model.GetEntityTypes())
        {
            foreach (var property in entityType.GetProperties())
            {
                if (property.FindAnnotation(ClickHouseAnnotationNames.EphemeralExpression) is null)
                    continue;

                result ??= new HashSet<string>(StringComparer.Ordinal);
                result.Add(property.GetColumnName() ?? property.Name);
            }
        }

        return result;
    }
}

/// <summary>
/// Visits SelectExpression trees and wraps TableExpression instances with ClickHouseTableModifierExpression.
/// This applies FINAL and SAMPLE modifiers to all tables in the query.
/// </summary>
internal class ClickHouseTableModifierApplyingVisitor : ExpressionVisitor
{
    private readonly ClickHouseQueryCompilationContextOptions _options;
    private readonly IModel _model;

    public ClickHouseTableModifierApplyingVisitor(
        ClickHouseQueryCompilationContextOptions options,
        IModel model)
    {
        _options = options;
        _model = model;
    }

    protected override Expression VisitExtension(Expression node)
    {
        // Wrap TableExpression with our modifier expression
        if (node is TableExpression tableExpression)
        {
            if (_options.UseFinal)
            {
                ValidateFinalEngine(tableExpression);
            }

            if (_options.SampleFraction.HasValue)
            {
                ValidateSampleableTable(tableExpression);
            }

            return new ClickHouseTableModifierExpression(
                tableExpression,
                _options.UseFinal,
                _options.SampleFraction,
                _options.SampleOffset);
        }

        // Handle ShapedQueryExpression specially - it doesn't support VisitChildren
        if (node is ShapedQueryExpression shapedQuery)
        {
            var newQueryExpression = Visit(shapedQuery.QueryExpression);
            var newShaperExpression = Visit(shapedQuery.ShaperExpression);

            if (newQueryExpression != shapedQuery.QueryExpression ||
                newShaperExpression != shapedQuery.ShaperExpression)
            {
                return shapedQuery.Update(newQueryExpression, newShaperExpression);
            }

            return shapedQuery;
        }

        // EnumerableExpression.VisitChildren throws in EF Core 10+
        // Return it unchanged to prevent the exception
        if (node is EnumerableExpression)
        {
            return node;
        }

        // For other extension expressions, use the default visitor behavior
        // base.VisitExtension calls node.VisitChildren which works for most expression types
        return base.VisitExtension(node);
    }

    /// <summary>
    /// Throws when <c>.Final()</c> is applied to a table whose engine does not
    /// support FINAL. ClickHouse only honours FINAL on MergeTree-family engines;
    /// applying it to Memory / Log / Distributed / etc. errors at execution with
    /// a confusing server-side message. We catch it here so the failure surfaces
    /// at the offending call site instead.
    /// </summary>
    private void ValidateFinalEngine(TableExpression tableExpression)
    {
        var entityType = FindEntityTypeByTableName(tableExpression.Name, tableExpression.Schema);
        if (entityType is null)
        {
            return; // Unknown table (e.g. a CTE alias or table function) — let the server decide.
        }

        var engine = entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value as string;
        if (engine is null || ClickHouseEngineNames.IsMergeTreeFamily(engine))
        {
            return;
        }

        throw new InvalidOperationException(
            $".Final() is only supported on MergeTree-family engines. Entity '{entityType.ClrType.Name}' " +
            $"is mapped to table '{tableExpression.Name}' using the '{engine}' engine, which does not support FINAL.");
    }

    private IEntityType? FindEntityTypeByTableName(string tableName, string? schema)
    {
        foreach (var entityType in _model.GetEntityTypes())
        {
            var entityTableName = entityType.GetTableName();
            var entitySchema = entityType.GetSchema() ?? _model.GetDefaultSchema();
            if (entityTableName == tableName && entitySchema == schema)
            {
                return entityType;
            }
        }
        return null;
    }

    /// <summary>
    /// Throws when <c>.Sample()</c> would wrap a table that has a ClickHouse engine
    /// declared in the model but no <c>SAMPLE BY</c> expression. The table modifier
    /// visitor wraps every <see cref="TableExpression"/> with the SAMPLE modifier,
    /// so a query that joins a sampled table with a lookup would otherwise emit
    /// SAMPLE on both — and the server returns "Storage X doesn't support sampling"
    /// if the lookup has no <c>SAMPLE BY</c>. Catching it here surfaces the failure at
    /// the offending call site (and forces the user to sample-then-join in two steps
    /// when only one of the tables is sampleable).
    ///
    /// We only validate when the entity has an Engine annotation: tests and minimal
    /// models that don't configure CH-specific metadata fall through to the server
    /// (which is what they did before this validation was added).
    /// </summary>
    private void ValidateSampleableTable(TableExpression tableExpression)
    {
        var entityType = FindEntityTypeByTableName(tableExpression.Name, tableExpression.Schema);
        if (entityType is null)
        {
            return; // unknown — let the server decide.
        }

        var engine = entityType.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value as string;
        if (string.IsNullOrEmpty(engine))
        {
            return; // no CH-specific metadata at all — let the server decide.
        }

        var sampleBy = entityType.FindAnnotation(ClickHouseAnnotationNames.SampleBy)?.Value as string;
        if (!string.IsNullOrWhiteSpace(sampleBy))
        {
            return;
        }

        throw new InvalidOperationException(
            $".Sample() is only supported on tables that declare a SAMPLE BY clause. " +
            $"Entity '{entityType.ClrType.Name}' is mapped to table '{tableExpression.Name}' " +
            $"and has no HasSampleBy(...) configured. If you intended to sample one side of a " +
            $"join, materialise the sampled query first and then join the result.");
    }
}

/// <summary>
/// Visits SelectExpression trees and replaces TableExpression instances for external entities
/// with ClickHouseExternalTableFunctionExpression.
/// </summary>
internal class ClickHouseExternalTableFunctionVisitor : ExpressionVisitor
{
    private readonly IModel _model;
    private readonly IExternalConfigResolver _externalConfigResolver;

    public ClickHouseExternalTableFunctionVisitor(IModel model, IExternalConfigResolver externalConfigResolver)
    {
        _model = model;
        _externalConfigResolver = externalConfigResolver;
    }

    protected override Expression VisitExtension(Expression node)
    {
        // Check if this is a TableExpression for an external entity
        if (node is TableExpression tableExpression)
        {
            var entityType = FindEntityTypeByTableName(tableExpression.Name, tableExpression.Schema);

            if (entityType != null && _externalConfigResolver.IsExternalTableFunction(entityType))
            {
                // Get the provider type from entity annotation
                var provider = entityType.FindAnnotation(Metadata.ClickHouseAnnotationNames.ExternalProvider)
                    ?.Value?.ToString() ?? "postgresql";

                // Replace with external table function expression
                var functionCall = _externalConfigResolver.ResolveTableFunction(entityType);
                return new ClickHouseExternalTableFunctionExpression(
                    tableExpression.Alias,
                    provider,
                    functionCall,
                    entityType.ClrType);
            }

            // Not an external entity - leave as-is
            return node;
        }

        // Handle ShapedQueryExpression specially - it doesn't support VisitChildren
        if (node is ShapedQueryExpression shapedQuery)
        {
            var newQueryExpression = Visit(shapedQuery.QueryExpression);
            var newShaperExpression = Visit(shapedQuery.ShaperExpression);

            if (newQueryExpression != shapedQuery.QueryExpression ||
                newShaperExpression != shapedQuery.ShaperExpression)
            {
                return shapedQuery.Update(newQueryExpression, newShaperExpression);
            }

            return shapedQuery;
        }

        // EnumerableExpression.VisitChildren throws in EF Core 10+
        if (node is EnumerableExpression)
        {
            return node;
        }

        return base.VisitExtension(node);
    }

    private IEntityType? FindEntityTypeByTableName(string tableName, string? schema)
    {
        // Look up entity type by table name and schema
        foreach (var entityType in _model.GetEntityTypes())
        {
            var entityTableName = entityType.GetTableName();
            var entitySchema = entityType.GetSchema() ?? _model.GetDefaultSchema();

            if (entityTableName == tableName && entitySchema == schema)
            {
                return entityType;
            }
        }

        return null;
    }
}

/// <summary>
/// Visits SelectExpression trees and replaces TableExpression instances for dictionary entities
/// with ClickHouseDictionaryTableExpression to use the dictionary() table function.
/// </summary>
internal class ClickHouseDictionaryTableFunctionVisitor : ExpressionVisitor
{
    private readonly IModel _model;

    public ClickHouseDictionaryTableFunctionVisitor(IModel model)
    {
        _model = model;
    }

    protected override Expression VisitExtension(Expression node)
    {
        // Check if this is a TableExpression for a dictionary entity
        if (node is TableExpression tableExpression)
        {
            var entityType = FindEntityTypeByTableName(tableExpression.Name, tableExpression.Schema);

            if (entityType != null && IsDictionaryEntity(entityType))
            {
                // Get dictionary name (uses table name or snake_case of type name)
                var dictionaryName = GetDictionaryName(entityType);

                // Replace with dictionary table function expression
                return new ClickHouseDictionaryTableExpression(
                    tableExpression.Alias,
                    dictionaryName,
                    entityType.ClrType);
            }

            // Not a dictionary entity - leave as-is
            return node;
        }

        // Handle ShapedQueryExpression specially - it doesn't support VisitChildren
        if (node is ShapedQueryExpression shapedQuery)
        {
            var newQueryExpression = Visit(shapedQuery.QueryExpression);
            var newShaperExpression = Visit(shapedQuery.ShaperExpression);

            if (newQueryExpression != shapedQuery.QueryExpression ||
                newShaperExpression != shapedQuery.ShaperExpression)
            {
                return shapedQuery.Update(newQueryExpression, newShaperExpression);
            }

            return shapedQuery;
        }

        // EnumerableExpression.VisitChildren throws in EF Core 10+
        if (node is EnumerableExpression)
        {
            return node;
        }

        return base.VisitExtension(node);
    }

    private bool IsDictionaryEntity(IEntityType entityType)
    {
        // Check for the Dictionary annotation
        return entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary) != null;
    }

    private string GetDictionaryName(IEntityType entityType)
    {
        // Dictionary name is the table name (already in snake_case from configuration)
        return entityType.GetTableName() ?? ConvertToSnakeCase(entityType.ClrType.Name);
    }

    private IEntityType? FindEntityTypeByTableName(string tableName, string? schema)
    {
        // Look up entity type by table name and schema
        foreach (var entityType in _model.GetEntityTypes())
        {
            var entityTableName = entityType.GetTableName();
            var entitySchema = entityType.GetSchema() ?? _model.GetDefaultSchema();

            if (entityTableName == tableName && entitySchema == schema)
            {
                return entityType;
            }
        }

        return null;
    }

    private static string ConvertToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var result = new StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                    result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }
}

/// <summary>
/// Extracts the innermost FROM-clause subquery (or table) from the outer SelectExpression,
/// stores it as a CTE definition, and replaces it with a <see cref="ClickHouseCteReferenceExpression"/>.
/// </summary>
internal class ClickHouseCteExtractionVisitor : ExpressionVisitor
{
    private readonly ClickHouseQueryCompilationContextOptions _options;
    private bool _isOuterSelect = true;

    public ClickHouseCteExtractionVisitor(ClickHouseQueryCompilationContextOptions options)
    {
        _options = options;
    }

    protected override Expression VisitExtension(Expression node)
    {
        // Handle ShapedQueryExpression specially
        if (node is ShapedQueryExpression shapedQuery)
        {
            var newQueryExpression = Visit(shapedQuery.QueryExpression);
            var newShaperExpression = Visit(shapedQuery.ShaperExpression);

            if (newQueryExpression != shapedQuery.QueryExpression ||
                newShaperExpression != shapedQuery.ShaperExpression)
            {
                return shapedQuery.Update(newQueryExpression, newShaperExpression);
            }

            return shapedQuery;
        }

        // EnumerableExpression.VisitChildren throws in EF Core 10+
        if (node is EnumerableExpression)
        {
            return node;
        }

        // For the outer SelectExpression, look for tables to extract as CTEs
        if (node is SelectExpression selectExpression && _isOuterSelect)
        {
            _isOuterSelect = false;
            return ExtractCteFromSelect(selectExpression);
        }

        return base.VisitExtension(node);
    }

    private Expression ExtractCteFromSelect(SelectExpression selectExpression)
    {
        var cteName = _options.PendingCteName!;
        _options.PendingCteName = null;

        // Look for the first table source that can be extracted as a CTE
        if (selectExpression.Tables.Count == 0)
        {
            return selectExpression;
        }

        var firstTable = selectExpression.Tables[0];

        if (firstTable is SelectExpression subquery)
        {
            // Subquery in FROM — extract as CTE body
            _options.CteDefinitions.Add(new CteDefinition(cteName, subquery));

            // Replace with CTE reference using the same alias
            var cteRef = new ClickHouseCteReferenceExpression(cteName, subquery.Alias);
            return ReplaceTables(selectExpression, 0, cteRef);
        }

        if (firstTable is TableExpression tableExpr)
        {
            // Direct table — store table reference for SQL generator to render SELECT * FROM "table"
            _options.CteDefinitions.Add(new CteDefinition(cteName, (TableExpressionBase)tableExpr));

            var cteRef = new ClickHouseCteReferenceExpression(cteName, tableExpr.Alias);
            return ReplaceTables(selectExpression, 0, cteRef);
        }

        if (firstTable is ClickHouseTableModifierExpression modifierExpr)
        {
            // Table with FINAL/SAMPLE modifiers — store for SQL generator
            var alias = modifierExpr.Table is TableExpression te ? te.Alias : modifierExpr.Alias;
            _options.CteDefinitions.Add(new CteDefinition(cteName, (TableExpressionBase)modifierExpr));

            var cteRef = new ClickHouseCteReferenceExpression(cteName, alias);
            return ReplaceTables(selectExpression, 0, cteRef);
        }

        // Fallback: can't extract CTE from this table type
        return selectExpression;
    }

    /// <summary>
    /// Replaces the table at the given index in a SelectExpression with a new table expression.
    /// Uses reflection to create a new SelectExpression with the modified tables list.
    /// </summary>
    private static SelectExpression ReplaceTables(
        SelectExpression selectExpression,
        int tableIndex,
        TableExpressionBase replacement)
    {
        // Use the SelectExpression's Update-style API via visitor pattern
        // We create a visitor that replaces only the target table
        var replacer = new TableReplacerVisitor(selectExpression.Tables[tableIndex], replacement);
        return (SelectExpression)replacer.Visit(selectExpression);
    }
}

/// <summary>
/// Replaces a specific table expression with a replacement in the expression tree.
/// </summary>
internal class TableReplacerVisitor : ExpressionVisitor
{
    private readonly TableExpressionBase _target;
    private readonly TableExpressionBase _replacement;

    public TableReplacerVisitor(TableExpressionBase target, TableExpressionBase replacement)
    {
        _target = target;
        _replacement = replacement;
    }

    protected override Expression VisitExtension(Expression node)
    {
        if (node == _target)
        {
            return _replacement;
        }

        // EnumerableExpression.VisitChildren throws in EF Core 10+
        if (node is EnumerableExpression)
        {
            return node;
        }

        return base.VisitExtension(node);
    }
}
