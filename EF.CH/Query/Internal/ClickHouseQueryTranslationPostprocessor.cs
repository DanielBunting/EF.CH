using System.Linq.Expressions;
using System.Text;
using EF.CH.External;
using EF.CH.Metadata;
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
            query = new ClickHouseTableModifierApplyingVisitor(options).Visit(query);
        }

        // Pass SETTINGS via thread-local to SQL generator
        // (SETTINGS is query-level, not table-level, so thread-local is appropriate)
        if (options.QuerySettings.Count > 0)
        {
            ClickHouseQuerySqlGenerator.SetQuerySettings(options.QuerySettings);
        }

        // Pass WITH FILL / INTERPOLATE specs via thread-local to SQL generator
        if (options.HasWithFill || options.HasInterpolate)
        {
            ClickHouseQuerySqlGenerator.SetWithFillOptions(options);
        }

        // Pass PREWHERE expression via thread-local to SQL generator
        if (options.PreWhereExpression != null)
        {
            ClickHouseQuerySqlGenerator.SetPreWhereExpression(options.PreWhereExpression);
        }

        // Pass LIMIT BY options via thread-local to SQL generator
        if (options.HasLimitBy)
        {
            ClickHouseQuerySqlGenerator.SetLimitBy(
                options.LimitByLimit!.Value,
                options.LimitByOffset,
                options.LimitByExpressions!);
        }

        // Pass GROUP BY modifier via thread-local to SQL generator
        if (options.GroupByModifier != GroupByModifier.None)
        {
            ClickHouseQuerySqlGenerator.SetGroupByModifier(options.GroupByModifier);
        }

        return query;
    }
}

/// <summary>
/// Visits SelectExpression trees and wraps TableExpression instances with ClickHouseTableModifierExpression.
/// This applies FINAL and SAMPLE modifiers to all tables in the query.
/// </summary>
internal class ClickHouseTableModifierApplyingVisitor : ExpressionVisitor
{
    private readonly ClickHouseQueryCompilationContextOptions _options;

    public ClickHouseTableModifierApplyingVisitor(ClickHouseQueryCompilationContextOptions options)
    {
        _options = options;
    }

    protected override Expression VisitExtension(Expression node)
    {
        // Wrap TableExpression with our modifier expression
        if (node is TableExpression tableExpression)
        {
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
