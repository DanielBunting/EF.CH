using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal;

/// <summary>
/// Post-processes translated queries to apply ClickHouse-specific modifiers (FINAL, SAMPLE, SETTINGS).
/// Runs after query translation but before SQL generation.
/// </summary>
public class ClickHouseQueryTranslationPostprocessor : RelationalQueryTranslationPostprocessor
{
    private readonly RelationalQueryCompilationContext _queryCompilationContext;

    public ClickHouseQueryTranslationPostprocessor(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _queryCompilationContext = queryCompilationContext;
    }

    public override Expression Process(Expression query)
    {
        // Let base class do standard processing first
        query = base.Process(query);

        // Get ClickHouse options set during translation phase
        var options = _queryCompilationContext.QueryCompilationContextOptions();

        // Apply FINAL/SAMPLE modifiers to table expressions
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

        // For other extension expressions, use the default visitor behavior
        // base.VisitExtension calls node.VisitChildren which works for most expression types
        return base.VisitExtension(node);
    }
}
