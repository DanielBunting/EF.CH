using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using EF.CH.Metadata;
using EF.CH.Query.Internal.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal;

/// <summary>
/// Processes SQL nullability for ClickHouse, handling custom table expressions like ClickHouseTableModifierExpression
/// and rewriting null comparisons for defaultForNull-default columns.
/// </summary>
public class ClickHouseSqlNullabilityProcessor : SqlNullabilityProcessor
{
    /// <summary>
    /// Thread-local storage for defaultForNull mappings during query processing.
    /// Key: ColumnName (simplified approach - assumes unique column names for defaultForNull properties)
    /// Value: defaultForNull value
    /// </summary>
    private static readonly AsyncLocal<Dictionary<string, object>?> DefaultForNullMappings = new();

    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseSqlNullabilityProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
        _sqlExpressionFactory = dependencies.SqlExpressionFactory;
        _typeMappingSource = dependencies.TypeMappingSource;
    }

    /// <summary>
    /// Sets the defaultForNull mappings for the current query processing context.
    /// Called by ClickHouseQueryTranslationPostprocessor before SQL processing.
    /// </summary>
    /// <remarks>
    /// This uses column names as keys. If you have multiple tables with the same
    /// column name but different defaultForNull configurations, consider using unique column names.
    /// </remarks>
    public static void SetDefaultForNullMappings(IModel model)
    {
        var mappings = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        foreach (var entityType in model.GetEntityTypes())
        {
            var tableName = entityType.GetTableName();
            if (tableName == null) continue;

            foreach (var property in entityType.GetProperties())
            {
                var defaultForNull = property.FindAnnotation(ClickHouseAnnotationNames.DefaultForNull)?.Value;
                if (defaultForNull != null)
                {
                    var columnName = property.GetColumnName() ?? property.Name;
                    // Use column name as key (last one wins if duplicates)
                    mappings[columnName] = defaultForNull;
                }
            }
        }

        DefaultForNullMappings.Value = mappings.Count > 0 ? mappings : null;
    }

    /// <summary>
    /// Clears the defaultForNull mappings after query processing.
    /// </summary>
    public static void ClearDefaultForNullMappings()
    {
        DefaultForNullMappings.Value = null;
    }

    /// <summary>
    /// Visits extension expressions, handling ClickHouse-specific expressions like ClickHouseTableModifierExpression
    /// and ClickHouseWindowFunctionExpression.
    /// </summary>
    protected override Expression VisitExtension(Expression node)
    {
        if (node is ClickHouseTableModifierExpression modifierExpression)
        {
            // Visit the wrapped table expression
            var visitedTable = Visit(modifierExpression.Table);

            // Return a new modifier expression with the visited table
            return visitedTable != modifierExpression.Table
                ? new ClickHouseTableModifierExpression(
                    (TableExpressionBase)visitedTable,
                    modifierExpression.UseFinal,
                    modifierExpression.SampleFraction,
                    modifierExpression.SampleOffset)
                : modifierExpression;
        }

        if (node is ClickHouseWindowFunctionExpression windowExpression)
        {
            return VisitWindowFunction(windowExpression);
        }

        if (node is ClickHouseJsonPathExpression jsonPathExpression)
        {
            return VisitJsonPath(jsonPathExpression);
        }

        return base.VisitExtension(node);
    }

    /// <summary>
    /// Visits a window function expression, processing its child expressions for nullability.
    /// </summary>
    private ClickHouseWindowFunctionExpression VisitWindowFunction(ClickHouseWindowFunctionExpression windowExpression)
    {
        var changed = false;

        // Visit arguments
        var arguments = new List<SqlExpression>(windowExpression.Arguments.Count);
        foreach (var arg in windowExpression.Arguments)
        {
            var visited = Visit(arg, allowOptimizedExpansion: false, out _);
            changed |= visited != arg;
            arguments.Add(visited);
        }

        // Visit partition by expressions
        var partitionBy = new List<SqlExpression>(windowExpression.PartitionBy.Count);
        foreach (var expr in windowExpression.PartitionBy)
        {
            var visited = Visit(expr, allowOptimizedExpansion: false, out _);
            changed |= visited != expr;
            partitionBy.Add(visited);
        }

        // Visit order by expressions
        var orderBy = new List<OrderingExpression>(windowExpression.OrderBy.Count);
        foreach (var ordering in windowExpression.OrderBy)
        {
            var visitedExpr = Visit(ordering.Expression, allowOptimizedExpansion: false, out _);
            changed |= visitedExpr != ordering.Expression;
            orderBy.Add(visitedExpr != ordering.Expression
                ? new OrderingExpression(visitedExpr, ordering.IsAscending)
                : ordering);
        }

        return changed
            ? windowExpression.Update(arguments, partitionBy, orderBy, windowExpression.Frame)
            : windowExpression;
    }

    /// <summary>
    /// Visits a JSON path expression, processing the column reference for nullability.
    /// </summary>
    private ClickHouseJsonPathExpression VisitJsonPath(ClickHouseJsonPathExpression jsonPathExpression)
    {
        // Visit the column expression
        var visitedColumn = Visit(jsonPathExpression.Column, allowOptimizedExpansion: false, out _);

        return visitedColumn != jsonPathExpression.Column
            ? new ClickHouseJsonPathExpression(
                visitedColumn,
                jsonPathExpression.PathSegments,
                jsonPathExpression.ArrayIndices,
                jsonPathExpression.Type,
                jsonPathExpression.TypeMapping)
            : jsonPathExpression;
    }

    /// <summary>
    /// Visits custom SQL expressions, handling ClickHouse-specific expressions like ClickHouseJsonPathExpression.
    /// </summary>
    protected override SqlExpression VisitCustomSqlExpression(
        SqlExpression sqlExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        if (sqlExpression is ClickHouseJsonPathExpression jsonPathExpression)
        {
            // JSON path expressions are nullable (the path may not exist)
            nullable = true;
            return VisitJsonPath(jsonPathExpression);
        }

        return base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable);
    }

    /// <summary>
    /// Visits binary expressions, rewriting null comparisons for defaultForNull-default columns.
    /// </summary>
    protected override SqlExpression VisitSqlBinary(
        SqlBinaryExpression sqlBinaryExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        // Check for null comparison patterns: column == null or column != null
        if (TryRewriteDefaultForNullComparison(sqlBinaryExpression, out var rewritten))
        {
            nullable = false;
            return rewritten;
        }

        return base.VisitSqlBinary(sqlBinaryExpression, allowOptimizedExpansion, out nullable);
    }

    /// <summary>
    /// Attempts to rewrite a null comparison to a defaultForNull comparison for columns with defaultForNull defaults.
    /// </summary>
    private bool TryRewriteDefaultForNullComparison(
        SqlBinaryExpression expr,
        [NotNullWhen(true)] out SqlExpression? result)
    {
        result = null;

        // Only handle equality and inequality operators
        if (expr.OperatorType is not (ExpressionType.Equal or ExpressionType.NotEqual))
            return false;

        // Check for: column == null, column != null, null == column, null != column
        ColumnExpression? column = null;
        SqlExpression? other = null;

        if (expr.Left is ColumnExpression leftCol)
        {
            column = leftCol;
            other = expr.Right;
        }
        else if (expr.Right is ColumnExpression rightCol)
        {
            column = rightCol;
            other = expr.Left;
        }

        if (column == null)
            return false;

        // Check if the other side is a constant null
        if (other is not SqlConstantExpression { Value: null })
            return false;

        // Look up the defaultForNull value for this column by name
        var defaultForNull = GetDefaultForNullValue(column.Name);
        if (defaultForNull == null)
            return false;

        // Create a constant expression for the defaultForNull value
        var typeMapping = _typeMappingSource.FindMapping(defaultForNull.GetType());
        var defaultForNullConstant = _sqlExpressionFactory.Constant(defaultForNull, typeMapping);

        // Rewrite: column == null → column = defaultForNull
        //          column != null → column <> defaultForNull
        result = expr.OperatorType == ExpressionType.Equal
            ? _sqlExpressionFactory.Equal(column, defaultForNullConstant)
            : _sqlExpressionFactory.NotEqual(column, defaultForNullConstant);

        return true;
    }

    /// <summary>
    /// Gets the defaultForNull value for a column by name if it has a defaultForNull default configured.
    /// </summary>
    /// <remarks>
    /// Used by both the nullability processor for null comparisons and the
    /// aggregate translator for defaultForNull-aware aggregations.
    /// </remarks>
    public static object? GetDefaultForNullValue(string columnName)
    {
        var mappings = DefaultForNullMappings.Value;
        if (mappings == null)
            return null;

        return mappings.TryGetValue(columnName, out var defaultForNull) ? defaultForNull : null;
    }
}
