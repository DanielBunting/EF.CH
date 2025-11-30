using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal;

/// <summary>
/// Generates ClickHouse SQL from query expressions.
/// </summary>
public class ClickHouseQuerySqlGenerator : QuerySqlGenerator
{
    private readonly ISqlGenerationHelper _sqlGenerationHelper;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly HashSet<string> _visitedParameters = new();
    private bool _inDeleteContext;

    public ClickHouseQuerySqlGenerator(
        QuerySqlGeneratorDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
        : base(dependencies)
    {
        _sqlGenerationHelper = dependencies.SqlGenerationHelper;
        _typeMappingSource = typeMappingSource;
    }

    /// <summary>
    /// Visits a SQL parameter expression and generates ClickHouse-format parameter placeholder.
    /// ClickHouse requires {name:Type} format for parameters.
    /// </summary>
    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        var parameterName = sqlParameterExpression.Name;
        var typeMapping = sqlParameterExpression.TypeMapping;

        // Get the ClickHouse type name
        var storeType = typeMapping?.StoreType ?? "String";

        // Strip Nullable() wrapper if present - ClickHouse.Client handles nullability differently
        if (storeType.StartsWith("Nullable(", StringComparison.OrdinalIgnoreCase) &&
            storeType.EndsWith(")"))
        {
            storeType = storeType.Substring(9, storeType.Length - 10);
        }

        // Register the parameter with EF Core's command builder (only once per parameter name)
        if (_visitedParameters.Add(parameterName) && typeMapping != null)
        {
            Sql.AddParameter(
                parameterName,
                _sqlGenerationHelper.GenerateParameterName(parameterName),
                typeMapping,
                sqlParameterExpression.IsNullable);
        }

        // Generate ClickHouse parameter format: {name:Type}
        Sql.Append("{")
           .Append(parameterName)
           .Append(":")
           .Append(storeType)
           .Append("}");

        return sqlParameterExpression;
    }

    /// <summary>
    /// Generates LIMIT/OFFSET clauses using ClickHouse's syntax.
    /// ClickHouse uses: LIMIT [offset,] count
    /// </summary>
    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        ArgumentNullException.ThrowIfNull(selectExpression);

        if (selectExpression.Limit is not null)
        {
            Sql.AppendLine()
                .Append("LIMIT ");

            if (selectExpression.Offset is not null)
            {
                Visit(selectExpression.Offset);
                Sql.Append(", ");
            }

            Visit(selectExpression.Limit);
        }
        else if (selectExpression.Offset is not null)
        {
            // ClickHouse requires a LIMIT when using OFFSET
            // Use max UInt64 value as effectively "unlimited"
            Sql.AppendLine()
                .Append("LIMIT ");
            Visit(selectExpression.Offset);
            Sql.Append(", 18446744073709551615");
        }
    }

    /// <summary>
    /// Handles ClickHouse-specific expression types.
    /// </summary>
    protected override Expression VisitExtension(Expression extensionExpression)
    {
        return extensionExpression switch
        {
            ClickHouseFinalExpression finalExpression => VisitFinal(finalExpression),
            ClickHouseSampleExpression sampleExpression => VisitSample(sampleExpression),
            _ => base.VisitExtension(extensionExpression)
        };
    }

    /// <summary>
    /// Generates FINAL modifier for ReplacingMergeTree tables.
    /// </summary>
    private Expression VisitFinal(ClickHouseFinalExpression expression)
    {
        Visit(expression.Table);
        Sql.Append(" FINAL");
        return expression;
    }

    /// <summary>
    /// Generates SAMPLE clause for probabilistic sampling.
    /// </summary>
    private Expression VisitSample(ClickHouseSampleExpression expression)
    {
        Sql.Append(" SAMPLE ");
        Sql.Append(expression.Fraction.ToString("G", System.Globalization.CultureInfo.InvariantCulture));

        if (expression.Offset.HasValue)
        {
            Sql.Append(" OFFSET ");
            Sql.Append(expression.Offset.Value.ToString("G", System.Globalization.CultureInfo.InvariantCulture));
        }

        return expression;
    }

    /// <summary>
    /// Override to ensure proper SQL generation for ClickHouse-specific constructs.
    /// </summary>
    protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
    {
        // Handle ClickHouse-specific function naming
        return base.VisitSqlFunction(sqlFunctionExpression);
    }

    /// <summary>
    /// Generates DELETE statement for ClickHouse.
    /// ClickHouse does not support table aliases in DELETE statements.
    /// </summary>
    protected override Expression VisitDelete(DeleteExpression deleteExpression)
    {
        var selectExpression = deleteExpression.SelectExpression;
        var table = deleteExpression.Table;

        // ClickHouse DELETE syntax: DELETE FROM "table" WHERE ...
        // Note: No table alias support
        Sql.Append("DELETE FROM ");
        Sql.Append(_sqlGenerationHelper.DelimitIdentifier(table.Name, table.Schema));

        if (selectExpression.Predicate != null)
        {
            Sql.AppendLine().Append("WHERE ");

            // Set flag to suppress table qualifiers in column references
            _inDeleteContext = true;
            try
            {
                Visit(selectExpression.Predicate);
            }
            finally
            {
                _inDeleteContext = false;
            }
        }

        return deleteExpression;
    }

    /// <summary>
    /// Generates column reference.
    /// In DELETE context, omits table alias since ClickHouse DELETE doesn't support aliases.
    /// </summary>
    protected override Expression VisitColumn(ColumnExpression columnExpression)
    {
        if (_inDeleteContext)
        {
            // In DELETE context, use unqualified column name
            Sql.Append(_sqlGenerationHelper.DelimitIdentifier(columnExpression.Name));
            return columnExpression;
        }

        return base.VisitColumn(columnExpression);
    }
}

/// <summary>
/// Factory for creating ClickHouse query SQL generators.
/// </summary>
public class ClickHouseQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseQuerySqlGeneratorFactory(
        QuerySqlGeneratorDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
    }

    public QuerySqlGenerator Create()
        => new ClickHouseQuerySqlGenerator(_dependencies, _typeMappingSource);
}
