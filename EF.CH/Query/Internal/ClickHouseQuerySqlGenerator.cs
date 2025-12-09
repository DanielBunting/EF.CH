using System.Globalization;
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

    // Thread-local storage for query settings (set during translation, read during SQL generation)
    [ThreadStatic]
    private static Dictionary<string, object>? _currentQuerySettings;

    public ClickHouseQuerySqlGenerator(
        QuerySqlGeneratorDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
        : base(dependencies)
    {
        _sqlGenerationHelper = dependencies.SqlGenerationHelper;
        _typeMappingSource = typeMappingSource;
    }

    /// <summary>
    /// Sets query settings to be appended as SETTINGS clause.
    /// This is called during query translation and read during SQL generation.
    /// </summary>
    internal static void SetQuerySettings(Dictionary<string, object> settings)
    {
        _currentQuerySettings = settings.Count > 0 ? new Dictionary<string, object>(settings) : null;
    }

    /// <summary>
    /// Clears query settings after SQL generation.
    /// </summary>
    internal static void ClearQuerySettings()
    {
        _currentQuerySettings = null;
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

        // Strip Nullable() wrapper if present - ClickHouse.Driver handles nullability differently
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
    /// Also generates SETTINGS clause if query settings are present.
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

        // Generate SETTINGS clause if any query settings are present
        GenerateSettings();
    }

    /// <summary>
    /// Generates the SETTINGS clause for ClickHouse query settings.
    /// </summary>
    private void GenerateSettings()
    {
        if (_currentQuerySettings == null || _currentQuerySettings.Count == 0)
        {
            return;
        }

        Sql.AppendLine()
            .Append("SETTINGS ");

        var first = true;
        foreach (var setting in _currentQuerySettings)
        {
            if (!first)
            {
                Sql.Append(", ");
            }
            first = false;

            Sql.Append(setting.Key)
                .Append(" = ")
                .Append(FormatSettingValue(setting.Value));
        }
    }

    /// <summary>
    /// Formats a setting value for the SETTINGS clause.
    /// </summary>
    private static string FormatSettingValue(object value)
    {
        return value switch
        {
            bool b => b ? "1" : "0",
            string s => $"'{s.Replace("'", "\\'")}'",
            int or long or short or byte or uint or ulong or ushort or sbyte => value.ToString()!,
            float f => f.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString(CultureInfo.InvariantCulture),
            decimal dec => dec.ToString(CultureInfo.InvariantCulture),
            _ => value.ToString()!
        };
    }

    /// <summary>
    /// Handles ClickHouse-specific expression types.
    /// </summary>
    protected override Expression VisitExtension(Expression extensionExpression)
    {
        return extensionExpression switch
        {
            ClickHouseTableModifierExpression modifierExpression => VisitTableModifier(modifierExpression),
            ClickHouseFinalExpression finalExpression => VisitFinal(finalExpression),
            ClickHouseSampleExpression sampleExpression => VisitSample(sampleExpression),
            _ => base.VisitExtension(extensionExpression)
        };
    }

    /// <summary>
    /// Generates table reference with FINAL and SAMPLE modifiers.
    /// Called by the postprocessor-injected ClickHouseTableModifierExpression.
    /// </summary>
    private Expression VisitTableModifier(ClickHouseTableModifierExpression expression)
    {
        // Visit the underlying table (generates: "schema"."table" AS "t")
        Visit(expression.Table);

        // Add FINAL if requested
        if (expression.UseFinal)
        {
            Sql.Append(" FINAL");
        }

        // Add SAMPLE if requested
        if (expression.SampleFraction.HasValue)
        {
            Sql.Append(" SAMPLE ");
            Sql.Append(expression.SampleFraction.Value.ToString("G", CultureInfo.InvariantCulture));

            if (expression.SampleOffset.HasValue)
            {
                Sql.Append(" OFFSET ");
                Sql.Append(expression.SampleOffset.Value.ToString("G", CultureInfo.InvariantCulture));
            }
        }

        return expression;
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
        Sql.Append(expression.Fraction.ToString("G", CultureInfo.InvariantCulture));

        if (expression.Offset.HasValue)
        {
            Sql.Append(" OFFSET ");
            Sql.Append(expression.Offset.Value.ToString("G", CultureInfo.InvariantCulture));
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
