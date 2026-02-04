using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates EF.Functions.RawSql() calls to raw SQL fragment expressions.
/// </summary>
public class ClickHouseRawSqlTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    private static readonly MethodInfo RawSqlMethod =
        typeof(ClickHouseDbFunctions).GetMethod(
            nameof(ClickHouseDbFunctions.RawSql),
            new[] { typeof(DbFunctions), typeof(string) })!;

    private static readonly MethodInfo RawSqlWithParamsMethod =
        typeof(ClickHouseDbFunctions).GetMethod(
            nameof(ClickHouseDbFunctions.RawSql),
            new[] { typeof(DbFunctions), typeof(string), typeof(object[]) })!;

    public ClickHouseRawSqlTranslator(
        ClickHouseSqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Handle EF.Functions.RawSql<TResult>(sql)
        if (method.IsGenericMethod)
        {
            var genericDef = method.GetGenericMethodDefinition();

            if (genericDef == RawSqlMethod)
            {
                return TranslateRawSql(method, arguments);
            }

            if (genericDef == RawSqlWithParamsMethod)
            {
                return TranslateRawSqlWithParams(method, arguments);
            }
        }

        return null;
    }

    private SqlExpression? TranslateRawSql(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] is DbFunctions instance (ignored)
        // arguments[1] is the raw SQL string
        if (arguments[1] is not SqlConstantExpression sqlConstant ||
            sqlConstant.Value is not string rawSql)
        {
            return null;
        }

        var returnType = method.GetGenericArguments()[0];
        var typeMapping = _typeMappingSource.FindMapping(returnType);

        return _sqlExpressionFactory.Fragment(rawSql, typeMapping);
    }

    private SqlExpression? TranslateRawSqlWithParams(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] is DbFunctions instance (ignored)
        // arguments[1] is the raw SQL template
        // arguments[2] is the parameters array
        if (arguments[1] is not SqlConstantExpression sqlConstant ||
            sqlConstant.Value is not string rawSqlTemplate)
        {
            return null;
        }

        // Extract parameter values from the array expression
        var parameters = ExtractParameterValues(arguments[2]);
        if (parameters == null)
        {
            return null;
        }

        // Substitute parameters into the SQL template
        var substitutedSql = SubstituteParameters(rawSqlTemplate, parameters);

        var returnType = method.GetGenericArguments()[0];
        var typeMapping = _typeMappingSource.FindMapping(returnType);

        return _sqlExpressionFactory.Fragment(substitutedSql, typeMapping);
    }

    private static object[]? ExtractParameterValues(SqlExpression parameterArrayExpr)
    {
        // Handle SqlConstantExpression containing the array directly
        if (parameterArrayExpr is SqlConstantExpression constantExpr)
        {
            if (constantExpr.Value is object[] array)
            {
                return array;
            }

            // Single value case - wrap in array
            if (constantExpr.Value != null)
            {
                return new[] { constantExpr.Value };
            }

            return Array.Empty<object>();
        }

        // Cannot extract parameters from complex expressions
        return null;
    }

    private string SubstituteParameters(string sqlTemplate, object[] parameters)
    {
        if (parameters.Length == 0)
        {
            return sqlTemplate;
        }

        return Regex.Replace(sqlTemplate, @"\{(\d+)\}", match =>
        {
            var index = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
            if (index < 0 || index >= parameters.Length)
            {
                throw new InvalidOperationException(
                    $"RawSql parameter index {{{index}}} is out of range. " +
                    $"Only {parameters.Length} parameters were provided.");
            }

            var value = parameters[index];
            return GenerateLiteral(value);
        });
    }

    private string GenerateLiteral(object? value)
    {
        if (value == null)
        {
            return "NULL";
        }

        // Try to get a type mapping for proper literal generation
        var clrType = value.GetType();
        var typeMapping = _typeMappingSource.FindMapping(clrType);

        if (typeMapping != null)
        {
            return typeMapping.GenerateSqlLiteral(value);
        }

        // Fallback literal generation
        return value switch
        {
            string s => $"'{s.Replace("\\", "\\\\").Replace("'", "\\'")}'",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
            DateTimeOffset dto => $"'{dto.UtcDateTime:yyyy-MM-dd HH:mm:ss}'",
            DateOnly d => $"'{d:yyyy-MM-dd}'",
            bool b => b ? "true" : "false",
            Guid g => $"'{g}'",
            decimal m => m.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString(CultureInfo.InvariantCulture),
            float f => f.ToString(CultureInfo.InvariantCulture),
            int i => i.ToString(CultureInfo.InvariantCulture),
            long l => l.ToString(CultureInfo.InvariantCulture),
            _ => value.ToString() ?? "NULL"
        };
    }
}
