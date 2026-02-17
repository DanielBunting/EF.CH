using System.Reflection;
using EF.CH.Extensions;
using EF.CH.Query.Internal.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates <see cref="ClickHouseFunctions.RawSql{T}"/> calls to <see cref="ClickHouseRawSqlExpression"/>.
/// This allows embedding raw SQL expressions (e.g. <c>quantile(0.95)(value)</c>) in LINQ projections.
/// </summary>
public class ClickHouseRawSqlTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo RawSqlMethodInfo =
        typeof(ClickHouseFunctions).GetMethod(nameof(ClickHouseFunctions.RawSql))!;

    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public ClickHouseRawSqlTranslator(IRelationalTypeMappingSource typeMappingSource)
    {
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (!method.IsGenericMethod ||
            method.GetGenericMethodDefinition() != RawSqlMethodInfo)
        {
            return null;
        }

        // The SQL string is passed as the first argument
        if (arguments[0] is not SqlConstantExpression { Value: string sql })
        {
            throw new InvalidOperationException(
                "ClickHouseFunctions.RawSql requires a constant string argument.");
        }

        var returnType = method.ReturnType;
        var typeMapping = _typeMappingSource.FindMapping(returnType);
        return new ClickHouseRawSqlExpression(sql, returnType, typeMapping);
    }
}
