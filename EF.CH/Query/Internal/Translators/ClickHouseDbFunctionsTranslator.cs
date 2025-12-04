using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates EF.Functions calls that are common across providers.
/// </summary>
public class ClickHouseDbFunctionsTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    private static readonly MethodInfo LikeMethod =
        typeof(DbFunctionsExtensions).GetMethod(
            nameof(DbFunctionsExtensions.Like),
            new[] { typeof(DbFunctions), typeof(string), typeof(string) })!;

    private static readonly MethodInfo LikeMethodWithEscape =
        typeof(DbFunctionsExtensions).GetMethod(
            nameof(DbFunctionsExtensions.Like),
            new[] { typeof(DbFunctions), typeof(string), typeof(string), typeof(string) })!;

    public ClickHouseDbFunctionsTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Handle EF.Functions.Like(matchExpression, pattern)
        if (method == LikeMethod)
        {
            // arguments[0] is DbFunctions instance (ignored)
            // arguments[1] is the match expression (arguments index is shifted because of how EF handles extension methods)
            return _sqlExpressionFactory.Like(
                arguments[1],  // matchExpression
                arguments[2]); // pattern
        }

        // Handle EF.Functions.Like(matchExpression, pattern, escapeCharacter)
        if (method == LikeMethodWithEscape)
        {
            return _sqlExpressionFactory.Like(
                arguments[1],  // matchExpression
                arguments[2],  // pattern
                arguments[3]); // escapeCharacter
        }

        return null;
    }
}
