using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates Guid methods to ClickHouse SQL functions.
/// </summary>
public class ClickHouseGuidMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    private static readonly MethodInfo NewGuidMethod =
        typeof(Guid).GetMethod(nameof(Guid.NewGuid), BindingFlags.Public | BindingFlags.Static)!;

    public ClickHouseGuidMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Guid.NewGuid() → generateUUIDv4()
        if (method == NewGuidMethod)
        {
            return _sqlExpressionFactory.Function(
                "generateUUIDv4",
                Array.Empty<SqlExpression>(),
                nullable: false,
                argumentsPropagateNullability: Array.Empty<bool>(),
                typeof(Guid));
        }

        return null;
    }
}

/// <summary>
/// Translates Guid member access to ClickHouse SQL.
/// </summary>
public class ClickHouseGuidMemberTranslator : IMemberTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseGuidMemberTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Handle Guid.Empty as a constant
        if (member.DeclaringType == typeof(Guid) && member.Name == nameof(Guid.Empty))
        {
            return _sqlExpressionFactory.Constant(Guid.Empty);
        }

        return null;
    }
}
