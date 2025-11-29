using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates DateTime member access (properties) to ClickHouse SQL functions.
/// </summary>
public class ClickHouseDateTimeMemberTranslator : IMemberTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    // DateTime instance property mappings
    private static readonly Dictionary<string, string> DateTimeMemberMappings = new()
    {
        [nameof(DateTime.Year)] = "toYear",
        [nameof(DateTime.Month)] = "toMonth",
        [nameof(DateTime.Day)] = "toDayOfMonth",
        [nameof(DateTime.Hour)] = "toHour",
        [nameof(DateTime.Minute)] = "toMinute",
        [nameof(DateTime.Second)] = "toSecond",
        [nameof(DateTime.Millisecond)] = "toMillisecond",
        [nameof(DateTime.DayOfYear)] = "toDayOfYear",
    };

    public ClickHouseDateTimeMemberTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        var declaringType = member.DeclaringType;

        // Handle DateTime and DateTimeOffset
        if (declaringType != typeof(DateTime) &&
            declaringType != typeof(DateTimeOffset) &&
            declaringType != typeof(DateOnly))
        {
            return null;
        }

        // Instance properties (require an instance)
        if (instance is not null)
        {
            // Standard date part extraction
            if (DateTimeMemberMappings.TryGetValue(member.Name, out var clickHouseFunction))
            {
                return _sqlExpressionFactory.Convert(
                    _sqlExpressionFactory.Function(
                        clickHouseFunction,
                        new[] { instance },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(int)),
                    typeof(int));
            }

            // DayOfWeek: Special handling to convert ClickHouse (Mon=1..Sun=7) to .NET (Sun=0..Sat=6)
            // Formula: toDayOfWeek(x) % 7 gives Mon=1..Sat=6, Sun=0
            if (member.Name == nameof(DateTime.DayOfWeek))
            {
                return _sqlExpressionFactory.Modulo(
                    _sqlExpressionFactory.Function(
                        "toDayOfWeek",
                        new[] { instance },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(int)),
                    _sqlExpressionFactory.Constant(7));
            }

            // Date property: toDate(instance)
            if (member.Name == nameof(DateTime.Date))
            {
                return _sqlExpressionFactory.Function(
                    "toDate",
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(DateTime));
            }

            // TimeOfDay: Extract time as seconds since midnight
            // toHour(x) * 3600 + toMinute(x) * 60 + toSecond(x)
            if (member.Name == nameof(DateTime.TimeOfDay))
            {
                // This returns TimeSpan - complex to implement fully
                // For now, return null to fall back to client evaluation
                return null;
            }

            // Ticks: Complex conversion from Unix timestamp
            // Formula: toUnixTimestamp64Milli(x) * 10000 + 621355968000000000
            if (member.Name == nameof(DateTime.Ticks))
            {
                const long ticksOffset = 621355968000000000L; // Ticks from 0001-01-01 to 1970-01-01

                return _sqlExpressionFactory.Add(
                    _sqlExpressionFactory.Multiply(
                        _sqlExpressionFactory.Function(
                            "toUnixTimestamp64Milli",
                            new[] { instance },
                            nullable: true,
                            argumentsPropagateNullability: new[] { true },
                            typeof(long)),
                        _sqlExpressionFactory.Constant(10000L)),
                    _sqlExpressionFactory.Constant(ticksOffset));
            }
        }

        // Static properties (no instance required)
        if (instance is null)
        {
            // DateTime.Now: now()
            if (member.Name == nameof(DateTime.Now) && declaringType == typeof(DateTime))
            {
                return _sqlExpressionFactory.Function(
                    "now",
                    Array.Empty<SqlExpression>(),
                    nullable: false,
                    argumentsPropagateNullability: Array.Empty<bool>(),
                    typeof(DateTime));
            }

            // DateTime.UtcNow: now('UTC')
            if (member.Name == nameof(DateTime.UtcNow) && declaringType == typeof(DateTime))
            {
                return _sqlExpressionFactory.Function(
                    "now",
                    new SqlExpression[] { _sqlExpressionFactory.Constant("UTC") },
                    nullable: false,
                    argumentsPropagateNullability: new[] { false },
                    typeof(DateTime));
            }

            // DateTime.Today: today()
            if (member.Name == nameof(DateTime.Today) && declaringType == typeof(DateTime))
            {
                return _sqlExpressionFactory.Function(
                    "today",
                    Array.Empty<SqlExpression>(),
                    nullable: false,
                    argumentsPropagateNullability: Array.Empty<bool>(),
                    typeof(DateTime));
            }

            // DateTimeOffset.Now and DateTimeOffset.UtcNow
            if (declaringType == typeof(DateTimeOffset))
            {
                if (member.Name == nameof(DateTimeOffset.Now))
                {
                    return _sqlExpressionFactory.Function(
                        "now",
                        Array.Empty<SqlExpression>(),
                        nullable: false,
                        argumentsPropagateNullability: Array.Empty<bool>(),
                        typeof(DateTimeOffset));
                }

                if (member.Name == nameof(DateTimeOffset.UtcNow))
                {
                    return _sqlExpressionFactory.Function(
                        "now",
                        new SqlExpression[] { _sqlExpressionFactory.Constant("UTC") },
                        nullable: false,
                        argumentsPropagateNullability: new[] { false },
                        typeof(DateTimeOffset));
                }
            }
        }

        return null;
    }
}

/// <summary>
/// Translates DateTime method calls to ClickHouse SQL functions.
/// </summary>
public class ClickHouseDateTimeMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    // DateTime.Add* method mappings
    private static readonly Dictionary<string, string> AddMethodMappings = new()
    {
        [nameof(DateTime.AddYears)] = "addYears",
        [nameof(DateTime.AddMonths)] = "addMonths",
        [nameof(DateTime.AddDays)] = "addDays",
        [nameof(DateTime.AddHours)] = "addHours",
        [nameof(DateTime.AddMinutes)] = "addMinutes",
        [nameof(DateTime.AddSeconds)] = "addSeconds",
        [nameof(DateTime.AddMilliseconds)] = "addMilliseconds",
    };

    public ClickHouseDateTimeMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        var declaringType = method.DeclaringType;

        if (declaringType != typeof(DateTime) &&
            declaringType != typeof(DateTimeOffset) &&
            declaringType != typeof(DateOnly))
        {
            return null;
        }

        if (instance is null)
        {
            return null;
        }

        // Handle Add* methods
        if (AddMethodMappings.TryGetValue(method.Name, out var clickHouseFunction))
        {
            // For AddMilliseconds, ClickHouse expects integer, but .NET uses double
            var argument = arguments[0];

            // If it's AddMilliseconds with a double, we need to cast to Int64
            if (method.Name == nameof(DateTime.AddMilliseconds) &&
                argument.Type == typeof(double))
            {
                argument = _sqlExpressionFactory.Convert(argument, typeof(long));
            }

            return _sqlExpressionFactory.Function(
                clickHouseFunction,
                new[] { instance, argument },
                nullable: true,
                argumentsPropagateNullability: new[] { true, false },
                method.ReturnType);
        }

        // AddTicks: Add ticks / 10000 as milliseconds
        if (method.Name == nameof(DateTime.AddTicks))
        {
            var ticksToMs = _sqlExpressionFactory.Divide(
                arguments[0],
                _sqlExpressionFactory.Constant(10000L));

            return _sqlExpressionFactory.Function(
                "addMilliseconds",
                new SqlExpression[] { instance, ticksToMs },
                nullable: true,
                argumentsPropagateNullability: new[] { true, false },
                method.ReturnType);
        }

        return null;
    }
}

/// <summary>
/// Translates DateOnly member access to ClickHouse SQL functions.
/// </summary>
public class ClickHouseDateOnlyMemberTranslator : IMemberTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    private static readonly Dictionary<string, string> DateOnlyMemberMappings = new()
    {
        [nameof(DateOnly.Year)] = "toYear",
        [nameof(DateOnly.Month)] = "toMonth",
        [nameof(DateOnly.Day)] = "toDayOfMonth",
        [nameof(DateOnly.DayOfYear)] = "toDayOfYear",
    };

    public ClickHouseDateOnlyMemberTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (member.DeclaringType != typeof(DateOnly) || instance is null)
        {
            return null;
        }

        if (DateOnlyMemberMappings.TryGetValue(member.Name, out var clickHouseFunction))
        {
            return _sqlExpressionFactory.Convert(
                _sqlExpressionFactory.Function(
                    clickHouseFunction,
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(int)),
                typeof(int));
        }

        // DayOfWeek for DateOnly
        if (member.Name == nameof(DateOnly.DayOfWeek))
        {
            return _sqlExpressionFactory.Modulo(
                _sqlExpressionFactory.Function(
                    "toDayOfWeek",
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(int)),
                _sqlExpressionFactory.Constant(7));
        }

        return null;
    }
}
