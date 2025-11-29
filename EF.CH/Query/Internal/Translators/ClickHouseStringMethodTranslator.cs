using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates .NET string methods to ClickHouse SQL functions.
/// Uses LIKE for Contains/StartsWith/EndsWith per design decision.
/// </summary>
public class ClickHouseStringMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    // String methods we handle
    private static readonly MethodInfo ContainsMethod =
        typeof(string).GetMethod(nameof(string.Contains), new[] { typeof(string) })!;

    private static readonly MethodInfo StartsWithMethod =
        typeof(string).GetMethod(nameof(string.StartsWith), new[] { typeof(string) })!;

    private static readonly MethodInfo EndsWithMethod =
        typeof(string).GetMethod(nameof(string.EndsWith), new[] { typeof(string) })!;

    private static readonly MethodInfo ToUpperMethod =
        typeof(string).GetMethod(nameof(string.ToUpper), Type.EmptyTypes)!;

    private static readonly MethodInfo ToLowerMethod =
        typeof(string).GetMethod(nameof(string.ToLower), Type.EmptyTypes)!;

    private static readonly MethodInfo TrimMethod =
        typeof(string).GetMethod(nameof(string.Trim), Type.EmptyTypes)!;

    private static readonly MethodInfo TrimStartMethod =
        typeof(string).GetMethod(nameof(string.TrimStart), Type.EmptyTypes)!;

    private static readonly MethodInfo TrimEndMethod =
        typeof(string).GetMethod(nameof(string.TrimEnd), Type.EmptyTypes)!;

    private static readonly MethodInfo SubstringMethodOneArg =
        typeof(string).GetMethod(nameof(string.Substring), new[] { typeof(int) })!;

    private static readonly MethodInfo SubstringMethodTwoArgs =
        typeof(string).GetMethod(nameof(string.Substring), new[] { typeof(int), typeof(int) })!;

    private static readonly MethodInfo ReplaceMethod =
        typeof(string).GetMethod(nameof(string.Replace), new[] { typeof(string), typeof(string) })!;

    private static readonly MethodInfo IndexOfMethod =
        typeof(string).GetMethod(nameof(string.IndexOf), new[] { typeof(string) })!;

    private static readonly MethodInfo IsNullOrEmptyMethod =
        typeof(string).GetMethod(nameof(string.IsNullOrEmpty), new[] { typeof(string) })!;

    private static readonly MethodInfo IsNullOrWhiteSpaceMethod =
        typeof(string).GetMethod(nameof(string.IsNullOrWhiteSpace), new[] { typeof(string) })!;

    private static readonly MethodInfo ConcatMethodTwoArgs =
        typeof(string).GetMethod(nameof(string.Concat), new[] { typeof(string), typeof(string) })!;

    private static readonly MethodInfo ConcatMethodThreeArgs =
        typeof(string).GetMethod(nameof(string.Concat), new[] { typeof(string), typeof(string), typeof(string) })!;

    public ClickHouseStringMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(string))
        {
            return null;
        }

        // Instance methods (called on a string instance)
        if (instance is not null)
        {
            // Contains: instance LIKE '%' || pattern || '%'
            if (method == ContainsMethod)
            {
                return TranslateLikePattern(instance, arguments[0], LikePatternMode.Contains);
            }

            // StartsWith: instance LIKE pattern || '%'
            if (method == StartsWithMethod)
            {
                return TranslateLikePattern(instance, arguments[0], LikePatternMode.StartsWith);
            }

            // EndsWith: instance LIKE '%' || pattern
            if (method == EndsWithMethod)
            {
                return TranslateLikePattern(instance, arguments[0], LikePatternMode.EndsWith);
            }

            // ToUpper: upperUTF8(instance)
            if (method == ToUpperMethod)
            {
                return _sqlExpressionFactory.Function(
                    "upperUTF8",
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(string));
            }

            // ToLower: lowerUTF8(instance)
            if (method == ToLowerMethod)
            {
                return _sqlExpressionFactory.Function(
                    "lowerUTF8",
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(string));
            }

            // Trim: trim(instance)
            if (method == TrimMethod)
            {
                return _sqlExpressionFactory.Function(
                    "trim",
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(string));
            }

            // TrimStart: trimLeft(instance)
            if (method == TrimStartMethod)
            {
                return _sqlExpressionFactory.Function(
                    "trimLeft",
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(string));
            }

            // TrimEnd: trimRight(instance)
            if (method == TrimEndMethod)
            {
                return _sqlExpressionFactory.Function(
                    "trimRight",
                    new[] { instance },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(string));
            }

            // Substring(startIndex): substring(instance, startIndex + 1)
            // ClickHouse uses 1-based indexing
            if (method == SubstringMethodOneArg)
            {
                return _sqlExpressionFactory.Function(
                    "substring",
                    new SqlExpression[]
                    {
                        instance,
                        _sqlExpressionFactory.Add(
                            arguments[0],
                            _sqlExpressionFactory.Constant(1))
                    },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true, false },
                    typeof(string));
            }

            // Substring(startIndex, length): substring(instance, startIndex + 1, length)
            if (method == SubstringMethodTwoArgs)
            {
                return _sqlExpressionFactory.Function(
                    "substring",
                    new SqlExpression[]
                    {
                        instance,
                        _sqlExpressionFactory.Add(
                            arguments[0],
                            _sqlExpressionFactory.Constant(1)),
                        arguments[1]
                    },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true, false, false },
                    typeof(string));
            }

            // Replace: replaceAll(instance, oldValue, newValue)
            if (method == ReplaceMethod)
            {
                return _sqlExpressionFactory.Function(
                    "replaceAll",
                    new[] { instance, arguments[0], arguments[1] },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true, true, true },
                    typeof(string));
            }

            // IndexOf: positionUTF8(instance, substring) - 1
            // ClickHouse returns 1-based, .NET expects 0-based
            if (method == IndexOfMethod)
            {
                return _sqlExpressionFactory.Subtract(
                    _sqlExpressionFactory.Function(
                        "positionUTF8",
                        new[] { instance, arguments[0] },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true, true },
                        typeof(int)),
                    _sqlExpressionFactory.Constant(1));
            }
        }

        // Static methods
        // IsNullOrEmpty: argument IS NULL OR empty(argument)
        if (method == IsNullOrEmptyMethod)
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.IsNull(argument),
                _sqlExpressionFactory.Function(
                    "empty",
                    new[] { argument },
                    nullable: false,
                    argumentsPropagateNullability: new[] { false },
                    typeof(bool)));
        }

        // IsNullOrWhiteSpace: argument IS NULL OR empty(trim(argument))
        if (method == IsNullOrWhiteSpaceMethod)
        {
            var argument = arguments[0];
            return _sqlExpressionFactory.OrElse(
                _sqlExpressionFactory.IsNull(argument),
                _sqlExpressionFactory.Function(
                    "empty",
                    new SqlExpression[]
                    {
                        _sqlExpressionFactory.Function(
                            "trim",
                            new[] { argument },
                            nullable: true,
                            argumentsPropagateNullability: new[] { true },
                            typeof(string))
                    },
                    nullable: false,
                    argumentsPropagateNullability: new[] { false },
                    typeof(bool)));
        }

        // Concat: concat(arg1, arg2, ...)
        if (method == ConcatMethodTwoArgs || method == ConcatMethodThreeArgs)
        {
            return _sqlExpressionFactory.Function(
                "concat",
                arguments,
                nullable: true,
                argumentsPropagateNullability: arguments.Select(_ => true).ToArray(),
                typeof(string));
        }

        return null;
    }

    /// <summary>
    /// Translates Contains/StartsWith/EndsWith to LIKE patterns.
    /// </summary>
    private SqlExpression TranslateLikePattern(
        SqlExpression instance,
        SqlExpression pattern,
        LikePatternMode mode)
    {
        // If the pattern is a constant, we can build the LIKE pattern directly
        if (pattern is SqlConstantExpression constantPattern &&
            constantPattern.Value is string stringPattern)
        {
            // Escape LIKE special characters
            var escapedPattern = EscapeLikePattern(stringPattern);

            var likePattern = mode switch
            {
                LikePatternMode.Contains => $"%{escapedPattern}%",
                LikePatternMode.StartsWith => $"{escapedPattern}%",
                LikePatternMode.EndsWith => $"%{escapedPattern}",
                _ => escapedPattern
            };

            return _sqlExpressionFactory.Like(
                instance,
                _sqlExpressionFactory.Constant(likePattern));
        }

        // For non-constant patterns, we need to use concat to build the pattern
        // This is less efficient but necessary for dynamic values
        SqlExpression likePatternExpr = mode switch
        {
            LikePatternMode.Contains => _sqlExpressionFactory.Function(
                "concat",
                new SqlExpression[]
                {
                    _sqlExpressionFactory.Constant("%"),
                    pattern,
                    _sqlExpressionFactory.Constant("%")
                },
                nullable: true,
                argumentsPropagateNullability: new[] { false, true, false },
                typeof(string)),

            LikePatternMode.StartsWith => _sqlExpressionFactory.Function(
                "concat",
                new SqlExpression[]
                {
                    pattern,
                    _sqlExpressionFactory.Constant("%")
                },
                nullable: true,
                argumentsPropagateNullability: new[] { true, false },
                typeof(string)),

            LikePatternMode.EndsWith => _sqlExpressionFactory.Function(
                "concat",
                new SqlExpression[]
                {
                    _sqlExpressionFactory.Constant("%"),
                    pattern
                },
                nullable: true,
                argumentsPropagateNullability: new[] { false, true },
                typeof(string)),

            _ => pattern
        };

        return _sqlExpressionFactory.Like(instance, likePatternExpr);
    }

    /// <summary>
    /// Escapes special LIKE pattern characters.
    /// </summary>
    private static string EscapeLikePattern(string pattern)
    {
        var builder = new StringBuilder(pattern.Length);
        foreach (var c in pattern)
        {
            switch (c)
            {
                case '%':
                    builder.Append("\\%");
                    break;
                case '_':
                    builder.Append("\\_");
                    break;
                case '\\':
                    builder.Append("\\\\");
                    break;
                default:
                    builder.Append(c);
                    break;
            }
        }
        return builder.ToString();
    }

    private enum LikePatternMode
    {
        Contains,
        StartsWith,
        EndsWith
    }
}

/// <summary>
/// Translates string.Length property access to ClickHouse char_length function.
/// </summary>
public class ClickHouseStringMemberTranslator : IMemberTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    private static readonly MemberInfo LengthMember =
        typeof(string).GetProperty(nameof(string.Length))!;

    public ClickHouseStringMemberTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (member.DeclaringType != typeof(string) || instance is null)
        {
            return null;
        }

        // Length: char_length(instance) or lengthUTF8(instance)
        if (member == LengthMember)
        {
            return _sqlExpressionFactory.Function(
                "char_length",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int));
        }

        return null;
    }
}
