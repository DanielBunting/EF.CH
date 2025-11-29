using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates .NET Math methods to ClickHouse SQL functions.
/// </summary>
public class ClickHouseMathMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    // Single-argument Math method mappings
    private static readonly Dictionary<string, string> SingleArgMathMappings = new()
    {
        // Basic functions
        [nameof(Math.Abs)] = "abs",
        [nameof(Math.Sign)] = "sign",
        [nameof(Math.Sqrt)] = "sqrt",
        [nameof(Math.Cbrt)] = "cbrt",
        [nameof(Math.Exp)] = "exp",

        // Rounding functions
        [nameof(Math.Floor)] = "floor",
        [nameof(Math.Ceiling)] = "ceil",
        [nameof(Math.Truncate)] = "trunc",

        // Logarithmic functions
        [nameof(Math.Log)] = "log",      // Natural log (single arg)
        [nameof(Math.Log10)] = "log10",
        [nameof(Math.Log2)] = "log2",

        // Trigonometric functions
        [nameof(Math.Sin)] = "sin",
        [nameof(Math.Cos)] = "cos",
        [nameof(Math.Tan)] = "tan",
        [nameof(Math.Asin)] = "asin",
        [nameof(Math.Acos)] = "acos",
        [nameof(Math.Atan)] = "atan",

        // Hyperbolic functions
        [nameof(Math.Sinh)] = "sinh",
        [nameof(Math.Cosh)] = "cosh",
        [nameof(Math.Tanh)] = "tanh",
        [nameof(Math.Asinh)] = "asinh",
        [nameof(Math.Acosh)] = "acosh",
        [nameof(Math.Atanh)] = "atanh",
    };

    // Two-argument Math method mappings
    private static readonly Dictionary<string, string> TwoArgMathMappings = new()
    {
        [nameof(Math.Pow)] = "pow",
        [nameof(Math.Atan2)] = "atan2",
        [nameof(Math.Min)] = "least",
        [nameof(Math.Max)] = "greatest",
    };

    public ClickHouseMathMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(Math) && method.DeclaringType != typeof(MathF))
        {
            return null;
        }

        var methodName = method.Name;

        // Single-argument methods
        if (arguments.Count == 1 && SingleArgMathMappings.TryGetValue(methodName, out var singleArgFunc))
        {
            return _sqlExpressionFactory.Function(
                singleArgFunc,
                arguments,
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                method.ReturnType);
        }

        // Two-argument methods
        if (arguments.Count == 2 && TwoArgMathMappings.TryGetValue(methodName, out var twoArgFunc))
        {
            return _sqlExpressionFactory.Function(
                twoArgFunc,
                arguments,
                nullable: true,
                argumentsPropagateNullability: new[] { true, true },
                method.ReturnType);
        }

        // Special handling for Round
        if (methodName == nameof(Math.Round))
        {
            if (arguments.Count == 1)
            {
                // Round to nearest integer
                return _sqlExpressionFactory.Function(
                    "round",
                    arguments,
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    method.ReturnType);
            }

            if (arguments.Count == 2)
            {
                // Round to specified decimal places
                return _sqlExpressionFactory.Function(
                    "round",
                    arguments,
                    nullable: true,
                    argumentsPropagateNullability: new[] { true, false },
                    method.ReturnType);
            }
        }

        // Special handling for Log with base
        if (methodName == nameof(Math.Log) && arguments.Count == 2)
        {
            // Math.Log(value, base) → log(base, value) in ClickHouse
            // Or implement as: log(value) / log(base)
            return _sqlExpressionFactory.Divide(
                _sqlExpressionFactory.Function(
                    "log",
                    new[] { arguments[0] },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(double)),
                _sqlExpressionFactory.Function(
                    "log",
                    new[] { arguments[1] },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(double)));
        }

        // Clamp: greatest(min, least(max, value))
        if (methodName == nameof(Math.Clamp) && arguments.Count == 3)
        {
            // arguments[0] = value, arguments[1] = min, arguments[2] = max
            return _sqlExpressionFactory.Function(
                "greatest",
                new SqlExpression[]
                {
                    arguments[1], // min
                    _sqlExpressionFactory.Function(
                        "least",
                        new[] { arguments[2], arguments[0] }, // max, value
                        nullable: true,
                        argumentsPropagateNullability: new[] { true, true },
                        method.ReturnType)
                },
                nullable: true,
                argumentsPropagateNullability: new[] { true, true },
                method.ReturnType);
        }

        // FusedMultiplyAdd: (x * y) + z
        if (methodName == nameof(Math.FusedMultiplyAdd) && arguments.Count == 3)
        {
            return _sqlExpressionFactory.Add(
                _sqlExpressionFactory.Multiply(arguments[0], arguments[1]),
                arguments[2]);
        }

        return null;
    }
}

/// <summary>
/// Translates Convert methods to ClickHouse type conversions.
/// </summary>
public class ClickHouseConvertMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    private static readonly Dictionary<string, (string Function, Type ReturnType)> ConvertMappings = new()
    {
        [nameof(Convert.ToBoolean)] = ("toBool", typeof(bool)),
        [nameof(Convert.ToByte)] = ("toUInt8", typeof(byte)),
        [nameof(Convert.ToSByte)] = ("toInt8", typeof(sbyte)),
        [nameof(Convert.ToInt16)] = ("toInt16", typeof(short)),
        [nameof(Convert.ToUInt16)] = ("toUInt16", typeof(ushort)),
        [nameof(Convert.ToInt32)] = ("toInt32", typeof(int)),
        [nameof(Convert.ToUInt32)] = ("toUInt32", typeof(uint)),
        [nameof(Convert.ToInt64)] = ("toInt64", typeof(long)),
        [nameof(Convert.ToUInt64)] = ("toUInt64", typeof(ulong)),
        [nameof(Convert.ToSingle)] = ("toFloat32", typeof(float)),
        [nameof(Convert.ToDouble)] = ("toFloat64", typeof(double)),
        [nameof(Convert.ToString)] = ("toString", typeof(string)),
    };

    public ClickHouseConvertMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(Convert))
        {
            return null;
        }

        if (arguments.Count >= 1 && ConvertMappings.TryGetValue(method.Name, out var mapping))
        {
            return _sqlExpressionFactory.Function(
                mapping.Function,
                new[] { arguments[0] },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                mapping.ReturnType);
        }

        return null;
    }
}
