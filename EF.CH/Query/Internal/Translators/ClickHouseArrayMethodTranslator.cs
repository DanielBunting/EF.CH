using System.Reflection;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates .NET array and list methods to ClickHouse SQL functions.
/// </summary>
/// <remarks>
/// Supported translations:
/// - array.Contains(x) → has(array, x)
/// - array.Length / list.Count → length(array)
/// - array[i] → array[i+1] (ClickHouse uses 1-based indexing)
/// - Enumerable.Contains(array, x) → has(array, x)
/// - Enumerable.Any(array) → notEmpty(array)
/// - Enumerable.Any(array, predicate) → arrayExists(lambda, array)
/// - Enumerable.All(array, predicate) → arrayAll(lambda, array)
/// - Enumerable.Count(array) → length(array)
/// - Enumerable.Count(array, predicate) → arrayCount(lambda, array)
/// - Enumerable.First(array) → arrayElement(array, 1)
/// - Enumerable.FirstOrDefault(array) → arrayElement(array, 1)
/// </remarks>
public class ClickHouseArrayMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    // ICollection<T>.Contains(T item) - generic interface method
    private static readonly MethodInfo ListContainsMethod =
        typeof(ICollection<>).GetMethod(nameof(ICollection<object>.Contains))!;

    // Enumerable.Contains<TSource>(IEnumerable<TSource>, TSource)
    private static readonly MethodInfo EnumerableContainsMethod =
        typeof(Enumerable).GetMethods()
            .First(m => m.Name == nameof(Enumerable.Contains) && m.GetParameters().Length == 2);

    // Enumerable.Any<TSource>(IEnumerable<TSource>)
    private static readonly MethodInfo EnumerableAnyWithoutPredicateMethod =
        typeof(Enumerable).GetMethods()
            .First(m => m.Name == nameof(Enumerable.Any) && m.GetParameters().Length == 1);

    // Enumerable.Count<TSource>(IEnumerable<TSource>)
    private static readonly MethodInfo EnumerableCountWithoutPredicateMethod =
        typeof(Enumerable).GetMethods()
            .First(m => m.Name == nameof(Enumerable.Count) && m.GetParameters().Length == 1);

    // Enumerable.First<TSource>(IEnumerable<TSource>)
    private static readonly MethodInfo EnumerableFirstWithoutPredicateMethod =
        typeof(Enumerable).GetMethods()
            .First(m => m.Name == nameof(Enumerable.First) && m.GetParameters().Length == 1);

    // Enumerable.FirstOrDefault<TSource>(IEnumerable<TSource>)
    private static readonly MethodInfo EnumerableFirstOrDefaultWithoutPredicateMethod =
        typeof(Enumerable).GetMethods()
            .First(m => m.Name == nameof(Enumerable.FirstOrDefault) && m.GetParameters().Length == 1);

    // Enumerable.Last<TSource>(IEnumerable<TSource>)
    private static readonly MethodInfo EnumerableLastWithoutPredicateMethod =
        typeof(Enumerable).GetMethods()
            .First(m => m.Name == nameof(Enumerable.Last) && m.GetParameters().Length == 1);

    // Enumerable.LastOrDefault<TSource>(IEnumerable<TSource>)
    private static readonly MethodInfo EnumerableLastOrDefaultWithoutPredicateMethod =
        typeof(Enumerable).GetMethods()
            .First(m => m.Name == nameof(Enumerable.LastOrDefault) && m.GetParameters().Length == 1);

    public ClickHouseArrayMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Handle ClickHouseAggregates array combinator methods (e.g., array.ArraySum())
        if (instance is not null && method.DeclaringType?.FullName == "EF.CH.Extensions.ClickHouseAggregates")
        {
            return TranslateArrayCombinator(instance, method, arguments);
        }

        // Handle instance methods on arrays/lists (e.g., list.Contains(x))
        if (instance is not null && IsArrayOrListType(method.DeclaringType))
        {
            return TranslateInstanceMethod(instance, method, arguments);
        }

        // Handle static Enumerable methods
        if (method.DeclaringType == typeof(Enumerable))
        {
            return TranslateEnumerableMethod(method, arguments);
        }

        // Handle ICollection<T>.Contains called on array/list
        if (instance is not null && method.Name == nameof(ICollection<object>.Contains))
        {
            var declaringType = method.DeclaringType;
            if (declaringType is not null && declaringType.IsGenericType &&
                declaringType.GetGenericTypeDefinition() == typeof(ICollection<>))
            {
                // has(array, element)
                return _sqlExpressionFactory.Function(
                    "has",
                    new[] { instance, arguments[0] },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true, true },
                    typeof(bool));
            }
        }

        return null;
    }

    /// <summary>
    /// Translates ClickHouseAggregates array combinator methods.
    /// </summary>
    private SqlExpression? TranslateArrayCombinator(
        SqlExpression instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        return method.Name switch
        {
            // ArraySum(array) → arraySum(array)
            "ArraySum" => _sqlExpressionFactory.Function(
                "arraySum",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                method.ReturnType),

            // ArrayAvg(array) → arrayAvg(array)
            "ArrayAvg" => _sqlExpressionFactory.Function(
                "arrayAvg",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(double)),

            // ArrayMin(array) → arrayMin(array)
            "ArrayMin" => _sqlExpressionFactory.Function(
                "arrayMin",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                method.ReturnType),

            // ArrayMax(array) → arrayMax(array)
            "ArrayMax" => _sqlExpressionFactory.Function(
                "arrayMax",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                method.ReturnType),

            // ArrayCount(array) → length(array)
            // ArrayCount(array, predicate) → arrayCount(lambda, array) - predicate handling requires lambda support
            "ArrayCount" when arguments.Count == 0 => _sqlExpressionFactory.Function(
                "length",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int)),

            _ => null
        };
    }

    private SqlExpression? TranslateInstanceMethod(
        SqlExpression instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        // Contains(T item) → has(array, item)
        if (method.Name == nameof(List<object>.Contains) && arguments.Count == 1)
        {
            return _sqlExpressionFactory.Function(
                "has",
                new[] { instance, arguments[0] },
                nullable: true,
                argumentsPropagateNullability: new[] { true, true },
                typeof(bool));
        }

        return null;
    }

    private SqlExpression? TranslateEnumerableMethod(
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        // All Enumerable methods take the source as the first argument
        if (arguments.Count == 0)
        {
            return null;
        }

        var source = arguments[0];

        // Skip Nested types - they have their own translator with first-field pattern
        if (source.TypeMapping is ClickHouseNestedTypeMapping)
        {
            return null;
        }

        // Check if the source is an array type
        if (!IsArrayOrListType(source.Type))
        {
            return null;
        }

        // Enumerable.Contains(source, item) → has(source, item)
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == EnumerableContainsMethod)
        {
            return _sqlExpressionFactory.Function(
                "has",
                new[] { source, arguments[1] },
                nullable: true,
                argumentsPropagateNullability: new[] { true, true },
                typeof(bool));
        }

        // Enumerable.Any(source) → notEmpty(source)
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == EnumerableAnyWithoutPredicateMethod)
        {
            return _sqlExpressionFactory.Function(
                "notEmpty",
                new[] { source },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(bool));
        }

        // Enumerable.Count(source) → length(source)
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == EnumerableCountWithoutPredicateMethod)
        {
            return _sqlExpressionFactory.Function(
                "length",
                new[] { source },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int));
        }

        // Enumerable.First(source) → arrayElement(source, 1)
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == EnumerableFirstWithoutPredicateMethod)
        {
            return _sqlExpressionFactory.Function(
                "arrayElement",
                new SqlExpression[] { source, _sqlExpressionFactory.Constant(1) },
                nullable: true,
                argumentsPropagateNullability: new[] { true, false },
                method.ReturnType);
        }

        // Enumerable.FirstOrDefault(source) → arrayElement(source, 1)
        // Note: ClickHouse returns default value for out-of-bounds access
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == EnumerableFirstOrDefaultWithoutPredicateMethod)
        {
            return _sqlExpressionFactory.Function(
                "arrayElement",
                new SqlExpression[] { source, _sqlExpressionFactory.Constant(1) },
                nullable: true,
                argumentsPropagateNullability: new[] { true, false },
                method.ReturnType);
        }

        // Enumerable.Last(source) → arrayElement(source, -1)
        // ClickHouse supports negative indexing from end
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == EnumerableLastWithoutPredicateMethod)
        {
            return _sqlExpressionFactory.Function(
                "arrayElement",
                new SqlExpression[] { source, _sqlExpressionFactory.Constant(-1) },
                nullable: true,
                argumentsPropagateNullability: new[] { true, false },
                method.ReturnType);
        }

        // Enumerable.LastOrDefault(source) → arrayElement(source, -1)
        if (method.IsGenericMethod &&
            method.GetGenericMethodDefinition() == EnumerableLastOrDefaultWithoutPredicateMethod)
        {
            return _sqlExpressionFactory.Function(
                "arrayElement",
                new SqlExpression[] { source, _sqlExpressionFactory.Constant(-1) },
                nullable: true,
                argumentsPropagateNullability: new[] { true, false },
                method.ReturnType);
        }

        return null;
    }

    /// <summary>
    /// Checks if a type is an array or generic list/collection.
    /// </summary>
    private static bool IsArrayOrListType(Type? type)
    {
        if (type is null)
        {
            return false;
        }

        if (type.IsArray)
        {
            return true;
        }

        if (type.IsGenericType)
        {
            var genericDef = type.GetGenericTypeDefinition();
            return genericDef == typeof(List<>) ||
                   genericDef == typeof(IList<>) ||
                   genericDef == typeof(ICollection<>) ||
                   genericDef == typeof(IEnumerable<>) ||
                   genericDef == typeof(IReadOnlyList<>) ||
                   genericDef == typeof(IReadOnlyCollection<>);
        }

        return false;
    }
}

/// <summary>
/// Translates array member access (Length, Count) to ClickHouse functions.
/// </summary>
public class ClickHouseArrayMemberTranslator : IMemberTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseArrayMemberTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null)
        {
            return null;
        }

        // Skip Nested types - they have their own translator with first-field pattern
        if (instance.TypeMapping is ClickHouseNestedTypeMapping)
        {
            return null;
        }

        var declaringType = member.DeclaringType;

        // Array.Length → length(array)
        if (declaringType?.IsArray == true && member.Name == "Length")
        {
            return _sqlExpressionFactory.Function(
                "length",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int));
        }

        // List<T>.Count, ICollection<T>.Count, etc. → length(array)
        if (declaringType is not null && member.Name == "Count")
        {
            if (declaringType.IsGenericType)
            {
                var genericDef = declaringType.GetGenericTypeDefinition();
                if (genericDef == typeof(List<>) ||
                    genericDef == typeof(IList<>) ||
                    genericDef == typeof(ICollection<>) ||
                    genericDef == typeof(IReadOnlyCollection<>) ||
                    genericDef == typeof(IReadOnlyList<>))
                {
                    return _sqlExpressionFactory.Function(
                        "length",
                        new[] { instance },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(int));
                }
            }
        }

        return null;
    }
}
