using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates .NET dictionary methods to ClickHouse SQL functions.
/// </summary>
/// <remarks>
/// Supported translations:
/// - dictionary.ContainsKey(key) → mapContains(map, key)
/// - dictionary[key] → map[key] (handled via indexer)
/// - dictionary.Keys → mapKeys(map)
/// - dictionary.Values → mapValues(map)
/// - dictionary.Count → length(mapKeys(map))
/// </remarks>
public class ClickHouseMapMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    // Dictionary<TKey, TValue>.ContainsKey(TKey key)
    private static readonly MethodInfo DictionaryContainsKeyMethod =
        typeof(Dictionary<,>).GetMethod(nameof(Dictionary<object, object>.ContainsKey))!;

    // IDictionary<TKey, TValue>.ContainsKey(TKey key)
    private static readonly MethodInfo IDictionaryContainsKeyMethod =
        typeof(IDictionary<,>).GetMethod(nameof(IDictionary<object, object>.ContainsKey))!;

    // IReadOnlyDictionary<TKey, TValue>.ContainsKey(TKey key)
    private static readonly MethodInfo IReadOnlyDictionaryContainsKeyMethod =
        typeof(IReadOnlyDictionary<,>).GetMethod(nameof(IReadOnlyDictionary<object, object>.ContainsKey))!;

    public ClickHouseMapMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Handle instance methods on dictionaries
        if (instance is not null && IsDictionaryType(method.DeclaringType))
        {
            return TranslateInstanceMethod(instance, method, arguments);
        }

        return null;
    }

    private SqlExpression? TranslateInstanceMethod(
        SqlExpression instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        // ContainsKey(key) → mapContains(map, key)
        if (method.Name == nameof(Dictionary<object, object>.ContainsKey) && arguments.Count == 1)
        {
            return _sqlExpressionFactory.Function(
                "mapContains",
                new[] { instance, arguments[0] },
                nullable: true,
                argumentsPropagateNullability: new[] { true, true },
                typeof(bool));
        }

        // TryGetValue is not directly translatable - would need special handling

        return null;
    }

    /// <summary>
    /// Checks if a type is a dictionary type.
    /// </summary>
    private static bool IsDictionaryType(Type? type)
    {
        if (type is null)
        {
            return false;
        }

        if (type.IsGenericType)
        {
            var genericDef = type.GetGenericTypeDefinition();
            return genericDef == typeof(Dictionary<,>) ||
                   genericDef == typeof(IDictionary<,>) ||
                   genericDef == typeof(IReadOnlyDictionary<,>);
        }

        return false;
    }
}

/// <summary>
/// Translates dictionary member access (Keys, Values, Count) to ClickHouse functions.
/// </summary>
public class ClickHouseMapMemberTranslator : IMemberTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseMapMemberTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
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

        var declaringType = member.DeclaringType;
        if (declaringType is null || !IsDictionaryType(declaringType))
        {
            return null;
        }

        // Keys → mapKeys(map)
        if (member.Name == "Keys")
        {
            return _sqlExpressionFactory.Function(
                "mapKeys",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                returnType);
        }

        // Values → mapValues(map)
        if (member.Name == "Values")
        {
            return _sqlExpressionFactory.Function(
                "mapValues",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                returnType);
        }

        // Count → length(mapKeys(map))
        if (member.Name == "Count")
        {
            var keysExpr = _sqlExpressionFactory.Function(
                "mapKeys",
                new[] { instance },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(object)); // Intermediate type

            return _sqlExpressionFactory.Function(
                "length",
                new[] { keysExpr },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(int));
        }

        return null;
    }

    private static bool IsDictionaryType(Type? type)
    {
        if (type is null)
        {
            return false;
        }

        if (type.IsGenericType)
        {
            var genericDef = type.GetGenericTypeDefinition();
            return genericDef == typeof(Dictionary<,>) ||
                   genericDef == typeof(IDictionary<,>) ||
                   genericDef == typeof(IReadOnlyDictionary<,>) ||
                   genericDef == typeof(ICollection<>); // For KeyValuePair collections
        }

        return false;
    }
}
