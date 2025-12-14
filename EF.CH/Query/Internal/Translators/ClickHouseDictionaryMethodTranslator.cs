using System.Linq.Expressions;
using System.Reflection;
using EF.CH.Dictionaries;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates ClickHouseDictionary methods to ClickHouse dictGet functions.
/// </summary>
/// <remarks>
/// Supported translations:
/// - dictionary.Get(key, c => c.Attr) → dictGet('name', 'Attr', key)
/// - dictionary.GetOrDefault(key, c => c.Attr, default) → dictGetOrDefault('name', 'Attr', key, default)
/// - dictionary.ContainsKey(key) → dictHas('name', key)
/// </remarks>
public class ClickHouseDictionaryMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    public ClickHouseDictionaryMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
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

        // Check if this is a ClickHouseDictionary<T,K> method
        if (declaringType is null || !declaringType.IsGenericType)
            return null;

        var genericDef = declaringType.GetGenericTypeDefinition();
        if (genericDef != typeof(ClickHouseDictionary<,>))
            return null;

        // Get the dictionary type (TDictionary) and key type (TKey)
        var typeArgs = declaringType.GetGenericArguments();
        var dictionaryType = typeArgs[0];
        // var keyType = typeArgs[1];  // Available if needed

        // Derive dictionary name from entity type (convert to snake_case)
        var dictionaryName = ConvertToSnakeCase(dictionaryType.Name);

        return method.Name switch
        {
            "Get" => TranslateGet(dictionaryName, method, arguments),
            "GetOrDefault" => TranslateGetOrDefault(dictionaryName, method, arguments),
            "ContainsKey" => TranslateContainsKey(dictionaryName, arguments),
            _ => null
        };
    }

    private SqlExpression? TranslateGet(
        string dictionaryName,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        // Get<TAttribute>(key, c => c.Attr)
        // arguments[0] = key
        // arguments[1] = attribute selector lambda (not passed as SqlExpression)

        if (arguments.Count < 1)
            return null;

        var keyExpression = arguments[0];

        // Get the attribute name from the method's generic type argument
        // The attribute selector is captured as part of the expression tree,
        // but we need to extract it from the original call

        // For now, we'll look at the method's second parameter type to get attribute info
        // In a complete implementation, we'd need access to the original lambda expression

        // The return type of Get<TAttribute> tells us the attribute type
        var returnType = method.ReturnType;

        // We'll need to extract the attribute name from the expression tree
        // This requires hooking into the expression visitor earlier
        // For now, use a placeholder that we'll enhance

        // Note: In EF Core's expression pipeline, the lambda for attribute selection
        // is typically captured during the expression tree analysis phase.
        // We need to access it through a different mechanism.

        // Simplified approach: Extract attribute name from a marker in the SQL expression
        // This will be enhanced when we hook into the query compilation pipeline

        // Check if the second argument contains property information
        string? attributeName = null;
        if (arguments.Count >= 2)
        {
            attributeName = ExtractPropertyNameFromExpression(arguments[1]);
        }

        attributeName ??= "Value"; // Default fallback

        // dictGet('dictionary_name', 'AttributeName', key)
        return _sqlExpressionFactory.Function(
            "dictGet",
            new SqlExpression[]
            {
                _sqlExpressionFactory.Constant(dictionaryName),
                _sqlExpressionFactory.Constant(attributeName),
                keyExpression
            },
            nullable: true,
            argumentsPropagateNullability: new[] { false, false, true },
            returnType);
    }

    private SqlExpression? TranslateGetOrDefault(
        string dictionaryName,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        // GetOrDefault<TAttribute>(key, c => c.Attr, defaultValue)
        // arguments[0] = key
        // arguments[1] = attribute selector or default value
        // arguments[2] = default value (if 3 args)

        if (arguments.Count < 2)
            return null;

        var keyExpression = arguments[0];
        var returnType = method.ReturnType;

        // Extract attribute name and default value
        string? attributeName = null;
        SqlExpression defaultValue;

        if (arguments.Count >= 3)
        {
            attributeName = ExtractPropertyNameFromExpression(arguments[1]);
            defaultValue = arguments[2];
        }
        else
        {
            // With 2 arguments, second is the default value
            defaultValue = arguments[1];
        }

        attributeName ??= "Value";

        // dictGetOrDefault('dictionary_name', 'AttributeName', key, default)
        return _sqlExpressionFactory.Function(
            "dictGetOrDefault",
            new SqlExpression[]
            {
                _sqlExpressionFactory.Constant(dictionaryName),
                _sqlExpressionFactory.Constant(attributeName),
                keyExpression,
                defaultValue
            },
            nullable: true,
            argumentsPropagateNullability: new[] { false, false, true, true },
            returnType);
    }

    private SqlExpression? TranslateContainsKey(
        string dictionaryName,
        IReadOnlyList<SqlExpression> arguments)
    {
        // ContainsKey(key)
        if (arguments.Count < 1)
            return null;

        var keyExpression = arguments[0];

        // dictHas('dictionary_name', key)
        return _sqlExpressionFactory.Function(
            "dictHas",
            new SqlExpression[]
            {
                _sqlExpressionFactory.Constant(dictionaryName),
                keyExpression
            },
            nullable: true,
            argumentsPropagateNullability: new[] { false, true },
            typeof(bool));
    }

    /// <summary>
    /// Attempts to extract a property name from a SQL expression.
    /// </summary>
    private static string? ExtractPropertyNameFromExpression(SqlExpression expression)
    {
        // This handles cases where the expression contains property information
        // Common cases include:
        // - SqlConstantExpression with the property name as a string
        // - SqlFragmentExpression with property info

        if (expression is SqlConstantExpression constant && constant.Value is string s)
        {
            return s;
        }

        // For ColumnExpression, extract the column name
        if (expression is ColumnExpression column)
        {
            return column.Name;
        }

        return null;
    }

    private static string ConvertToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var result = new System.Text.StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                    result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }
}
