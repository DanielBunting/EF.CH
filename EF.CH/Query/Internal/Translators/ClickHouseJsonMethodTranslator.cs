using System.Reflection;
using System.Text.Json;
using EF.CH.Extensions;
using EF.CH.Query.Internal.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates ClickHouseJsonFunctions extension methods to ClickHouse SQL expressions.
/// </summary>
/// <remarks>
/// Supported translations:
/// - json.GetPath&lt;T&gt;("path") → "column"."path"."segment"
/// - json.GetPathOrDefault&lt;T&gt;("path", default) → ifNull("column"."path", default)
/// - json.HasPath("path") → "column"."path" IS NOT NULL
/// - json.GetArray&lt;T&gt;("path") → "column"."path"
/// - json.GetObject("path") → "column"."path"
/// </remarks>
public class ClickHouseJsonMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    // GetPath<T>(this JsonElement, string path)
    private static readonly MethodInfo JsonElementGetPathMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.GetPath)
                     && m.GetParameters()[0].ParameterType == typeof(JsonElement));

    // GetPath<T>(this JsonDocument, string path)
    private static readonly MethodInfo JsonDocumentGetPathMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.GetPath)
                     && m.GetParameters()[0].ParameterType == typeof(JsonDocument));

    // GetPathOrDefault<T>(this JsonElement, string path, T default)
    private static readonly MethodInfo JsonElementGetPathOrDefaultMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.GetPathOrDefault)
                     && m.GetParameters()[0].ParameterType == typeof(JsonElement));

    // GetPathOrDefault<T>(this JsonDocument, string path, T default)
    private static readonly MethodInfo JsonDocumentGetPathOrDefaultMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.GetPathOrDefault)
                     && m.GetParameters()[0].ParameterType == typeof(JsonDocument));

    // HasPath(this JsonElement, string path)
    private static readonly MethodInfo JsonElementHasPathMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.HasPath)
                     && m.GetParameters()[0].ParameterType == typeof(JsonElement));

    // HasPath(this JsonDocument, string path)
    private static readonly MethodInfo JsonDocumentHasPathMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.HasPath)
                     && m.GetParameters()[0].ParameterType == typeof(JsonDocument));

    // GetArray<T>(this JsonElement, string path)
    private static readonly MethodInfo JsonElementGetArrayMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.GetArray)
                     && m.GetParameters()[0].ParameterType == typeof(JsonElement));

    // GetArray<T>(this JsonDocument, string path)
    private static readonly MethodInfo JsonDocumentGetArrayMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.GetArray)
                     && m.GetParameters()[0].ParameterType == typeof(JsonDocument));

    // GetObject(this JsonElement, string path)
    private static readonly MethodInfo JsonElementGetObjectMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.GetObject)
                     && m.GetParameters()[0].ParameterType == typeof(JsonElement));

    // GetObject(this JsonDocument, string path)
    private static readonly MethodInfo JsonDocumentGetObjectMethod =
        typeof(ClickHouseJsonFunctions).GetMethods()
            .First(m => m.Name == nameof(ClickHouseJsonFunctions.GetObject)
                     && m.GetParameters()[0].ParameterType == typeof(JsonDocument));

    public ClickHouseJsonMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Only handle ClickHouseJsonFunctions methods
        if (method.DeclaringType != typeof(ClickHouseJsonFunctions))
        {
            return null;
        }

        // GetPath<T>(this JsonElement/JsonDocument json, string path)
        if (IsMethod(method, JsonElementGetPathMethod) || IsMethod(method, JsonDocumentGetPathMethod))
        {
            return TranslateGetPath(arguments, method.GetGenericArguments()[0]);
        }

        // GetPathOrDefault<T>(this JsonElement/JsonDocument json, string path, T default)
        if (IsMethod(method, JsonElementGetPathOrDefaultMethod) || IsMethod(method, JsonDocumentGetPathOrDefaultMethod))
        {
            return TranslateGetPathOrDefault(arguments, method.GetGenericArguments()[0]);
        }

        // HasPath(this JsonElement/JsonDocument json, string path)
        if (method == JsonElementHasPathMethod || method == JsonDocumentHasPathMethod)
        {
            return TranslateHasPath(arguments);
        }

        // GetArray<T>(this JsonElement/JsonDocument json, string path)
        if (IsMethod(method, JsonElementGetArrayMethod) || IsMethod(method, JsonDocumentGetArrayMethod))
        {
            var elementType = method.GetGenericArguments()[0];
            return TranslateGetPath(arguments, elementType.MakeArrayType());
        }

        // GetObject(this JsonElement/JsonDocument json, string path)
        if (method == JsonElementGetObjectMethod || method == JsonDocumentGetObjectMethod)
        {
            return TranslateGetPath(arguments, typeof(JsonElement));
        }

        return null;
    }

    private static bool IsMethod(MethodInfo method, MethodInfo targetMethod)
    {
        // For generic methods, compare the generic definition
        if (method.IsGenericMethod && targetMethod.IsGenericMethod)
        {
            return method.GetGenericMethodDefinition() == targetMethod.GetGenericMethodDefinition()
                   || method.GetGenericMethodDefinition() == targetMethod;
        }
        return method == targetMethod;
    }

    /// <summary>
    /// Translates GetPath&lt;T&gt;(json, path) to a JSON path expression.
    /// </summary>
    private SqlExpression? TranslateGetPath(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        if (arguments.Count < 2)
            return null;

        var jsonColumn = arguments[0];
        var pathArg = arguments[1];

        // Path must be a constant string
        if (pathArg is not SqlConstantExpression { Value: string path })
        {
            // Non-constant paths are not supported
            return null;
        }

        // Build the JSON path expression
        return ClickHouseJsonPathExpression.Create(
            jsonColumn,
            path,
            returnType,
            _sqlExpressionFactory.TypeMappingSource.FindMapping(returnType));
    }

    /// <summary>
    /// Translates GetPathOrDefault&lt;T&gt;(json, path, default) to ifNull(path_expr, default).
    /// </summary>
    private SqlExpression? TranslateGetPathOrDefault(IReadOnlyList<SqlExpression> arguments, Type returnType)
    {
        if (arguments.Count < 3)
            return null;

        var pathExpr = TranslateGetPath(arguments.Take(2).ToList(), returnType);
        if (pathExpr == null)
            return null;

        var defaultValue = arguments[2];

        // Generate: ifNull(path_expr, default)
        return _sqlExpressionFactory.Function(
            "ifNull",
            new[] { pathExpr, defaultValue },
            nullable: true,
            argumentsPropagateNullability: new[] { true, true },
            returnType,
            _sqlExpressionFactory.TypeMappingSource.FindMapping(returnType));
    }

    /// <summary>
    /// Translates HasPath(json, path) to path_expr IS NOT NULL.
    /// </summary>
    private SqlExpression? TranslateHasPath(IReadOnlyList<SqlExpression> arguments)
    {
        if (arguments.Count < 2)
            return null;

        // Get the path expression with string type for the existence check
        // (we just need IS NOT NULL, the actual type doesn't matter for the check)
        var pathExpr = TranslateGetPath(arguments, typeof(string));
        if (pathExpr == null)
            return null;

        // Generate: path_expr IS NOT NULL
        return _sqlExpressionFactory.IsNotNull(pathExpr);
    }
}
