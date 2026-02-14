using System.Reflection;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates ClickHouse text search DbFunctions to SQL.
/// </summary>
public class ClickHouseTextSearchMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    /// <summary>
    /// Maps C# method names to ClickHouse SQL function names.
    /// All methods take (DbFunctions, haystack, ...) — the DbFunctions arg is skipped.
    /// </summary>
    private static readonly Dictionary<string, string> FunctionMappings = new()
    {
        // Token functions
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.HasToken)] = "hasToken",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.HasTokenCaseInsensitive)] = "hasTokenCaseInsensitive",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.HasAnyToken)] = "hasAnyToken",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.HasAllTokens)] = "hasAllTokens",

        // Multi-search functions
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchAny)] = "multiSearchAny",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchAnyCaseInsensitive)] = "multiSearchAnyCaseInsensitive",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchAll)] = "multiSearchAllPositions",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchAllCaseInsensitive)] = "multiSearchAllPositionsCaseInsensitive",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchFirstPosition)] = "multiSearchFirstPosition",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchFirstPositionCaseInsensitive)] = "multiSearchFirstPositionCaseInsensitive",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchFirstIndex)] = "multiSearchFirstIndex",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiSearchFirstIndexCaseInsensitive)] = "multiSearchFirstIndexCaseInsensitive",

        // N-gram functions
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramSearch)] = "ngramSearch",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramSearchCaseInsensitive)] = "ngramSearchCaseInsensitive",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramDistance)] = "ngramDistance",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramDistanceCaseInsensitive)] = "ngramDistanceCaseInsensitive",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramSearchUTF8)] = "ngramSearchUTF8",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramSearchCaseInsensitiveUTF8)] = "ngramSearchCaseInsensitiveUTF8",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramDistanceUTF8)] = "ngramDistanceUTF8",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.NgramDistanceCaseInsensitiveUTF8)] = "ngramDistanceCaseInsensitiveUTF8",

        // Subsequence functions
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.HasSubsequence)] = "hasSubsequence",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.HasSubsequenceCaseInsensitive)] = "hasSubsequenceCaseInsensitive",

        // Substring counting
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.CountSubstrings)] = "countSubstrings",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.CountSubstringsCaseInsensitive)] = "countSubstringsCaseInsensitive",

        // Multi-match (regex via Hyperscan)
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiMatchAny)] = "multiMatchAny",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiMatchAnyIndex)] = "multiMatchAnyIndex",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.MultiMatchAllIndices)] = "multiMatchAllIndices",

        // Extract functions
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.ExtractAll)] = "extractAll",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.SplitByNonAlpha)] = "splitByNonAlpha",
    };

    public ClickHouseTextSearchMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(ClickHouseTextSearchDbFunctionsExtensions))
        {
            return null;
        }

        if (!FunctionMappings.TryGetValue(method.Name, out var clickHouseFunction))
        {
            return null;
        }

        // Skip the DbFunctions argument at index 0
        var functionArguments = arguments.Skip(1).ToArray();
        var nullability = Enumerable.Repeat(true, functionArguments.Length).ToArray();

        return _sqlExpressionFactory.Function(
            clickHouseFunction,
            functionArguments,
            nullable: true,
            argumentsPropagateNullability: nullability,
            method.ReturnType);
    }
}
