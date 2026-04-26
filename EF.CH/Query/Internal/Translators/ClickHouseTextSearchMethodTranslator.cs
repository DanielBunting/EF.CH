using System.Reflection;
using EF.CH.Extensions;
using EF.CH.Query.Internal.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates ClickHouse text search DbFunctions to SQL.
/// </summary>
public class ClickHouseTextSearchMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    /// <summary>
    /// Maps C# method names to ClickHouse SQL function names.
    /// All methods take (DbFunctions, haystack, ...) — the DbFunctions arg is skipped.
    /// </summary>
    private static readonly Dictionary<string, string> FunctionMappings = new()
    {
        // Token functions (HasAnyToken / HasAllTokens are special-cased — ClickHouse 25.6
        // dropped them, so they're emitted as arrayExists / arrayAll over hasToken).
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.HasToken)] = "hasToken",
        [nameof(ClickHouseTextSearchDbFunctionsExtensions.HasTokenCaseInsensitive)] = "hasTokenCaseInsensitive",

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

    public ClickHouseTextSearchMethodTranslator(
        ClickHouseSqlExpressionFactory sqlExpressionFactory,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
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

        if (method.Name == nameof(ClickHouseTextSearchDbFunctionsExtensions.HasAnyToken))
        {
            return TranslateTokenSetMembership("hasAny", arguments);
        }

        if (method.Name == nameof(ClickHouseTextSearchDbFunctionsExtensions.HasAllTokens))
        {
            return TranslateTokenSetMembership("hasAll", arguments);
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

    /// <summary>
    /// Replacement for ClickHouse's dropped <c>hasAnyToken</c> / <c>hasAllTokens</c>:
    /// emits <c>hasAny(splitByNonAlpha(haystack), tokens)</c> / <c>hasAll(...)</c>.
    /// <c>splitByNonAlpha</c> produces the same token boundaries the dropped functions used,
    /// and <c>hasAny</c> / <c>hasAll</c> accept runtime token arrays — unlike <c>hasToken</c>,
    /// whose second argument must be a parse-time constant.
    /// </summary>
    private SqlExpression TranslateTokenSetMembership(string higherOrderFn, IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] is the DbFunctions instance (ignored), [1] is haystack, [2] is tokens[].
        var haystack = arguments[1];
        var tokens = arguments[2];

        var stringArrayMapping = _typeMappingSource.FindMapping(typeof(string[]));
        var boolMapping = _typeMappingSource.FindMapping(typeof(bool));

        var split = _sqlExpressionFactory.Function(
            "splitByNonAlpha",
            new[] { haystack },
            nullable: true,
            argumentsPropagateNullability: new[] { true },
            typeof(string[]),
            stringArrayMapping);

        return _sqlExpressionFactory.Function(
            higherOrderFn,
            new SqlExpression[] { split, tokens },
            nullable: true,
            argumentsPropagateNullability: new[] { true, true },
            typeof(bool),
            boolMapping);
    }
}
