using System.Reflection;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EF.CH.Query.Internal.Translators;

/// <summary>
/// Translates ClickHouse utility DbFunctions to SQL (null, string distance, URL, hash, format,
/// date truncation, IP, encoding, type checking, string split, UUID).
/// </summary>
public class ClickHouseUtilityMethodTranslator : IMethodCallTranslator
{
    private readonly ClickHouseSqlExpressionFactory _sqlExpressionFactory;

    private static readonly HashSet<Type> SupportedTypes =
    [
        typeof(ClickHouseNullDbFunctionsExtensions),
        typeof(ClickHouseStringDistanceDbFunctionsExtensions),
        typeof(ClickHouseUrlDbFunctionsExtensions),
        typeof(ClickHouseHashDbFunctionsExtensions),
        typeof(ClickHouseFormatDbFunctionsExtensions),
        typeof(ClickHouseDateTruncDbFunctionsExtensions),
        typeof(ClickHouseIpDbFunctionsExtensions),
        typeof(ClickHouseEncodingDbFunctionsExtensions),
        typeof(ClickHouseTypeCheckDbFunctionsExtensions),
        typeof(ClickHouseStringSplitDbFunctionsExtensions),
        typeof(ClickHouseUuidDbFunctionsExtensions),
    ];

    /// <summary>
    /// Maps (DeclaringType, MethodName) to ClickHouse SQL function name.
    /// </summary>
    private static readonly Dictionary<(Type, string), string> FunctionMappings = new()
    {
        // Null functions
        [(typeof(ClickHouseNullDbFunctionsExtensions), nameof(ClickHouseNullDbFunctionsExtensions.IfNull))] = "ifNull",
        [(typeof(ClickHouseNullDbFunctionsExtensions), nameof(ClickHouseNullDbFunctionsExtensions.NullIf))] = "nullIf",
        [(typeof(ClickHouseNullDbFunctionsExtensions), nameof(ClickHouseNullDbFunctionsExtensions.AssumeNotNull))] = "assumeNotNull",
        [(typeof(ClickHouseNullDbFunctionsExtensions), nameof(ClickHouseNullDbFunctionsExtensions.Coalesce))] = "coalesce",
        [(typeof(ClickHouseNullDbFunctionsExtensions), nameof(ClickHouseNullDbFunctionsExtensions.IsNull))] = "isNull",
        [(typeof(ClickHouseNullDbFunctionsExtensions), nameof(ClickHouseNullDbFunctionsExtensions.IsNotNull))] = "isNotNull",

        // String distance functions
        [(typeof(ClickHouseStringDistanceDbFunctionsExtensions), nameof(ClickHouseStringDistanceDbFunctionsExtensions.LevenshteinDistance))] = "levenshteinDistance",
        [(typeof(ClickHouseStringDistanceDbFunctionsExtensions), nameof(ClickHouseStringDistanceDbFunctionsExtensions.LevenshteinDistanceUTF8))] = "levenshteinDistanceUTF8",
        [(typeof(ClickHouseStringDistanceDbFunctionsExtensions), nameof(ClickHouseStringDistanceDbFunctionsExtensions.DamerauLevenshteinDistance))] = "damerauLevenshteinDistance",
        [(typeof(ClickHouseStringDistanceDbFunctionsExtensions), nameof(ClickHouseStringDistanceDbFunctionsExtensions.JaroSimilarity))] = "jaroSimilarity",
        [(typeof(ClickHouseStringDistanceDbFunctionsExtensions), nameof(ClickHouseStringDistanceDbFunctionsExtensions.JaroWinklerSimilarity))] = "jaroWinklerSimilarity",

        // URL functions
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.Domain))] = "domain",
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.DomainWithoutWWW))] = "domainWithoutWWW",
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.TopLevelDomain))] = "topLevelDomain",
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.Protocol))] = "protocol",
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.UrlPath))] = "path",
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.ExtractURLParameter))] = "extractURLParameter",
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.ExtractURLParameters))] = "extractURLParameters",
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.CutURLParameter))] = "cutURLParameter",
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.DecodeURLComponent))] = "decodeURLComponent",
        [(typeof(ClickHouseUrlDbFunctionsExtensions), nameof(ClickHouseUrlDbFunctionsExtensions.EncodeURLComponent))] = "encodeURLComponent",

        // Hash functions
        [(typeof(ClickHouseHashDbFunctionsExtensions), nameof(ClickHouseHashDbFunctionsExtensions.CityHash64))] = "cityHash64",
        [(typeof(ClickHouseHashDbFunctionsExtensions), nameof(ClickHouseHashDbFunctionsExtensions.SipHash64))] = "sipHash64",
        [(typeof(ClickHouseHashDbFunctionsExtensions), nameof(ClickHouseHashDbFunctionsExtensions.XxHash64))] = "xxHash64",
        [(typeof(ClickHouseHashDbFunctionsExtensions), nameof(ClickHouseHashDbFunctionsExtensions.MurmurHash3_64))] = "murmurHash3_64",
        [(typeof(ClickHouseHashDbFunctionsExtensions), nameof(ClickHouseHashDbFunctionsExtensions.FarmHash64))] = "farmHash64",
        [(typeof(ClickHouseHashDbFunctionsExtensions), nameof(ClickHouseHashDbFunctionsExtensions.ConsistentHash))] = "yandexConsistentHash",

        // Format functions
        [(typeof(ClickHouseFormatDbFunctionsExtensions), nameof(ClickHouseFormatDbFunctionsExtensions.FormatDateTime))] = "formatDateTime",
        [(typeof(ClickHouseFormatDbFunctionsExtensions), nameof(ClickHouseFormatDbFunctionsExtensions.FormatReadableSize))] = "formatReadableSize",
        [(typeof(ClickHouseFormatDbFunctionsExtensions), nameof(ClickHouseFormatDbFunctionsExtensions.FormatReadableDecimalSize))] = "formatReadableDecimalSize",
        [(typeof(ClickHouseFormatDbFunctionsExtensions), nameof(ClickHouseFormatDbFunctionsExtensions.FormatReadableQuantity))] = "formatReadableQuantity",
        [(typeof(ClickHouseFormatDbFunctionsExtensions), nameof(ClickHouseFormatDbFunctionsExtensions.FormatReadableTimeDelta))] = "formatReadableTimeDelta",
        [(typeof(ClickHouseFormatDbFunctionsExtensions), nameof(ClickHouseFormatDbFunctionsExtensions.ParseDateTime))] = "parseDateTime",

        // Date truncation functions
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfYear))] = "toStartOfYear",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfQuarter))] = "toStartOfQuarter",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfMonth))] = "toStartOfMonth",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfWeek))] = "toStartOfWeek",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToMonday))] = "toMonday",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfDay))] = "toStartOfDay",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfHour))] = "toStartOfHour",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfMinute))] = "toStartOfMinute",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfFiveMinutes))] = "toStartOfFiveMinutes",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfFifteenMinutes))] = "toStartOfFifteenMinutes",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.DateDiff))] = "dateDiff",

        // IP address functions
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IPv4NumToString))] = "IPv4NumToString",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IPv4StringToNum))] = "IPv4StringToNum",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IsIPAddressInRange))] = "isIPAddressInRange",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IsIPv4String))] = "isIPv4String",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IsIPv6String))] = "isIPv6String",

        // Encoding functions
        [(typeof(ClickHouseEncodingDbFunctionsExtensions), nameof(ClickHouseEncodingDbFunctionsExtensions.Base64Encode))] = "base64Encode",
        [(typeof(ClickHouseEncodingDbFunctionsExtensions), nameof(ClickHouseEncodingDbFunctionsExtensions.Base64Decode))] = "base64Decode",
        [(typeof(ClickHouseEncodingDbFunctionsExtensions), nameof(ClickHouseEncodingDbFunctionsExtensions.Hex))] = "hex",
        [(typeof(ClickHouseEncodingDbFunctionsExtensions), nameof(ClickHouseEncodingDbFunctionsExtensions.Unhex))] = "unhex",

        // Type checking functions
        [(typeof(ClickHouseTypeCheckDbFunctionsExtensions), nameof(ClickHouseTypeCheckDbFunctionsExtensions.IsNaN))] = "isNaN",
        [(typeof(ClickHouseTypeCheckDbFunctionsExtensions), nameof(ClickHouseTypeCheckDbFunctionsExtensions.IsFinite))] = "isFinite",
        [(typeof(ClickHouseTypeCheckDbFunctionsExtensions), nameof(ClickHouseTypeCheckDbFunctionsExtensions.IsInfinite))] = "isInfinite",

        // String split/join functions
        [(typeof(ClickHouseStringSplitDbFunctionsExtensions), nameof(ClickHouseStringSplitDbFunctionsExtensions.SplitByChar))] = "splitByChar",
        [(typeof(ClickHouseStringSplitDbFunctionsExtensions), nameof(ClickHouseStringSplitDbFunctionsExtensions.SplitByString))] = "splitByString",
        [(typeof(ClickHouseStringSplitDbFunctionsExtensions), nameof(ClickHouseStringSplitDbFunctionsExtensions.ArrayStringConcat))] = "arrayStringConcat",

        // UUID functions
        [(typeof(ClickHouseUuidDbFunctionsExtensions), nameof(ClickHouseUuidDbFunctionsExtensions.NewGuidV7))] = "generateUUIDv7",
    };

    // Md5 and Sha256 need special nested handling: hex(MD5(x)), hex(SHA256(x))
    private static readonly Dictionary<string, string> NestedHexFunctions = new()
    {
        [nameof(ClickHouseHashDbFunctionsExtensions.Md5)] = "MD5",
        [nameof(ClickHouseHashDbFunctionsExtensions.Sha256)] = "SHA256",
    };

    public ClickHouseUtilityMethodTranslator(ClickHouseSqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType is null || !SupportedTypes.Contains(method.DeclaringType))
        {
            return null;
        }

        // Skip the DbFunctions argument at index 0
        var functionArguments = arguments.Skip(1).ToArray();
        var nullability = Enumerable.Repeat(true, functionArguments.Length).ToArray();

        // Special case: Md5/Sha256 → hex(MD5(x)) / hex(SHA256(x))
        if (method.DeclaringType == typeof(ClickHouseHashDbFunctionsExtensions)
            && NestedHexFunctions.TryGetValue(method.Name, out var innerFunctionName))
        {
            var innerCall = _sqlExpressionFactory.Function(
                innerFunctionName,
                functionArguments,
                nullable: true,
                argumentsPropagateNullability: nullability,
                typeof(byte[]));

            return _sqlExpressionFactory.Function(
                "hex",
                new[] { innerCall },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(string));
        }

        if (!FunctionMappings.TryGetValue((method.DeclaringType, method.Name), out var clickHouseFunction))
        {
            return null;
        }

        return _sqlExpressionFactory.Function(
            clickHouseFunction,
            functionArguments,
            nullable: true,
            argumentsPropagateNullability: nullability,
            method.ReturnType);
    }
}
