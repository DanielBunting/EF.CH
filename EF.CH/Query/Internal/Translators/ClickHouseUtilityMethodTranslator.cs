using System.Linq.Expressions;
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
        typeof(ClickHouseKeeperDbFunctionsExtensions),
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
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfSecond))] = "toStartOfSecond",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfTenMinutes))] = "toStartOfTenMinutes",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToUnixTimestamp))] = "toUnixTimestamp",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.FromUnixTimestamp))] = "fromUnixTimestamp",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.FromUnixTimestamp64Milli))] = "fromUnixTimestamp64Milli",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToRelativeYearNum))] = "toRelativeYearNum",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToRelativeMonthNum))] = "toRelativeMonthNum",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToRelativeWeekNum))] = "toRelativeWeekNum",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToRelativeDayNum))] = "toRelativeDayNum",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToRelativeHourNum))] = "toRelativeHourNum",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToRelativeMinuteNum))] = "toRelativeMinuteNum",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToRelativeSecondNum))] = "toRelativeSecondNum",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.DateAdd))] = "date_add",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.DateSub))] = "date_sub",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.Age))] = "age",

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

        // Keeper-backed scalar functions
        [(typeof(ClickHouseKeeperDbFunctionsExtensions), nameof(ClickHouseKeeperDbFunctionsExtensions.GenerateSerialID))] = "generateSerialID",
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

        // DateAdd / DateSub: ClickHouse 24+ no longer accepts the dynamic-unit
        // form `date_add(unit, n, dt)` — the parser rewrites it to `plus(...)`
        // which only takes 2 args. Emit `dt ± toIntervalUnit(n)` instead.
        // The unit arg is at slot 0 by method signature, so extract it
        // unconditionally (irrespective of how EF wrapped it in CAST/Convert).
        if (method.DeclaringType == typeof(ClickHouseDateTruncDbFunctionsExtensions)
            && (method.Name == nameof(ClickHouseDateTruncDbFunctionsExtensions.DateAdd)
             || method.Name == nameof(ClickHouseDateTruncDbFunctionsExtensions.DateSub))
            && functionArguments.Length == 3
            && TryExtractKnownIntervalUnit(functionArguments[0], out var unitArg))
        {
            var op = method.Name == nameof(ClickHouseDateTruncDbFunctionsExtensions.DateAdd) ? "+" : "-";
            var intervalFn = "toInterval" + char.ToUpperInvariant(unitArg.ToString()[0]) + unitArg.ToString().Substring(1);
            var interval = _sqlExpressionFactory.Function(
                intervalFn,
                new[] { functionArguments[1] },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(object));
            return _sqlExpressionFactory.MakeBinary(
                op == "+" ? ExpressionType.Add : ExpressionType.Subtract,
                functionArguments[2],
                interval,
                typeMapping: null)!;
        }

        // Convert ClickHouseIntervalUnit enum args to lowercase string constants for SQL
        if (method.DeclaringType == typeof(ClickHouseDateTruncDbFunctionsExtensions))
        {
            functionArguments = ConvertIntervalUnitArgs(functionArguments);
        }

        if (!FunctionMappings.TryGetValue((method.DeclaringType, method.Name), out var clickHouseFunction))
        {
            return null;
        }

        var call = _sqlExpressionFactory.Function(
            clickHouseFunction,
            functionArguments,
            nullable: true,
            argumentsPropagateNullability: nullability,
            method.ReturnType);

        // Some ClickHouse functions return a type that doesn't materialise to the
        // C# method's declared return type — toUnixTimestamp/toRelativeMonthNum
        // return UInt32/Int32 but the C# methods are declared as long;
        // yandexConsistentHash returns UInt16 for ≤256 buckets but the C# method
        // is uint. Wrap with the matching CH cast so the row reader sees the
        // expected width and signedness.
        if (NarrowingFunctions.Contains(clickHouseFunction))
        {
            return _sqlExpressionFactory.WrapWithClrCast(call, method.ReturnType);
        }

        return call;
    }

    /// <summary>
    /// ClickHouse functions known to return a type narrower or differently-signed
    /// than the C# method signature declares. The wrapper cast chosen matches
    /// <c>method.ReturnType</c>.
    /// </summary>
    private static readonly HashSet<string> NarrowingFunctions = new(StringComparer.Ordinal)
    {
        "toUnixTimestamp",
        "toRelativeYearNum",
        "toRelativeMonthNum",
        "toRelativeWeekNum",
        "toRelativeDayNum",
        "toRelativeHourNum",
        "toRelativeMinuteNum",
        "toRelativeSecondNum",
        "yandexConsistentHash",
    };

    private SqlExpression[] ConvertIntervalUnitArgs(SqlExpression[] args)
    {
        for (var i = 0; i < args.Length; i++)
        {
            if (TryExtractIntervalUnit(args[i], out var unit))
            {
                args[i] = _sqlExpressionFactory.Constant(unit.ToString().ToLowerInvariant());
            }
        }

        return args;
    }

    /// <summary>
    /// Recognises a ClickHouseIntervalUnit value through the various shapes
    /// EF Core may wrap it in: a plain SqlConstantExpression, an int constant
    /// (when the enum was flattened by the SQL pipeline), or a Convert/CAST
    /// wrapping either of the above (the closure-captured Theory parameter case).
    /// </summary>
    /// <summary>
    /// Like <see cref="TryExtractIntervalUnit"/> but skips the unit-typing
    /// guard, intended for slots where the method signature guarantees the
    /// arg is a unit (e.g. DateAdd's first user arg).
    /// </summary>
    private static bool TryExtractKnownIntervalUnit(SqlExpression expr, out ClickHouseIntervalUnit unit)
    {
        unit = default;
        var unwrap = expr;
        // Unwrap SqlUnaryExpression(Convert) layers and any SQL "_CAST" function
        // wrapping (EF Core uses both depending on whether the cast came from
        // an explicit Expression.Convert vs an enum-to-int promotion).
        while (true)
        {
            if (unwrap is SqlUnaryExpression { OperatorType: ExpressionType.Convert, Operand: var inner })
            {
                unwrap = inner;
                continue;
            }
            if (unwrap is SqlFunctionExpression { Name: "_CAST" or "CAST", Arguments: { Count: >= 1 } argv })
            {
                unwrap = argv[0];
                continue;
            }
            break;
        }
        if (unwrap is SqlConstantExpression { Value: ClickHouseIntervalUnit u })
        {
            unit = u;
            return true;
        }
        if (unwrap is SqlConstantExpression { Value: int i }
            && Enum.IsDefined(typeof(ClickHouseIntervalUnit), i))
        {
            unit = (ClickHouseIntervalUnit)i;
            return true;
        }
        return false;
    }

    private static bool TryExtractIntervalUnit(SqlExpression expr, out ClickHouseIntervalUnit unit)
    {
        unit = default;
        // Only consider expressions whose declared CLR type is ClickHouseIntervalUnit
        // (or its underlying int after Convert) — otherwise the literal `1` argument
        // to DateAdd(Day, 1, dt) would be misread as the unit Minute (enum value 1).
        var unwrap = expr;
        var isUnitTyped = expr.Type == typeof(ClickHouseIntervalUnit) || expr.Type == typeof(ClickHouseIntervalUnit?);
        while (unwrap is SqlUnaryExpression { OperatorType: ExpressionType.Convert, Operand: var inner })
        {
            if (unwrap.Type == typeof(ClickHouseIntervalUnit) || unwrap.Type == typeof(ClickHouseIntervalUnit?))
                isUnitTyped = true;
            unwrap = inner;
        }
        if (unwrap is SqlConstantExpression { Value: ClickHouseIntervalUnit u })
        {
            unit = u;
            return true;
        }
        if (isUnitTyped
            && unwrap is SqlConstantExpression { Value: int i }
            && Enum.IsDefined(typeof(ClickHouseIntervalUnit), i))
        {
            unit = (ClickHouseIntervalUnit)i;
            return true;
        }
        return false;
    }
}
