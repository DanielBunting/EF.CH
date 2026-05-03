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
        typeof(ClickHouseConditionalDbFunctionsExtensions),
        typeof(ClickHouseStringPatternDbFunctionsExtensions),
        typeof(ClickHouseSafeCastDbFunctionsExtensions),
        typeof(ClickHouseBitDbFunctionsExtensions),
        typeof(ClickHouseJsonExtractDbFunctionsExtensions),
        typeof(ClickHouseRandomDbFunctionsExtensions),
        typeof(ClickHouseServerDbFunctionsExtensions),
        typeof(ClickHouseTupleDbFunctionsExtensions),
        typeof(ClickHouseStringExtraDbFunctionsExtensions),
        typeof(ClickHouseMathDbFunctionsExtensions),
        typeof(ClickHouseArrayDbFunctionsExtensions),
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
        [(typeof(ClickHouseFormatDbFunctionsExtensions), nameof(ClickHouseFormatDbFunctionsExtensions.FormatRow))] = "formatRow",
        [(typeof(ClickHouseFormatDbFunctionsExtensions), nameof(ClickHouseFormatDbFunctionsExtensions.FormatString))] = "format",

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
        [(typeof(ClickHouseStringSplitDbFunctionsExtensions), nameof(ClickHouseStringSplitDbFunctionsExtensions.SplitByRegexp))] = "splitByRegexp",
        [(typeof(ClickHouseStringSplitDbFunctionsExtensions), nameof(ClickHouseStringSplitDbFunctionsExtensions.AlphaTokens))] = "alphaTokens",

        // UUID functions
        [(typeof(ClickHouseUuidDbFunctionsExtensions), nameof(ClickHouseUuidDbFunctionsExtensions.NewGuidV7))] = "generateUUIDv7",
        [(typeof(ClickHouseUuidDbFunctionsExtensions), nameof(ClickHouseUuidDbFunctionsExtensions.NewGuidV4))] = "generateUUIDv4",
        [(typeof(ClickHouseUuidDbFunctionsExtensions), nameof(ClickHouseUuidDbFunctionsExtensions.UUIDStringToNum))] = "UUIDStringToNum",
        [(typeof(ClickHouseUuidDbFunctionsExtensions), nameof(ClickHouseUuidDbFunctionsExtensions.UUIDNumToString))] = "UUIDNumToString",
        [(typeof(ClickHouseUuidDbFunctionsExtensions), nameof(ClickHouseUuidDbFunctionsExtensions.ToUUIDOrNull))] = "toUUIDOrNull",

        // Keeper-backed scalar functions
        [(typeof(ClickHouseKeeperDbFunctionsExtensions), nameof(ClickHouseKeeperDbFunctionsExtensions.GenerateSerialID))] = "generateSerialID",
        [(typeof(ClickHouseKeeperDbFunctionsExtensions), nameof(ClickHouseKeeperDbFunctionsExtensions.GetMacro))] = "getMacro",

        // Conditional functions (Tier 1a)
        [(typeof(ClickHouseConditionalDbFunctionsExtensions), nameof(ClickHouseConditionalDbFunctionsExtensions.MultiIf))] = "multiIf",

        // String pattern matching (Tier 1b)
        [(typeof(ClickHouseStringPatternDbFunctionsExtensions), nameof(ClickHouseStringPatternDbFunctionsExtensions.ILike))] = "ilike",
        [(typeof(ClickHouseStringPatternDbFunctionsExtensions), nameof(ClickHouseStringPatternDbFunctionsExtensions.NotLike))] = "notLike",
        [(typeof(ClickHouseStringPatternDbFunctionsExtensions), nameof(ClickHouseStringPatternDbFunctionsExtensions.NotILike))] = "notILike",
        [(typeof(ClickHouseStringPatternDbFunctionsExtensions), nameof(ClickHouseStringPatternDbFunctionsExtensions.Match))] = "match",
        [(typeof(ClickHouseStringPatternDbFunctionsExtensions), nameof(ClickHouseStringPatternDbFunctionsExtensions.ReplaceRegex))] = "replaceRegexpOne",
        [(typeof(ClickHouseStringPatternDbFunctionsExtensions), nameof(ClickHouseStringPatternDbFunctionsExtensions.ReplaceRegexAll))] = "replaceRegexpAll",
        [(typeof(ClickHouseStringPatternDbFunctionsExtensions), nameof(ClickHouseStringPatternDbFunctionsExtensions.MatchAll))] = "extractAllGroupsHorizontal",
        [(typeof(ClickHouseStringPatternDbFunctionsExtensions), nameof(ClickHouseStringPatternDbFunctionsExtensions.Position))] = "position",
        [(typeof(ClickHouseStringPatternDbFunctionsExtensions), nameof(ClickHouseStringPatternDbFunctionsExtensions.PositionCaseInsensitive))] = "positionCaseInsensitiveUTF8",

        // Safe type conversion (Tier 1c)
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToInt8OrNull))] = "toInt8OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToInt16OrNull))] = "toInt16OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToInt32OrNull))] = "toInt32OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToInt64OrNull))] = "toInt64OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToUInt8OrNull))] = "toUInt8OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToUInt16OrNull))] = "toUInt16OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToUInt32OrNull))] = "toUInt32OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToUInt64OrNull))] = "toUInt64OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToInt8OrZero))] = "toInt8OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToInt16OrZero))] = "toInt16OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToInt32OrZero))] = "toInt32OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToInt64OrZero))] = "toInt64OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToUInt8OrZero))] = "toUInt8OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToUInt16OrZero))] = "toUInt16OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToUInt32OrZero))] = "toUInt32OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToUInt64OrZero))] = "toUInt64OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToFloat32OrNull))] = "toFloat32OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToFloat64OrNull))] = "toFloat64OrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToFloat32OrZero))] = "toFloat32OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToFloat64OrZero))] = "toFloat64OrZero",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToDateOrNull))] = "toDateOrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ToDateTimeOrNull))] = "toDateTimeOrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ParseDateTimeBestEffort))] = "parseDateTimeBestEffort",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ParseDateTimeBestEffortOrNull))] = "parseDateTimeBestEffortOrNull",
        [(typeof(ClickHouseSafeCastDbFunctionsExtensions), nameof(ClickHouseSafeCastDbFunctionsExtensions.ParseDateTimeBestEffortOrZero))] = "parseDateTimeBestEffortOrZero",

        // DateTime extras (Tier 1d) — DateTrunc, ToTimeZone, TimeZoneOf, Now64.
        // ToStartOfInterval has special handling further below (it needs an INTERVAL literal).
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.DateTrunc))] = "dateTrunc",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.ToTimeZone))] = "toTimeZone",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.TimeZoneOf))] = "timeZoneOf",
        [(typeof(ClickHouseDateTruncDbFunctionsExtensions), nameof(ClickHouseDateTruncDbFunctionsExtensions.Now64))] = "now64",

        // Bit functions (Tier 2a)
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitAnd))] = "bitAnd",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitOr))] = "bitOr",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitXor))] = "bitXor",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitNot))] = "bitNot",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitShiftLeft))] = "bitShiftLeft",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitShiftRight))] = "bitShiftRight",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitRotateLeft))] = "bitRotateLeft",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitRotateRight))] = "bitRotateRight",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitCount))] = "bitCount",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitTest))] = "bitTest",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitTestAll))] = "bitTestAll",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitTestAny))] = "bitTestAny",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitSlice))] = "bitSlice",
        [(typeof(ClickHouseBitDbFunctionsExtensions), nameof(ClickHouseBitDbFunctionsExtensions.BitHammingDistance))] = "bitHammingDistance",

        // JSON typed extraction (Tier 2c)
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.JSONExtractInt))] = "JSONExtractInt",
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.JSONExtractUInt))] = "JSONExtractUInt",
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.JSONExtractFloat))] = "JSONExtractFloat",
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.JSONExtractBool))] = "JSONExtractBool",
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.JSONExtractString))] = "JSONExtractString",
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.JSONExtractRaw))] = "JSONExtractRaw",
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.JSONHas))] = "JSONHas",
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.JSONLength))] = "JSONLength",
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.JSONType))] = "JSONType",
        [(typeof(ClickHouseJsonExtractDbFunctionsExtensions), nameof(ClickHouseJsonExtractDbFunctionsExtensions.IsValidJSON))] = "isValidJSON",

        // Random functions (Tier 2d)
        [(typeof(ClickHouseRandomDbFunctionsExtensions), nameof(ClickHouseRandomDbFunctionsExtensions.Rand))] = "rand",
        [(typeof(ClickHouseRandomDbFunctionsExtensions), nameof(ClickHouseRandomDbFunctionsExtensions.Rand64))] = "rand64",
        [(typeof(ClickHouseRandomDbFunctionsExtensions), nameof(ClickHouseRandomDbFunctionsExtensions.RandCanonical))] = "randCanonical",
        [(typeof(ClickHouseRandomDbFunctionsExtensions), nameof(ClickHouseRandomDbFunctionsExtensions.RandomString))] = "randomString",
        [(typeof(ClickHouseRandomDbFunctionsExtensions), nameof(ClickHouseRandomDbFunctionsExtensions.RandomFixedString))] = "randomFixedString",
        [(typeof(ClickHouseRandomDbFunctionsExtensions), nameof(ClickHouseRandomDbFunctionsExtensions.RandomPrintableASCII))] = "randomPrintableASCII",
        [(typeof(ClickHouseRandomDbFunctionsExtensions), nameof(ClickHouseRandomDbFunctionsExtensions.RandUniform))] = "randUniform",
        [(typeof(ClickHouseRandomDbFunctionsExtensions), nameof(ClickHouseRandomDbFunctionsExtensions.RandNormal))] = "randNormal",

        // Server / session metadata (Tier 2e)
        [(typeof(ClickHouseServerDbFunctionsExtensions), nameof(ClickHouseServerDbFunctionsExtensions.Version))] = "version",
        [(typeof(ClickHouseServerDbFunctionsExtensions), nameof(ClickHouseServerDbFunctionsExtensions.HostName))] = "hostName",
        [(typeof(ClickHouseServerDbFunctionsExtensions), nameof(ClickHouseServerDbFunctionsExtensions.CurrentDatabase))] = "currentDatabase",
        [(typeof(ClickHouseServerDbFunctionsExtensions), nameof(ClickHouseServerDbFunctionsExtensions.CurrentUser))] = "currentUser",
        [(typeof(ClickHouseServerDbFunctionsExtensions), nameof(ClickHouseServerDbFunctionsExtensions.ServerTimezone))] = "serverTimezone",
        [(typeof(ClickHouseServerDbFunctionsExtensions), nameof(ClickHouseServerDbFunctionsExtensions.ServerUUID))] = "serverUUID",
        [(typeof(ClickHouseServerDbFunctionsExtensions), nameof(ClickHouseServerDbFunctionsExtensions.Uptime))] = "uptime",

        // Tuple ops (Tier 3a)
        [(typeof(ClickHouseTupleDbFunctionsExtensions), nameof(ClickHouseTupleDbFunctionsExtensions.TupleElement))] = "tupleElement",
        [(typeof(ClickHouseTupleDbFunctionsExtensions), nameof(ClickHouseTupleDbFunctionsExtensions.DotProduct))] = "dotProduct",
        [(typeof(ClickHouseTupleDbFunctionsExtensions), nameof(ClickHouseTupleDbFunctionsExtensions.TupleHammingDistance))] = "tupleHammingDistance",
        [(typeof(ClickHouseTupleDbFunctionsExtensions), nameof(ClickHouseTupleDbFunctionsExtensions.TuplePlus))] = "tuplePlus",
        [(typeof(ClickHouseTupleDbFunctionsExtensions), nameof(ClickHouseTupleDbFunctionsExtensions.TupleMinus))] = "tupleMinus",
        [(typeof(ClickHouseTupleDbFunctionsExtensions), nameof(ClickHouseTupleDbFunctionsExtensions.TupleMultiply))] = "tupleMultiply",
        [(typeof(ClickHouseTupleDbFunctionsExtensions), nameof(ClickHouseTupleDbFunctionsExtensions.TupleDivide))] = "tupleDivide",
        [(typeof(ClickHouseTupleDbFunctionsExtensions), nameof(ClickHouseTupleDbFunctionsExtensions.TupleNegate))] = "tupleNegate",
        [(typeof(ClickHouseTupleDbFunctionsExtensions), nameof(ClickHouseTupleDbFunctionsExtensions.FlattenTuple))] = "flattenTuple",

        // IPv6 + IP additions (Tier 3b)
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IPv6StringToNum))] = "IPv6StringToNum",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IPv6NumToString))] = "IPv6NumToString",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IPv4CIDRToRange))] = "IPv4CIDRToRange",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IPv6CIDRToRange))] = "IPv6CIDRToRange",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.IPv4ToIPv6))] = "IPv4ToIPv6",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.CutIPv6))] = "cutIPv6",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.ToIPv4))] = "toIPv4",
        [(typeof(ClickHouseIpDbFunctionsExtensions), nameof(ClickHouseIpDbFunctionsExtensions.ToIPv6))] = "toIPv6",

        // UUID v7 helpers (Tier 3c). DateTimeToUUIDv7 is intentionally NOT mapped
        // here — it's evaluated client-side via Guid.CreateVersion7(DateTimeOffset)
        // and bound as a parameter, so older CH servers that lack the
        // dateTimeToUUIDv7 builder still work.
        [(typeof(ClickHouseUuidDbFunctionsExtensions), nameof(ClickHouseUuidDbFunctionsExtensions.ToUUIDOrZero))] = "toUUIDOrZero",
        [(typeof(ClickHouseUuidDbFunctionsExtensions), nameof(ClickHouseUuidDbFunctionsExtensions.UUIDv7ToDateTime))] = "UUIDv7ToDateTime",

        // Math specials (Tier 3d)
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Pi))] = "pi",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.E))] = "e",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Degrees))] = "degrees",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Radians))] = "radians",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Factorial))] = "factorial",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Erf))] = "erf",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Erfc))] = "erfc",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Lgamma))] = "lgamma",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Tgamma))] = "tgamma",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.RoundBankers))] = "roundBankers",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.WidthBucket))] = "widthBucket",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Hypot))] = "hypot",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Log1P))] = "log1p",
        [(typeof(ClickHouseMathDbFunctionsExtensions), nameof(ClickHouseMathDbFunctionsExtensions.Sigmoid))] = "sigmoid",

        // String extras (Tier 3e)
        [(typeof(ClickHouseStringExtraDbFunctionsExtensions), nameof(ClickHouseStringExtraDbFunctionsExtensions.Left))] = "leftUTF8",
        [(typeof(ClickHouseStringExtraDbFunctionsExtensions), nameof(ClickHouseStringExtraDbFunctionsExtensions.Right))] = "rightUTF8",
        [(typeof(ClickHouseStringExtraDbFunctionsExtensions), nameof(ClickHouseStringExtraDbFunctionsExtensions.LeftPad))] = "leftPad",
        [(typeof(ClickHouseStringExtraDbFunctionsExtensions), nameof(ClickHouseStringExtraDbFunctionsExtensions.RightPad))] = "rightPad",
        [(typeof(ClickHouseStringExtraDbFunctionsExtensions), nameof(ClickHouseStringExtraDbFunctionsExtensions.Repeat))] = "repeat",
        [(typeof(ClickHouseStringExtraDbFunctionsExtensions), nameof(ClickHouseStringExtraDbFunctionsExtensions.Reverse))] = "reverseUTF8",
        [(typeof(ClickHouseStringExtraDbFunctionsExtensions), nameof(ClickHouseStringExtraDbFunctionsExtensions.InitCap))] = "initcapUTF8",
        [(typeof(ClickHouseStringExtraDbFunctionsExtensions), nameof(ClickHouseStringExtraDbFunctionsExtensions.Space))] = "space",
        [(typeof(ClickHouseStringExtraDbFunctionsExtensions), nameof(ClickHouseStringExtraDbFunctionsExtensions.ConcatWithSeparator))] = "concatWithSeparator",

        // Array helpers, no-lambda (Tier 2b-step-1)
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayDistinct))] = "arrayDistinct",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayUniq))] = "arrayUniq",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayCompact))] = "arrayCompact",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayConcat))] = "arrayConcat",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArraySlice))] = "arraySlice",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArraySort))] = "arraySort",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayReverseSort))] = "arrayReverseSort",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayProduct))] = "arrayProduct",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayCumSum))] = "arrayCumSum",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayDifference))] = "arrayDifference",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.IndexOf))] = "indexOf",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.CountEqual))] = "countEqual",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayElement))] = "arrayElement",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayPushBack))] = "arrayPushBack",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayPushFront))] = "arrayPushFront",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayPopBack))] = "arrayPopBack",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayPopFront))] = "arrayPopFront",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayResize))] = "arrayResize",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayZip))] = "arrayZip",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayReverse))] = "arrayReverse",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayFlatten))] = "arrayFlatten",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayIntersect))] = "arrayIntersect",
        [(typeof(ClickHouseArrayDbFunctionsExtensions), nameof(ClickHouseArrayDbFunctionsExtensions.ArrayEnumerate))] = "arrayEnumerate",
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

        // Special case: HasZooKeeperConfig() → no direct CH function. Use
        // hasColumnInTable('system', 'zookeeper', 'path') as a probe — the
        // system.zookeeper table only exposes a 'path' column when Keeper /
        // ZooKeeper is configured for the server.
        if (method.DeclaringType == typeof(ClickHouseKeeperDbFunctionsExtensions)
            && method.Name == nameof(ClickHouseKeeperDbFunctionsExtensions.HasZooKeeperConfig))
        {
            return _sqlExpressionFactory.Function(
                "hasColumnInTable",
                new SqlExpression[]
                {
                    _sqlExpressionFactory.Constant("system"),
                    _sqlExpressionFactory.Constant("zookeeper"),
                    _sqlExpressionFactory.Constant("path"),
                },
                nullable: false,
                argumentsPropagateNullability: new[] { false, false, false },
                typeof(bool));
        }

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

        // ToStartOfInterval(dt, value, unit) → toStartOfInterval(dt, toIntervalUnit(value)).
        // CH's toStartOfInterval expects an INTERVAL value as its second argument,
        // not separate value+unit args.
        if (method.DeclaringType == typeof(ClickHouseDateTruncDbFunctionsExtensions)
            && method.Name == nameof(ClickHouseDateTruncDbFunctionsExtensions.ToStartOfInterval)
            && functionArguments.Length == 3
            && TryExtractKnownIntervalUnit(functionArguments[2], out var startOfIntervalUnit))
        {
            var intervalFn = "toInterval"
                + char.ToUpperInvariant(startOfIntervalUnit.ToString()[0])
                + startOfIntervalUnit.ToString().Substring(1);
            var interval = _sqlExpressionFactory.Function(
                intervalFn,
                new[] { functionArguments[1] },
                nullable: true,
                argumentsPropagateNullability: new[] { true },
                typeof(object));
            return _sqlExpressionFactory.Function(
                "toStartOfInterval",
                new[] { functionArguments[0], interval },
                nullable: true,
                argumentsPropagateNullability: new[] { true, true },
                method.ReturnType);
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
        // Tier 2-3: CH returns UInt8/UInt32/UInt64 while our C# methods declare
        // a wider/signed type — wrap with a CH cast so the row reader sees the
        // declared CLR type.
        "bitCount",
        "bitTest",
        "bitHammingDistance",
        "factorial",
        "indexOf",
        "countEqual",
        "arrayUniq",
        "JSONLength",
        "JSONExtractInt",
        "JSONExtractUInt",
        "uptime",
        "position",
        "positionCaseInsensitiveUTF8",
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
