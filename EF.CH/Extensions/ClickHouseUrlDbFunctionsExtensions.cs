using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse URL parsing functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseUrlDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>domain(url)</c>.
    /// Extracts the domain from a URL.
    /// </summary>
    public static string Domain(this DbFunctions _, string url)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>domainWithoutWWW(url)</c>.
    /// Extracts the domain from a URL, removing "www." prefix if present.
    /// </summary>
    public static string DomainWithoutWWW(this DbFunctions _, string url)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>topLevelDomain(url)</c>.
    /// Extracts the top-level domain from a URL.
    /// </summary>
    public static string TopLevelDomain(this DbFunctions _, string url)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>protocol(url)</c>.
    /// Extracts the protocol (e.g. "https") from a URL.
    /// </summary>
    public static string Protocol(this DbFunctions _, string url)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>path(url)</c>.
    /// Extracts the path component from a URL.
    /// </summary>
    public static string UrlPath(this DbFunctions _, string url)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>extractURLParameter(url, name)</c>.
    /// Extracts the value of a named URL query parameter.
    /// </summary>
    public static string ExtractURLParameter(this DbFunctions _, string url, string name)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>extractURLParameters(url)</c>.
    /// Extracts all URL query parameters as an array of name=value strings.
    /// </summary>
    public static string[] ExtractURLParameters(this DbFunctions _, string url)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>cutURLParameter(url, name)</c>.
    /// Returns the URL with the specified query parameter removed.
    /// </summary>
    public static string CutURLParameter(this DbFunctions _, string url, string name)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>decodeURLComponent(s)</c>.
    /// Decodes a URL-encoded string.
    /// </summary>
    public static string DecodeURLComponent(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>encodeURLComponent(s)</c>.
    /// URL-encodes a string.
    /// </summary>
    public static string EncodeURLComponent(this DbFunctions _, string s)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
