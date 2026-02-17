using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse hashing functions.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseHashDbFunctionsExtensions
{
    /// <summary>
    /// Translates to ClickHouse <c>cityHash64(value)</c>.
    /// Computes the CityHash64 hash of the given value.
    /// </summary>
    public static ulong CityHash64<T>(this DbFunctions _, T value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>sipHash64(value)</c>.
    /// Computes the SipHash64 hash of the given value.
    /// </summary>
    public static ulong SipHash64<T>(this DbFunctions _, T value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>xxHash64(value)</c>.
    /// Computes the xxHash64 hash of the given value.
    /// </summary>
    public static ulong XxHash64<T>(this DbFunctions _, T value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>murmurHash3_64(value)</c>.
    /// Computes the MurmurHash3 64-bit hash of the given value.
    /// </summary>
    public static ulong MurmurHash3_64<T>(this DbFunctions _, T value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>farmHash64(value)</c>.
    /// Computes the FarmHash64 hash of the given value.
    /// </summary>
    public static ulong FarmHash64<T>(this DbFunctions _, T value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>hex(MD5(value))</c>.
    /// Computes the MD5 hash and returns it as a hex string.
    /// </summary>
    public static string Md5<T>(this DbFunctions _, T value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>hex(SHA256(value))</c>.
    /// Computes the SHA-256 hash and returns it as a hex string.
    /// </summary>
    public static string Sha256<T>(this DbFunctions _, T value)
        => throw new InvalidOperationException("This method is for LINQ translation only.");

    /// <summary>
    /// Translates to ClickHouse <c>yandexConsistentHash(hash, buckets)</c>.
    /// Maps a hash to one of N buckets using consistent hashing.
    /// </summary>
    public static uint ConsistentHash(this DbFunctions _, ulong hash, uint buckets)
        => throw new InvalidOperationException("This method is for LINQ translation only.");
}
