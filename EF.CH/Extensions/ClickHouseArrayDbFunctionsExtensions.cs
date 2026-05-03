using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse array helpers
/// that don't take a lambda. These are LINQ translation stubs — calling them
/// outside of a query will throw.
/// </summary>
/// <remarks>
/// Lambda-based higher-order array functions (<c>arrayMap</c>,
/// <c>arrayFilter</c>, <c>arrayReduce</c>) are a separate effort — they need
/// lambda translation similar to <c>WindowSpec.PartitionBy</c>. The functions
/// here are the no-lambda subset.
/// </remarks>
public static class ClickHouseArrayDbFunctionsExtensions
{
    /// <summary>Translates to <c>arrayDistinct(arr)</c>.</summary>
    public static T[] ArrayDistinct<T>(this DbFunctions _, T[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayUniq(arr)</c>. Returns the count of distinct elements.</summary>
    public static long ArrayUniq<T>(this DbFunctions _, T[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayCompact(arr)</c>. Removes consecutive duplicates.</summary>
    public static T[] ArrayCompact<T>(this DbFunctions _, T[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayConcat(a, b)</c>.</summary>
    public static T[] ArrayConcat<T>(this DbFunctions _, T[] a, T[] b) => throw NotSupported();

    /// <summary>Translates to <c>arraySlice(arr, offset, length)</c>. 1-based offset.</summary>
    public static T[] ArraySlice<T>(this DbFunctions _, T[] arr, int offset, int length) => throw NotSupported();

    /// <summary>Translates to <c>arraySort(arr)</c>.</summary>
    public static T[] ArraySort<T>(this DbFunctions _, T[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayReverseSort(arr)</c>.</summary>
    public static T[] ArrayReverseSort<T>(this DbFunctions _, T[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayProduct(arr)</c>.</summary>
    public static double ArrayProduct(this DbFunctions _, double[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayCumSum(arr)</c>.</summary>
    public static long[] ArrayCumSum(this DbFunctions _, long[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayDifference(arr)</c>. Adjacent-pair differences.</summary>
    public static long[] ArrayDifference(this DbFunctions _, long[] arr) => throw NotSupported();

    /// <summary>
    /// Translates to <c>indexOf(arr, x)</c>. Returns the 1-based index of the
    /// first occurrence of <paramref name="x"/> in <paramref name="arr"/>,
    /// or 0 if not found.
    /// </summary>
    public static int IndexOf<T>(this DbFunctions _, T[] arr, T x) => throw NotSupported();

    /// <summary>Translates to <c>countEqual(arr, x)</c>.</summary>
    public static long CountEqual<T>(this DbFunctions _, T[] arr, T x) => throw NotSupported();

    /// <summary>Translates to <c>arrayElement(arr, n)</c>. 1-based; negative <paramref name="n"/> indexes from the end.</summary>
    public static T ArrayElement<T>(this DbFunctions _, T[] arr, int n) => throw NotSupported();

    /// <summary>Translates to <c>arrayPushBack(arr, x)</c>.</summary>
    public static T[] ArrayPushBack<T>(this DbFunctions _, T[] arr, T x) => throw NotSupported();

    /// <summary>Translates to <c>arrayPushFront(arr, x)</c>.</summary>
    public static T[] ArrayPushFront<T>(this DbFunctions _, T[] arr, T x) => throw NotSupported();

    /// <summary>Translates to <c>arrayPopBack(arr)</c>.</summary>
    public static T[] ArrayPopBack<T>(this DbFunctions _, T[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayPopFront(arr)</c>.</summary>
    public static T[] ArrayPopFront<T>(this DbFunctions _, T[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayResize(arr, size, defaultValue)</c>.</summary>
    public static T[] ArrayResize<T>(this DbFunctions _, T[] arr, int size, T defaultValue) => throw NotSupported();

    /// <summary>Translates to <c>arrayZip(a, b)</c>.</summary>
    public static object[] ArrayZip<T1, T2>(this DbFunctions _, T1[] a, T2[] b) => throw NotSupported();

    /// <summary>Translates to <c>arrayReverse(arr)</c>.</summary>
    public static T[] ArrayReverse<T>(this DbFunctions _, T[] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayFlatten(arr)</c>.</summary>
    public static T[] ArrayFlatten<T>(this DbFunctions _, T[][] arr) => throw NotSupported();

    /// <summary>Translates to <c>arrayIntersect(a, b)</c>.</summary>
    public static T[] ArrayIntersect<T>(this DbFunctions _, T[] a, T[] b) => throw NotSupported();

    /// <summary>Translates to <c>arrayEnumerate(arr)</c>. Returns 1..length(arr).</summary>
    public static int[] ArrayEnumerate<T>(this DbFunctions _, T[] arr) => throw NotSupported();

    private static InvalidOperationException NotSupported() =>
        new("This method is for LINQ translation only.");
}
