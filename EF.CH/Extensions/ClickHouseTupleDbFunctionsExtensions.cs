using Microsoft.EntityFrameworkCore;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods on <see cref="DbFunctions"/> for ClickHouse tuple
/// operations. The headline use case is <see cref="DotProduct"/> and
/// <see cref="TupleHammingDistance"/> for vector-similarity workloads.
/// These are LINQ translation stubs — calling them outside of a query will throw.
/// </summary>
public static class ClickHouseTupleDbFunctionsExtensions
{
    /// <summary>Translates to <c>tupleElement(t, n)</c>. 1-based index.</summary>
    public static T TupleElement<T>(this DbFunctions _, object t, int n) => throw NotSupported();

    /// <summary>
    /// Translates to <c>dotProduct(t1, t2)</c>. Returns the scalar dot product
    /// of two equal-arity numeric tuples — useful for cosine-similarity /
    /// embedded-vector comparisons.
    /// </summary>
    public static double DotProduct(this DbFunctions _, object t1, object t2) => throw NotSupported();

    /// <summary>Translates to <c>tupleHammingDistance(t1, t2)</c>.</summary>
    public static long TupleHammingDistance(this DbFunctions _, object t1, object t2) => throw NotSupported();

    /// <summary>Translates to <c>tuplePlus(t1, t2)</c>. Element-wise addition.</summary>
    public static object TuplePlus(this DbFunctions _, object t1, object t2) => throw NotSupported();

    /// <summary>Translates to <c>tupleMinus(t1, t2)</c>. Element-wise subtraction.</summary>
    public static object TupleMinus(this DbFunctions _, object t1, object t2) => throw NotSupported();

    /// <summary>Translates to <c>tupleMultiply(t1, t2)</c>. Element-wise multiplication.</summary>
    public static object TupleMultiply(this DbFunctions _, object t1, object t2) => throw NotSupported();

    /// <summary>Translates to <c>tupleDivide(t1, t2)</c>. Element-wise division.</summary>
    public static object TupleDivide(this DbFunctions _, object t1, object t2) => throw NotSupported();

    /// <summary>Translates to <c>tupleNegate(t)</c>. Element-wise negation.</summary>
    public static object TupleNegate(this DbFunctions _, object t) => throw NotSupported();

    /// <summary>Translates to <c>flattenTuple(t)</c>.</summary>
    public static object FlattenTuple(this DbFunctions _, object t) => throw NotSupported();

    private static InvalidOperationException NotSupported() =>
        new("This method is for LINQ translation only.");
}
