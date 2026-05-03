using System.Linq.Expressions;

namespace EF.CH.Extensions;

/// <summary>
/// Entry point for the ARRAY JOIN fluent chain. Created by
/// <see cref="ClickHouseQueryableExtensions.ArrayJoin{TEntity}"/>.
/// Add array selectors with <see cref="Of{T1}"/>; finalize with
/// <c>Select(...)</c> on a typed builder.
/// </summary>
public sealed class ArrayJoinBuilder<TEntity>
{
    internal readonly IQueryable<TEntity> Source;
    internal readonly bool IsLeft;

    internal ArrayJoinBuilder(IQueryable<TEntity> source, bool isLeft)
    {
        Source = source;
        IsLeft = isLeft;
    }

    /// <summary>
    /// Adds an array column to the chain.
    /// </summary>
    public ArrayJoinBuilder<TEntity, T1> Of<T1>(Expression<Func<TEntity, IEnumerable<T1>>> arraySelector)
    {
        ArgumentNullException.ThrowIfNull(arraySelector);
        return new ArrayJoinBuilder<TEntity, T1>(Source, IsLeft, arraySelector);
    }
}

/// <summary>One-array stage. Call <c>Select((e, t1) => ...)</c> to terminate, or <c>Of</c> to add another.</summary>
public sealed class ArrayJoinBuilder<TEntity, T1>
{
    internal readonly IQueryable<TEntity> Source;
    internal readonly bool IsLeft;
    internal readonly Expression<Func<TEntity, IEnumerable<T1>>> Array1;

    internal ArrayJoinBuilder(IQueryable<TEntity> source, bool isLeft,
        Expression<Func<TEntity, IEnumerable<T1>>> array1)
    {
        Source = source;
        IsLeft = isLeft;
        Array1 = array1;
    }

    public ArrayJoinBuilder<TEntity, T1, T2> Of<T2>(Expression<Func<TEntity, IEnumerable<T2>>> arraySelector)
    {
        ArgumentNullException.ThrowIfNull(arraySelector);
        return new ArrayJoinBuilder<TEntity, T1, T2>(Source, IsLeft, Array1, arraySelector);
    }

    public IQueryable<TResult> Select<TResult>(Expression<Func<TEntity, T1, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(resultSelector);
        return IsLeft
            ? Source.LeftArrayJoin(Array1, resultSelector)
            : Source.ArrayJoin(Array1, resultSelector);
    }
}

/// <summary>Two-array stage.</summary>
public sealed class ArrayJoinBuilder<TEntity, T1, T2>
{
    internal readonly IQueryable<TEntity> Source;
    internal readonly bool IsLeft;
    internal readonly Expression<Func<TEntity, IEnumerable<T1>>> Array1;
    internal readonly Expression<Func<TEntity, IEnumerable<T2>>> Array2;

    internal ArrayJoinBuilder(IQueryable<TEntity> source, bool isLeft,
        Expression<Func<TEntity, IEnumerable<T1>>> array1,
        Expression<Func<TEntity, IEnumerable<T2>>> array2)
    {
        Source = source;
        IsLeft = isLeft;
        Array1 = array1;
        Array2 = array2;
    }

    public ArrayJoinBuilder<TEntity, T1, T2, T3> Of<T3>(Expression<Func<TEntity, IEnumerable<T3>>> arraySelector)
    {
        ArgumentNullException.ThrowIfNull(arraySelector);
        return new ArrayJoinBuilder<TEntity, T1, T2, T3>(Source, IsLeft, Array1, Array2, arraySelector);
    }

    public IQueryable<TResult> Select<TResult>(Expression<Func<TEntity, T1, T2, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(resultSelector);
        if (IsLeft)
        {
            throw new NotSupportedException(
                "LEFT ARRAY JOIN is not yet supported with multiple arrays. " +
                "Use ArrayJoin(left: false) for the multi-array form.");
        }
        return Source.ArrayJoin(Array1, Array2, resultSelector);
    }
}

/// <summary>Three-array stage.</summary>
public sealed class ArrayJoinBuilder<TEntity, T1, T2, T3>
{
    internal readonly IQueryable<TEntity> Source;
    internal readonly bool IsLeft;
    internal readonly Expression<Func<TEntity, IEnumerable<T1>>> Array1;
    internal readonly Expression<Func<TEntity, IEnumerable<T2>>> Array2;
    internal readonly Expression<Func<TEntity, IEnumerable<T3>>> Array3;

    internal ArrayJoinBuilder(IQueryable<TEntity> source, bool isLeft,
        Expression<Func<TEntity, IEnumerable<T1>>> array1,
        Expression<Func<TEntity, IEnumerable<T2>>> array2,
        Expression<Func<TEntity, IEnumerable<T3>>> array3)
    {
        Source = source;
        IsLeft = isLeft;
        Array1 = array1;
        Array2 = array2;
        Array3 = array3;
    }

    public ArrayJoinBuilder<TEntity, T1, T2, T3, T4> Of<T4>(Expression<Func<TEntity, IEnumerable<T4>>> arraySelector)
    {
        ArgumentNullException.ThrowIfNull(arraySelector);
        return new ArrayJoinBuilder<TEntity, T1, T2, T3, T4>(Source, IsLeft, Array1, Array2, Array3, arraySelector);
    }

    public IQueryable<TResult> Select<TResult>(Expression<Func<TEntity, T1, T2, T3, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(resultSelector);
        if (IsLeft)
        {
            throw new NotSupportedException(
                "LEFT ARRAY JOIN is not yet supported with multiple arrays. " +
                "Use ArrayJoin(left: false) for the multi-array form.");
        }
        return ClickHouseArrayJoinExtensions.ArrayJoin3(Source, Array1, Array2, Array3, resultSelector);
    }
}

/// <summary>Four-array stage.</summary>
public sealed class ArrayJoinBuilder<TEntity, T1, T2, T3, T4>
{
    internal readonly IQueryable<TEntity> Source;
    internal readonly bool IsLeft;
    internal readonly Expression<Func<TEntity, IEnumerable<T1>>> Array1;
    internal readonly Expression<Func<TEntity, IEnumerable<T2>>> Array2;
    internal readonly Expression<Func<TEntity, IEnumerable<T3>>> Array3;
    internal readonly Expression<Func<TEntity, IEnumerable<T4>>> Array4;

    internal ArrayJoinBuilder(IQueryable<TEntity> source, bool isLeft,
        Expression<Func<TEntity, IEnumerable<T1>>> array1,
        Expression<Func<TEntity, IEnumerable<T2>>> array2,
        Expression<Func<TEntity, IEnumerable<T3>>> array3,
        Expression<Func<TEntity, IEnumerable<T4>>> array4)
    {
        Source = source;
        IsLeft = isLeft;
        Array1 = array1;
        Array2 = array2;
        Array3 = array3;
        Array4 = array4;
    }

    public ArrayJoinBuilder<TEntity, T1, T2, T3, T4, T5> Of<T5>(Expression<Func<TEntity, IEnumerable<T5>>> arraySelector)
    {
        ArgumentNullException.ThrowIfNull(arraySelector);
        return new ArrayJoinBuilder<TEntity, T1, T2, T3, T4, T5>(Source, IsLeft,
            Array1, Array2, Array3, Array4, arraySelector);
    }

    public IQueryable<TResult> Select<TResult>(Expression<Func<TEntity, T1, T2, T3, T4, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(resultSelector);
        if (IsLeft)
        {
            throw new NotSupportedException(
                "LEFT ARRAY JOIN is not yet supported with multiple arrays. " +
                "Use ArrayJoin(left: false) for the multi-array form.");
        }
        return ClickHouseArrayJoinExtensions.ArrayJoin4(Source, Array1, Array2, Array3, Array4, resultSelector);
    }
}

/// <summary>Five-array (terminal) stage. Past this, fall back to nested closures inside <c>Select</c>.</summary>
public sealed class ArrayJoinBuilder<TEntity, T1, T2, T3, T4, T5>
{
    internal readonly IQueryable<TEntity> Source;
    internal readonly bool IsLeft;
    internal readonly Expression<Func<TEntity, IEnumerable<T1>>> Array1;
    internal readonly Expression<Func<TEntity, IEnumerable<T2>>> Array2;
    internal readonly Expression<Func<TEntity, IEnumerable<T3>>> Array3;
    internal readonly Expression<Func<TEntity, IEnumerable<T4>>> Array4;
    internal readonly Expression<Func<TEntity, IEnumerable<T5>>> Array5;

    internal ArrayJoinBuilder(IQueryable<TEntity> source, bool isLeft,
        Expression<Func<TEntity, IEnumerable<T1>>> array1,
        Expression<Func<TEntity, IEnumerable<T2>>> array2,
        Expression<Func<TEntity, IEnumerable<T3>>> array3,
        Expression<Func<TEntity, IEnumerable<T4>>> array4,
        Expression<Func<TEntity, IEnumerable<T5>>> array5)
    {
        Source = source;
        IsLeft = isLeft;
        Array1 = array1;
        Array2 = array2;
        Array3 = array3;
        Array4 = array4;
        Array5 = array5;
    }

    public IQueryable<TResult> Select<TResult>(Expression<Func<TEntity, T1, T2, T3, T4, T5, TResult>> resultSelector)
    {
        ArgumentNullException.ThrowIfNull(resultSelector);
        if (IsLeft)
        {
            throw new NotSupportedException(
                "LEFT ARRAY JOIN is not yet supported with multiple arrays. " +
                "Use ArrayJoin(left: false) for the multi-array form.");
        }
        return ClickHouseArrayJoinExtensions.ArrayJoin5(Source, Array1, Array2, Array3, Array4, Array5, resultSelector);
    }
}
