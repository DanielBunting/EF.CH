using System.Linq.Expressions;

namespace EF.CH.Extensions;

/// <summary>
/// Builder for specifying multiple column interpolations.
/// </summary>
/// <typeparam name="T">The entity type.</typeparam>
public class InterpolateBuilder<T>
{
    internal List<InterpolateColumn> Columns { get; } = new();

    /// <summary>
    /// Specifies a column to interpolate using a mode (Default or Prev).
    /// </summary>
    /// <typeparam name="TValue">The column value type.</typeparam>
    /// <param name="column">The column to interpolate.</param>
    /// <param name="mode">The interpolation mode.</param>
    /// <returns>This builder for chaining.</returns>
    public InterpolateBuilder<T> Fill<TValue>(Expression<Func<T, TValue>> column, InterpolateMode mode)
    {
        ArgumentNullException.ThrowIfNull(column);
        Columns.Add(new InterpolateColumn(column, mode, null, false));
        return this;
    }

    /// <summary>
    /// Specifies a column to fill with a constant value.
    /// </summary>
    /// <typeparam name="TValue">The column value type.</typeparam>
    /// <param name="column">The column to fill.</param>
    /// <param name="constant">The constant value to use for filled rows.</param>
    /// <returns>This builder for chaining.</returns>
    public InterpolateBuilder<T> Fill<TValue>(Expression<Func<T, TValue>> column, TValue constant)
    {
        ArgumentNullException.ThrowIfNull(column);
        Columns.Add(new InterpolateColumn(column, InterpolateMode.Default, constant, true));
        return this;
    }

    internal record InterpolateColumn(
        LambdaExpression Column,
        InterpolateMode Mode,
        object? Constant,
        bool IsConstant);
}
