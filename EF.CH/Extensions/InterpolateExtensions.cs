using System.Linq.Expressions;
using System.Reflection;

namespace EF.CH.Extensions;

/// <summary>
/// Provides LINQ extension methods for ClickHouse WITH FILL and INTERPOLATE clauses.
/// </summary>
/// <remarks>
/// <para>
/// Interpolate fills gaps in time series or sequence data by inserting missing rows.
/// It works with ORDER BY columns to ensure continuous data points.
/// </para>
/// <para>
/// Typical usage patterns:
/// <code>
/// // Basic gap filling
/// context.Readings
///     .OrderBy(x => x.Hour)
///     .Interpolate(x => x.Hour, TimeSpan.FromHours(1))
///
/// // With forward-fill for values
/// context.Readings
///     .OrderBy(x => x.Hour)
///     .Interpolate(x => x.Hour, TimeSpan.FromHours(1), x => x.Value, InterpolateMode.Prev)
///
/// // Multiple columns
/// context.Readings
///     .OrderBy(x => x.Hour)
///     .Interpolate(x => x.Hour, TimeSpan.FromHours(1), i => i
///         .Fill(x => x.Value, InterpolateMode.Prev)
///         .Fill(x => x.Count, 0))
/// </code>
/// </para>
/// </remarks>
public static class InterpolateExtensions
{
    #region TimeSpan step overloads

    /// <summary>
    /// Fills gaps in the ORDER BY column using a TimeSpan step.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        TimeSpan step)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateTimeSpanMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step)));
    }

    /// <summary>
    /// Fills gaps with FROM/TO bounds using a TimeSpan step.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        TimeSpan step,
        TFill from,
        TFill to)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateTimeSpanFromToMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Constant(from, typeof(TFill)),
                Expression.Constant(to, typeof(TFill))));
    }

    /// <summary>
    /// Fills gaps and interpolates a single column using an InterpolateMode.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill, TValue>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        TimeSpan step,
        Expression<Func<T, TValue>> column,
        InterpolateMode mode)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);
        ArgumentNullException.ThrowIfNull(column);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateTimeSpanColumnModeMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill), typeof(TValue)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Quote(column),
                Expression.Constant(mode)));
    }

    /// <summary>
    /// Fills gaps and interpolates a single column with a constant value.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill, TValue>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        TimeSpan step,
        Expression<Func<T, TValue>> column,
        TValue constant)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);
        ArgumentNullException.ThrowIfNull(column);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateTimeSpanColumnConstantMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill), typeof(TValue)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Quote(column),
                Expression.Constant(constant, typeof(TValue))));
    }

    /// <summary>
    /// Fills gaps and interpolates multiple columns via a builder.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        TimeSpan step,
        Action<InterpolateBuilder<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new InterpolateBuilder<T>();
        configure(builder);

        return InterpolateWithBuilder(source, fill, step, builder);
    }

    /// <summary>
    /// Internal method that takes the builder directly (for expression tree capture).
    /// </summary>
    internal static IQueryable<T> InterpolateWithBuilder<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        TimeSpan step,
        InterpolateBuilder<T> builder)
    {
        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateTimeSpanBuilderMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Constant(builder)));
    }

    #endregion

    #region ClickHouseInterval step overloads

    /// <summary>
    /// Fills gaps using a ClickHouseInterval step (for months, quarters, years).
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        ClickHouseInterval step)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateIntervalMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step)));
    }

    /// <summary>
    /// Fills gaps with FROM/TO bounds using a ClickHouseInterval step.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        ClickHouseInterval step,
        TFill from,
        TFill to)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateIntervalFromToMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Constant(from, typeof(TFill)),
                Expression.Constant(to, typeof(TFill))));
    }

    /// <summary>
    /// Fills gaps and interpolates a single column using an InterpolateMode.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill, TValue>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        ClickHouseInterval step,
        Expression<Func<T, TValue>> column,
        InterpolateMode mode)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);
        ArgumentNullException.ThrowIfNull(column);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateIntervalColumnModeMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill), typeof(TValue)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Quote(column),
                Expression.Constant(mode)));
    }

    /// <summary>
    /// Fills gaps and interpolates a single column with a constant value.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill, TValue>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        ClickHouseInterval step,
        Expression<Func<T, TValue>> column,
        TValue constant)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);
        ArgumentNullException.ThrowIfNull(column);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateIntervalColumnConstantMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill), typeof(TValue)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Quote(column),
                Expression.Constant(constant, typeof(TValue))));
    }

    /// <summary>
    /// Fills gaps and interpolates multiple columns via a builder.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        ClickHouseInterval step,
        Action<InterpolateBuilder<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new InterpolateBuilder<T>();
        configure(builder);

        return InterpolateWithBuilder(source, fill, step, builder);
    }

    /// <summary>
    /// Internal method that takes the builder directly (for expression tree capture).
    /// </summary>
    internal static IQueryable<T> InterpolateWithBuilder<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        ClickHouseInterval step,
        InterpolateBuilder<T> builder)
    {
        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateIntervalBuilderMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Constant(builder)));
    }

    #endregion

    #region Numeric step overloads

    /// <summary>
    /// Fills gaps using a numeric step (for numeric ORDER BY columns).
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        int step)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateNumericMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step)));
    }

    /// <summary>
    /// Fills gaps with FROM/TO bounds using a numeric step.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        int step,
        TFill from,
        TFill to)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateNumericFromToMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Constant(from, typeof(TFill)),
                Expression.Constant(to, typeof(TFill))));
    }

    /// <summary>
    /// Fills gaps and interpolates a single column using an InterpolateMode.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill, TValue>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        int step,
        Expression<Func<T, TValue>> column,
        InterpolateMode mode)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);
        ArgumentNullException.ThrowIfNull(column);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateNumericColumnModeMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill), typeof(TValue)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Quote(column),
                Expression.Constant(mode)));
    }

    /// <summary>
    /// Fills gaps and interpolates a single column with a constant value.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill, TValue>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        int step,
        Expression<Func<T, TValue>> column,
        TValue constant)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);
        ArgumentNullException.ThrowIfNull(column);

        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateNumericColumnConstantMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill), typeof(TValue)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Quote(column),
                Expression.Constant(constant, typeof(TValue))));
    }

    /// <summary>
    /// Fills gaps and interpolates multiple columns via a builder.
    /// </summary>
    public static IQueryable<T> Interpolate<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        int step,
        Action<InterpolateBuilder<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(fill);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new InterpolateBuilder<T>();
        configure(builder);

        return InterpolateWithBuilder(source, fill, step, builder);
    }

    /// <summary>
    /// Internal method that takes the builder directly (for expression tree capture).
    /// </summary>
    internal static IQueryable<T> InterpolateWithBuilder<T, TFill>(
        this IQueryable<T> source,
        Expression<Func<T, TFill>> fill,
        int step,
        InterpolateBuilder<T> builder)
    {
        return source.Provider.CreateQuery<T>(
            Expression.Call(
                null,
                InterpolateNumericBuilderMethodInfo.MakeGenericMethod(typeof(T), typeof(TFill)),
                source.Expression,
                Expression.Quote(fill),
                Expression.Constant(step),
                Expression.Constant(builder)));
    }

    #endregion

    #region MethodInfo references for translation

    // TimeSpan step methods
    internal static readonly MethodInfo InterpolateTimeSpanMethodInfo =
        GetMethodInfo(nameof(Interpolate), 3, typeof(TimeSpan), false, false);

    internal static readonly MethodInfo InterpolateTimeSpanFromToMethodInfo =
        GetMethodInfo(nameof(Interpolate), 5, typeof(TimeSpan), false, false);

    internal static readonly MethodInfo InterpolateTimeSpanColumnModeMethodInfo =
        GetMethodInfo(nameof(Interpolate), 5, typeof(TimeSpan), true, true);

    internal static readonly MethodInfo InterpolateTimeSpanColumnConstantMethodInfo =
        GetMethodInfo(nameof(Interpolate), 5, typeof(TimeSpan), true, false);

    internal static readonly MethodInfo InterpolateTimeSpanBuilderMethodInfo =
        GetBuilderMethodInfo(nameof(Interpolate), typeof(TimeSpan));

    // ClickHouseInterval step methods
    internal static readonly MethodInfo InterpolateIntervalMethodInfo =
        GetMethodInfo(nameof(Interpolate), 3, typeof(ClickHouseInterval), false, false);

    internal static readonly MethodInfo InterpolateIntervalFromToMethodInfo =
        GetMethodInfo(nameof(Interpolate), 5, typeof(ClickHouseInterval), false, false);

    internal static readonly MethodInfo InterpolateIntervalColumnModeMethodInfo =
        GetMethodInfo(nameof(Interpolate), 5, typeof(ClickHouseInterval), true, true);

    internal static readonly MethodInfo InterpolateIntervalColumnConstantMethodInfo =
        GetMethodInfo(nameof(Interpolate), 5, typeof(ClickHouseInterval), true, false);

    internal static readonly MethodInfo InterpolateIntervalBuilderMethodInfo =
        GetBuilderMethodInfo(nameof(Interpolate), typeof(ClickHouseInterval));

    // Numeric step methods
    internal static readonly MethodInfo InterpolateNumericMethodInfo =
        GetMethodInfo(nameof(Interpolate), 3, typeof(int), false, false);

    internal static readonly MethodInfo InterpolateNumericFromToMethodInfo =
        GetMethodInfo(nameof(Interpolate), 5, typeof(int), false, false);

    internal static readonly MethodInfo InterpolateNumericColumnModeMethodInfo =
        GetMethodInfo(nameof(Interpolate), 5, typeof(int), true, true);

    internal static readonly MethodInfo InterpolateNumericColumnConstantMethodInfo =
        GetMethodInfo(nameof(Interpolate), 5, typeof(int), true, false);

    internal static readonly MethodInfo InterpolateNumericBuilderMethodInfo =
        GetBuilderMethodInfo(nameof(Interpolate), typeof(int));

    private static MethodInfo GetMethodInfo(string name, int paramCount, Type stepType, bool hasColumn, bool hasMode)
    {
        return typeof(InterpolateExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == name
                && m.GetParameters().Length == paramCount
                && m.GetParameters()[2].ParameterType == stepType
                && (hasColumn ? m.GetGenericArguments().Length == 3 : m.GetGenericArguments().Length == 2)
                && (hasMode
                    ? m.GetParameters().Last().ParameterType == typeof(InterpolateMode)
                    : m.GetParameters().Last().ParameterType != typeof(InterpolateMode)));
    }

    private static MethodInfo GetBuilderMethodInfo(string name, Type stepType)
    {
        // Look for the internal InterpolateWithBuilder method that takes InterpolateBuilder<T> directly
        return typeof(InterpolateExtensions).GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
            .First(m => m.Name == nameof(InterpolateWithBuilder)
                && m.GetParameters().Length == 4
                && m.GetParameters()[2].ParameterType == stepType
                && m.GetParameters()[3].ParameterType.IsGenericType
                && m.GetParameters()[3].ParameterType.GetGenericTypeDefinition() == typeof(InterpolateBuilder<>));
    }

    /// <summary>
    /// All MethodInfo references for use by the expression filter.
    /// </summary>
    internal static readonly MethodInfo[] AllMethodInfos = new[]
    {
        InterpolateTimeSpanMethodInfo,
        InterpolateTimeSpanFromToMethodInfo,
        InterpolateTimeSpanColumnModeMethodInfo,
        InterpolateTimeSpanColumnConstantMethodInfo,
        InterpolateTimeSpanBuilderMethodInfo,
        InterpolateIntervalMethodInfo,
        InterpolateIntervalFromToMethodInfo,
        InterpolateIntervalColumnModeMethodInfo,
        InterpolateIntervalColumnConstantMethodInfo,
        InterpolateIntervalBuilderMethodInfo,
        InterpolateNumericMethodInfo,
        InterpolateNumericFromToMethodInfo,
        InterpolateNumericColumnModeMethodInfo,
        InterpolateNumericColumnConstantMethodInfo,
        InterpolateNumericBuilderMethodInfo
    };

    #endregion
}
