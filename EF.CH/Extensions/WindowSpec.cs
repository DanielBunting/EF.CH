namespace EF.CH.Extensions;

/// <summary>
/// Fluent builder for configuring window function OVER clauses in lambda expressions.
/// Used with the lambda-style Window function API.
/// </summary>
/// <remarks>
/// <para>
/// This type is used within lambda expressions passed to Window functions:
/// <code>
/// var result = context.Orders.Select(o => new
/// {
///     RowNum = Window.RowNumber(w => w
///         .PartitionBy(o.Region)
///         .OrderBy(o.OrderDate))
/// });
/// </code>
/// </para>
/// <para>
/// Unlike <see cref="WindowBuilder{T}"/>, this class is non-generic because it only
/// configures the OVER clause - the result type is determined by the Window method itself.
/// </para>
/// </remarks>
public sealed class WindowSpec
{
    /// <summary>
    /// Singleton instance used as the lambda parameter.
    /// </summary>
    internal static readonly WindowSpec Instance = new();

    private WindowSpec() { }

    /// <summary>
    /// Adds a PARTITION BY clause with the specified column.
    /// Multiple columns can be added by chaining calls.
    /// </summary>
    /// <typeparam name="TCol">The type of the column.</typeparam>
    /// <param name="column">The column expression to partition by.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec PartitionBy<TCol>(TCol column) => this;

    /// <summary>
    /// Adds an ORDER BY ASC clause with the specified column.
    /// Multiple columns can be added by chaining calls.
    /// </summary>
    /// <typeparam name="TCol">The type of the column.</typeparam>
    /// <param name="column">The column expression to order by ascending.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec OrderBy<TCol>(TCol column) => this;

    /// <summary>
    /// Adds an ORDER BY DESC clause with the specified column.
    /// Multiple columns can be added by chaining calls.
    /// </summary>
    /// <typeparam name="TCol">The type of the column.</typeparam>
    /// <param name="column">The column expression to order by descending.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec OrderByDescending<TCol>(TCol column) => this;

    /// <summary>
    /// Specifies that the frame type is ROWS (physical rows).
    /// Must be followed by frame boundary methods.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec Rows() => this;

    /// <summary>
    /// Specifies that the frame type is RANGE (logical value range).
    /// Must be followed by frame boundary methods.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec Range() => this;

    /// <summary>
    /// Specifies UNBOUNDED PRECEDING as the frame start or end boundary.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec UnboundedPreceding() => this;

    /// <summary>
    /// Specifies CURRENT ROW as the frame start or end boundary.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec CurrentRow() => this;

    /// <summary>
    /// Specifies UNBOUNDED FOLLOWING as the frame end boundary.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec UnboundedFollowing() => this;

    /// <summary>
    /// Specifies N PRECEDING as the frame start or end boundary.
    /// </summary>
    /// <param name="n">The number of rows preceding.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec Preceding(int n) => this;

    /// <summary>
    /// Specifies N FOLLOWING as the frame end boundary.
    /// </summary>
    /// <param name="n">The number of rows following.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowSpec Following(int n) => this;
}
