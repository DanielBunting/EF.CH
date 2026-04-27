namespace EF.CH.Extensions;

/// <summary>
/// Internal type used by the window function translator. Public callers use the
/// lambda-style overloads on <see cref="Window"/> (e.g. <c>Window.RowNumber(w =&gt; ...)</c>);
/// the lambda receives a <see cref="WindowSpec"/>, not this type.
/// </summary>
/// <typeparam name="T">The result type of the window function.</typeparam>
internal sealed class WindowBuilder<T>
{
    /// <summary>
    /// Gets the SQL function name (e.g., "row_number", "lagInFrame").
    /// </summary>
    internal string FunctionName { get; }

    /// <summary>
    /// Gets the function arguments (e.g., buckets for ntile, offset for lag).
    /// These are literal values, not entity-dependent expressions.
    /// </summary>
    internal object?[] FunctionArguments { get; }

    /// <summary>
    /// Creates a new WindowBuilder for the specified function.
    /// </summary>
    internal WindowBuilder(string functionName, params object?[] arguments)
    {
        FunctionName = functionName;
        FunctionArguments = arguments;
    }

    /// <summary>
    /// Adds a PARTITION BY clause with the specified column.
    /// Multiple columns can be added by chaining calls.
    /// </summary>
    /// <typeparam name="TCol">The type of the column.</typeparam>
    /// <param name="column">The column expression to partition by.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> PartitionBy<TCol>(TCol column) => this;

    /// <summary>
    /// Adds an ORDER BY ASC clause with the specified column.
    /// Multiple columns can be added by chaining calls.
    /// </summary>
    /// <typeparam name="TCol">The type of the column.</typeparam>
    /// <param name="column">The column expression to order by ascending.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> OrderBy<TCol>(TCol column) => this;

    /// <summary>
    /// Adds an ORDER BY DESC clause with the specified column.
    /// Multiple columns can be added by chaining calls.
    /// </summary>
    /// <typeparam name="TCol">The type of the column.</typeparam>
    /// <param name="column">The column expression to order by descending.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> OrderByDescending<TCol>(TCol column) => this;

    /// <summary>
    /// Specifies that the frame type is ROWS (physical rows).
    /// Must be followed by frame boundary methods.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> Rows() => this;

    /// <summary>
    /// Specifies that the frame type is RANGE (logical value range).
    /// Must be followed by frame boundary methods.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> Range() => this;

    /// <summary>
    /// Specifies UNBOUNDED PRECEDING as the frame start or end boundary.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> UnboundedPreceding() => this;

    /// <summary>
    /// Specifies CURRENT ROW as the frame start or end boundary.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> CurrentRow() => this;

    /// <summary>
    /// Specifies UNBOUNDED FOLLOWING as the frame end boundary.
    /// </summary>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> UnboundedFollowing() => this;

    /// <summary>
    /// Specifies N PRECEDING as the frame start or end boundary.
    /// </summary>
    /// <param name="n">The number of rows preceding.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> Preceding(int n) => this;

    /// <summary>
    /// Specifies N FOLLOWING as the frame end boundary.
    /// </summary>
    /// <param name="n">The number of rows following.</param>
    /// <returns>The builder for method chaining.</returns>
    public WindowBuilder<T> Following(int n) => this;

    /// <summary>
    /// Builds and returns the window function result value.
    /// Use this method at the end of the window function chain in LINQ projections
    /// to ensure the correct result type is used.
    /// </summary>
    /// <example>
    /// <code>
    /// var query = context.Orders.Select(o => new
    /// {
    ///     RowNum = Window.RowNumber()
    ///         .PartitionBy(o.Region)
    ///         .OrderBy(o.OrderDate)
    ///         .Build()
    /// });
    /// </code>
    /// </example>
    /// <remarks>
    /// This method returns default(T?) - the actual value is computed by the database.
    /// The method exists to ensure the correct type is captured in the expression tree.
    /// </remarks>
    public T? Build() => default;
}
