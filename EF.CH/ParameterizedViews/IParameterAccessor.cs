namespace EF.CH.ParameterizedViews;

/// <summary>
/// Provides access to parameterized view parameters in Where clause expressions.
/// </summary>
/// <remarks>
/// <para>
/// This interface is used in fluent view configuration to access parameters
/// in the Where clause expressions. The expressions using this interface are
/// analyzed at configuration time to generate the parameterized SQL.
/// </para>
/// <para>
/// Example usage:
/// <code>
/// entity.AsParameterizedView&lt;UserEventView, Event&gt;(cfg => cfg
///     .Parameter&lt;ulong&gt;("user_id")
///     .Parameter&lt;DateTime&gt;("start_date")
///     .Where((e, p) => e.UserId == p.Get&lt;ulong&gt;("user_id"))
///     .Where((e, p) => e.Timestamp >= p.Get&lt;DateTime&gt;("start_date")));
/// </code>
/// </para>
/// </remarks>
public interface IParameterAccessor
{
    /// <summary>
    /// Gets the value of a parameter by name.
    /// </summary>
    /// <typeparam name="T">The expected type of the parameter.</typeparam>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <returns>The parameter value (used for expression translation, not actual execution).</returns>
    /// <remarks>
    /// This method is not executed at runtime. It is used in LINQ expressions
    /// that are analyzed to generate parameterized SQL with <c>{name:Type}</c> syntax.
    /// </remarks>
    T Get<T>(string parameterName);
}

/// <summary>
/// Internal implementation of <see cref="IParameterAccessor"/> for expression analysis.
/// </summary>
/// <remarks>
/// This implementation throws at runtime because it's only meant for expression
/// tree analysis during view configuration.
/// </remarks>
internal sealed class ParameterAccessorPlaceholder : IParameterAccessor
{
    /// <summary>
    /// Gets the singleton instance.
    /// </summary>
    public static readonly ParameterAccessorPlaceholder Instance = new();

    private ParameterAccessorPlaceholder() { }

    /// <inheritdoc />
    public T Get<T>(string parameterName)
    {
        throw new InvalidOperationException(
            $"IParameterAccessor.Get<{typeof(T).Name}>(\"{parameterName}\") should not be called at runtime. " +
            "This method is only for use in expression trees during view configuration.");
    }
}
