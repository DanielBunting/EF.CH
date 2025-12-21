namespace EF.CH.Metadata.Attributes;

/// <summary>
/// Specifies a DEFAULT expression for a column in ClickHouse.
/// The expression is computed if no value is provided on INSERT.
/// </summary>
/// <remarks>
/// <para>
/// This is different from EF Core's <c>DefaultValueSql</c> in that it uses
/// ClickHouse-specific SQL syntax and functions.
/// </para>
/// <para>
/// Unlike MATERIALIZED columns, DEFAULT columns can be explicitly set during INSERT.
/// The expression is only evaluated when no value is provided.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public class Event
/// {
///     public Guid Id { get; set; }
///
///     [DefaultExpression("now()")]
///     public DateTime CreatedAt { get; set; }
///
///     [DefaultExpression("generateUUIDv4()")]
///     public Guid TraceId { get; set; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class DefaultExpressionAttribute : Attribute
{
    /// <summary>
    /// The ClickHouse SQL expression for the default value.
    /// </summary>
    public string Expression { get; }

    /// <summary>
    /// Initializes a new instance of <see cref="DefaultExpressionAttribute"/>.
    /// </summary>
    /// <param name="expression">ClickHouse SQL expression (e.g., "now()", "generateUUIDv4()")</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="expression"/> is null or whitespace.</exception>
    public DefaultExpressionAttribute(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        Expression = expression;
    }
}
