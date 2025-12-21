namespace EF.CH.Metadata.Attributes;

/// <summary>
/// Marks a property as a MATERIALIZED column in ClickHouse.
/// The expression is computed on INSERT and stored on disk.
/// </summary>
/// <remarks>
/// <para>
/// MATERIALIZED columns are not returned by SELECT * by default.
/// Use explicit column selection to read them.
/// </para>
/// <para>
/// When using this attribute, the property is automatically configured with
/// <c>ValueGenerated.OnAdd</c> to exclude it from INSERT statements.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public class Order
/// {
///     public decimal Amount { get; set; }
///
///     [MaterializedColumn("Amount * 1.1")]
///     public decimal TotalWithTax { get; set; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class MaterializedColumnAttribute : Attribute
{
    /// <summary>
    /// The ClickHouse SQL expression to compute the column value.
    /// </summary>
    public string Expression { get; }

    /// <summary>
    /// Initializes a new instance of <see cref="MaterializedColumnAttribute"/>.
    /// </summary>
    /// <param name="expression">ClickHouse SQL expression (e.g., "Amount * 1.1", "toYear(CreatedAt)")</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="expression"/> is null or whitespace.</exception>
    public MaterializedColumnAttribute(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        Expression = expression;
    }
}
