namespace EF.CH.Metadata.Attributes;

/// <summary>
/// Marks a property as an ALIAS column in ClickHouse.
/// The expression is computed at query time and not stored.
/// </summary>
/// <remarks>
/// <para>
/// ALIAS columns cannot be inserted into and have no storage cost.
/// They are computed on every read.
/// </para>
/// <para>
/// When using this attribute, the property is automatically configured with
/// <c>ValueGenerated.OnAddOrUpdate</c> to exclude it from all modifications.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public class Person
/// {
///     public string FirstName { get; set; }
///     public string LastName { get; set; }
///
///     [AliasColumn("concat(FirstName, ' ', LastName)")]
///     public string FullName { get; set; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class AliasColumnAttribute : Attribute
{
    /// <summary>
    /// The ClickHouse SQL expression to compute the column value.
    /// </summary>
    public string Expression { get; }

    /// <summary>
    /// Initializes a new instance of <see cref="AliasColumnAttribute"/>.
    /// </summary>
    /// <param name="expression">ClickHouse SQL expression (e.g., "concat(FirstName, ' ', LastName)")</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="expression"/> is null or whitespace.</exception>
    public AliasColumnAttribute(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        Expression = expression;
    }
}
