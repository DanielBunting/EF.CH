namespace EF.CH.Metadata.Attributes;

/// <summary>
/// Marks a property as a ClickHouse EPHEMERAL column. EPHEMERAL columns are
/// not stored on disk; they exist only during INSERT and their values feed
/// into DEFAULT / MATERIALIZED expressions on other columns.
/// </summary>
/// <remarks>
/// <para>
/// EPHEMERAL columns cannot be SELECTed from ClickHouse. EF.CH rewrites
/// projections of ephemeral properties to <c>NULL</c> so queries don't error
/// — but the property will always read back as its CLR default.
/// </para>
/// <para>
/// Mutually exclusive with <see cref="MaterializedColumnAttribute"/>,
/// <see cref="AliasColumnAttribute"/>, and <see cref="DefaultExpressionAttribute"/>.
/// CODECs are not allowed (ephemeral columns have no storage).
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public class Record
/// {
///     public Guid Id { get; set; }
///
///     [EphemeralColumn]
///     public ulong UnhashedKey { get; set; }
///
///     [MaterializedColumn("sipHash64(UnhashedKey)")]
///     public ulong HashedKey { get; set; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class EphemeralColumnAttribute : Attribute
{
    /// <summary>
    /// Optional ClickHouse SQL default expression. When set, ClickHouse uses
    /// this value if the caller omits the column from INSERT.
    /// </summary>
    public string? DefaultExpression { get; }

    /// <summary>
    /// Marks the column as EPHEMERAL with no default expression.
    /// </summary>
    public EphemeralColumnAttribute()
    {
    }

    /// <summary>
    /// Marks the column as EPHEMERAL with a ClickHouse SQL default expression.
    /// </summary>
    /// <param name="defaultExpression">ClickHouse SQL expression (e.g. <c>"now()"</c>).</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="defaultExpression"/> is null or whitespace.</exception>
    public EphemeralColumnAttribute(string defaultExpression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(defaultExpression);
        DefaultExpression = defaultExpression;
    }
}
