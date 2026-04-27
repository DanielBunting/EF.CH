using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Metadata;

/// <summary>
/// Read-side extension methods on <see cref="IReadOnlyProperty"/> for ClickHouse-specific
/// metadata.
/// </summary>
public static class ClickHousePropertyExtensions
{
    /// <summary>
    /// Returns the value configured via <c>HasDefaultForNull</c> for this property,
    /// or <c>null</c> if no default-for-null is configured.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this accessor when writing query helpers that need to branch on whether a
    /// property has a default-for-null configured. Properties with a configured
    /// default-for-null have a value converter that maps <c>null</c> to the default
    /// value on read, which silently breaks predicates like <c>p.HasValue</c> and
    /// <c>p ?? fallback</c>.
    /// </para>
    /// <para>
    /// A return value of <c>null</c> means no default-for-null was configured (the
    /// property either uses ClickHouse's <c>Nullable(T)</c> wrapper or is not
    /// nullable). This is distinct from a configured default of <c>null</c>, which
    /// is not a valid configuration.
    /// </para>
    /// </remarks>
    /// <param name="property">The property to inspect.</param>
    /// <returns>The configured default value, or <c>null</c> if not configured.</returns>
    public static object? GetDefaultForNullValue(this IReadOnlyProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);
        return property.FindAnnotation(ClickHouseAnnotationNames.DefaultForNull)?.Value;
    }
}
