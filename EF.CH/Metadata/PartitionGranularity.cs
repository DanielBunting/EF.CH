namespace EF.CH.Metadata;

/// <summary>
/// Granularity used by the column-selector
/// <see cref="EF.CH.Extensions.ClickHouseEntityTypeBuilderExtensions.HasPartitionBy{TEntity, TProperty}(Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder{TEntity}, System.Linq.Expressions.Expression{System.Func{TEntity, TProperty}}, PartitionGranularity)"/>
/// overload to wrap the column in the corresponding ClickHouse date function.
/// </summary>
/// <remarks>
/// The Day / Month / Year values map to the integer-key functions
/// <c>toYYYYMMDD</c> / <c>toYYYYMM</c> / <c>toYear</c> for byte-identity
/// with the legacy granularity-specific methods that this enum replaces.
/// Hour / Week / Quarter map to <c>toStartOfHour</c> / <c>toStartOfWeek</c> /
/// <c>toStartOfQuarter</c> — they have no integer-key analogue in ClickHouse.
/// </remarks>
public enum PartitionGranularity
{
    /// <summary>
    /// No wrapper — partition by the raw column.
    /// </summary>
    None,

    /// <summary>
    /// <c>toStartOfHour(column)</c>.
    /// </summary>
    Hour,

    /// <summary>
    /// <c>toYYYYMMDD(column)</c>.
    /// </summary>
    Day,

    /// <summary>
    /// <c>toStartOfWeek(column)</c>.
    /// </summary>
    Week,

    /// <summary>
    /// <c>toYYYYMM(column)</c>.
    /// </summary>
    Month,

    /// <summary>
    /// <c>toStartOfQuarter(column)</c>.
    /// </summary>
    Quarter,

    /// <summary>
    /// <c>toYear(column)</c>.
    /// </summary>
    Year,
}
