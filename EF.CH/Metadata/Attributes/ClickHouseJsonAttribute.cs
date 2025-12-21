namespace EF.CH.Metadata.Attributes;

/// <summary>
/// Configures a property as a native ClickHouse JSON column.
/// </summary>
/// <remarks>
/// <para>
/// Requires ClickHouse 24.8 or later for native JSON type support.
/// </para>
/// <para>
/// The native JSON type stores data with automatic subcolumn extraction,
/// allowing efficient path-based queries using subcolumn syntax.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// public class Event
/// {
///     public Guid Id { get; set; }
///
///     [ClickHouseJson(MaxDynamicPaths = 1024)]
///     public JsonElement Metadata { get; set; }
///
///     // Typed JSON with POCO mapping
///     [ClickHouseJson(MaxDynamicPaths = 256, MaxDynamicTypes = 16)]
///     public OrderMetadata OrderData { get; set; }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public class ClickHouseJsonAttribute : Attribute
{
    /// <summary>
    /// Maximum number of paths stored as subcolumns.
    /// </summary>
    /// <remarks>
    /// When -1, uses ClickHouse default (1024).
    /// Increase for JSON with many distinct paths; decrease to limit subcolumn bloat.
    /// </remarks>
    public int MaxDynamicPaths { get; set; } = -1;

    /// <summary>
    /// Maximum number of different types per path.
    /// </summary>
    /// <remarks>
    /// When -1, uses ClickHouse default (32).
    /// Increase if paths have highly variable types.
    /// </remarks>
    public int MaxDynamicTypes { get; set; } = -1;

    /// <summary>
    /// Whether this JSON column represents a typed POCO for navigation support.
    /// </summary>
    /// <remarks>
    /// When true, property navigation on the CLR type will be translated
    /// to ClickHouse subcolumn access in LINQ queries.
    /// </remarks>
    public bool IsTyped { get; set; }
}
