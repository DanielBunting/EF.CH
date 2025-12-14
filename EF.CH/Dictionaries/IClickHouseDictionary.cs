namespace EF.CH.Dictionaries;

/// <summary>
/// Marker interface for ClickHouse dictionary entities.
/// Implement this interface on entity classes that represent ClickHouse dictionaries.
/// </summary>
/// <remarks>
/// Dictionary entities are configured using the <c>AsDictionary</c> fluent API
/// in <c>OnModelCreating</c>. This interface serves as a compile-time marker
/// for the source generator and provides type safety.
/// </remarks>
/// <example>
/// <code>
/// public class CountryLookup : IClickHouseDictionary
/// {
///     public ulong Id { get; set; }
///     public string Name { get; set; } = string.Empty;
///     public string IsoCode { get; set; } = string.Empty;
/// }
/// </code>
/// </example>
public interface IClickHouseDictionary;
