namespace EF.CH.External;

/// <summary>
/// Service interface for resolving external keys to local IDs.
/// Used for caching key-to-ID mappings from external data sources (like PostgreSQL)
/// in ClickHouse dictionaries for fast lookups.
/// </summary>
/// <typeparam name="TKey">The external key type (e.g., string, Guid).</typeparam>
/// <typeparam name="TId">The local ID type (e.g., uint, ulong).</typeparam>
public interface IExternalKeyIdResolver<TKey, TId>
    where TKey : notnull
    where TId : struct
{
    /// <summary>
    /// Gets the local ID for an external key, creating a new mapping if one doesn't exist.
    /// </summary>
    /// <param name="key">The external key to resolve.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The local ID for the key.</returns>
    /// <exception cref="InvalidOperationException">If the key cannot be resolved or created.</exception>
    Task<TId> GetOrCreateIdAsync(TKey key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Tries to get the local ID for an external key without creating a new mapping.
    /// </summary>
    /// <param name="key">The external key to look up.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The local ID if found, or null if the key is not mapped.</returns>
    Task<TId?> TryGetIdAsync(TKey key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Forces a reload of the dictionary from the mapping table.
    /// Call this after bulk inserts to the mapping table.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task ReloadDictionaryAsync(CancellationToken cancellationToken = default);
}
