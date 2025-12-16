using System.Collections.Concurrent;
using System.Linq.Expressions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;

namespace EF.CH.Dictionaries;

/// <summary>
/// Provides typed access to a ClickHouse dictionary.
/// </summary>
/// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
/// <typeparam name="TKey">The dictionary key type.</typeparam>
/// <remarks>
/// This class provides methods for querying dictionary data that translate to
/// ClickHouse dictGet functions in LINQ queries. When used in a LINQ expression,
/// methods like <see cref="Get{TAttribute}"/> are translated to <c>dictGet()</c> SQL.
/// </remarks>
public sealed class ClickHouseDictionary<TDictionary, TKey>
    where TDictionary : class
{
    private static DictionaryMetadata<TDictionary, TKey>? _cachedMetadata;

    private readonly DbContext _context;
    private readonly DictionaryMetadata<TDictionary, TKey> _metadata;

    /// <summary>
    /// Creates a new dictionary accessor with automatic metadata discovery.
    /// </summary>
    /// <param name="context">The DbContext instance.</param>
    /// <remarks>
    /// Metadata is resolved from EF Core model annotations configured via AsDictionary&lt;&gt;() in OnModelCreating.
    /// The metadata is cached statically for performance.
    /// </remarks>
    public ClickHouseDictionary(DbContext context)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _metadata = ResolveMetadataFromModel(context);
    }

    /// <summary>
    /// Creates a new dictionary accessor with explicit metadata.
    /// </summary>
    /// <param name="context">The DbContext instance.</param>
    /// <param name="metadata">The dictionary metadata.</param>
    public ClickHouseDictionary(DbContext context, DictionaryMetadata<TDictionary, TKey> metadata)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
    }

    /// <summary>
    /// Gets the dictionary name.
    /// </summary>
    public string Name => _metadata.Name;

    /// <summary>
    /// Gets a single attribute value from the dictionary.
    /// </summary>
    /// <typeparam name="TAttribute">The attribute type.</typeparam>
    /// <param name="key">The dictionary key.</param>
    /// <param name="attribute">Expression selecting the attribute.</param>
    /// <returns>The attribute value.</returns>
    /// <remarks>
    /// When used in LINQ queries, this translates to:
    /// <code>dictGet('dictionary_name', 'AttributeName', key)</code>
    /// </remarks>
    /// <example>
    /// <code>
    /// var countryName = db.CountryLookup.Get(countryId, c => c.Name);
    ///
    /// // In LINQ:
    /// var orders = db.Orders
    ///     .Select(o => new {
    ///         o.Id,
    ///         Country = db.CountryLookup.Get(o.CountryId, c => c.Name)
    ///     });
    /// </code>
    /// </example>
    public TAttribute Get<TAttribute>(TKey key, Expression<Func<TDictionary, TAttribute>> attribute)
    {
        // This method is intended to be used in LINQ expressions and translated to SQL.
        // Direct execution is not supported - use GetAsync for direct calls.
        throw new InvalidOperationException(
            "This method is intended for use in LINQ expressions. " +
            "For direct dictionary access, use GetAsync instead.");
    }

    /// <summary>
    /// Gets a single attribute value from the dictionary, or a default value if the key doesn't exist.
    /// </summary>
    /// <typeparam name="TAttribute">The attribute type.</typeparam>
    /// <param name="key">The dictionary key.</param>
    /// <param name="attribute">Expression selecting the attribute.</param>
    /// <param name="defaultValue">The default value if key not found.</param>
    /// <returns>The attribute value or default.</returns>
    /// <remarks>
    /// When used in LINQ queries, this translates to:
    /// <code>dictGetOrDefault('dictionary_name', 'AttributeName', key, defaultValue)</code>
    /// </remarks>
    public TAttribute GetOrDefault<TAttribute>(
        TKey key,
        Expression<Func<TDictionary, TAttribute>> attribute,
        TAttribute defaultValue)
    {
        // This method is intended to be used in LINQ expressions and translated to SQL.
        throw new InvalidOperationException(
            "This method is intended for use in LINQ expressions. " +
            "For direct dictionary access, use GetOrDefaultAsync instead.");
    }

    /// <summary>
    /// Checks if a key exists in the dictionary.
    /// </summary>
    /// <param name="key">The dictionary key.</param>
    /// <returns>True if the key exists.</returns>
    /// <remarks>
    /// When used in LINQ queries, this translates to:
    /// <code>dictHas('dictionary_name', key)</code>
    /// </remarks>
    public bool ContainsKey(TKey key)
    {
        // This method is intended to be used in LINQ expressions and translated to SQL.
        throw new InvalidOperationException(
            "This method is intended for use in LINQ expressions. " +
            "For direct dictionary access, use ContainsKeyAsync instead.");
    }

    /// <summary>
    /// Asynchronously gets a single attribute value from the dictionary.
    /// </summary>
    /// <typeparam name="TAttribute">The attribute type.</typeparam>
    /// <param name="key">The dictionary key.</param>
    /// <param name="attribute">Expression selecting the attribute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The attribute value.</returns>
    public async Task<TAttribute> GetAsync<TAttribute>(
        TKey key,
        Expression<Func<TDictionary, TAttribute>> attribute,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(attribute);

        var propertyName = GetPropertyName(attribute);
        var sql = $"SELECT dictGet('{EscapeSqlString(_metadata.Name)}', '{EscapeSqlString(propertyName)}', {{0}})";

        var result = await _context.Database
            .SqlQueryRaw<TAttribute>(sql, key!)
            .FirstOrDefaultAsync(cancellationToken);

        return result!;
    }

    /// <summary>
    /// Asynchronously gets a single attribute value from the dictionary, or a default value.
    /// </summary>
    /// <typeparam name="TAttribute">The attribute type.</typeparam>
    /// <param name="key">The dictionary key.</param>
    /// <param name="attribute">Expression selecting the attribute.</param>
    /// <param name="defaultValue">The default value if key not found.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The attribute value or default.</returns>
    public async Task<TAttribute> GetOrDefaultAsync<TAttribute>(
        TKey key,
        Expression<Func<TDictionary, TAttribute>> attribute,
        TAttribute defaultValue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(attribute);

        var propertyName = GetPropertyName(attribute);
        var sql = $"SELECT dictGetOrDefault('{EscapeSqlString(_metadata.Name)}', '{EscapeSqlString(propertyName)}', {{0}}, {{1}})";

        var result = await _context.Database
            .SqlQueryRaw<TAttribute>(sql, key!, defaultValue!)
            .FirstOrDefaultAsync(cancellationToken);

        return result!;
    }

    /// <summary>
    /// Asynchronously checks if a key exists in the dictionary.
    /// </summary>
    /// <param name="key">The dictionary key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the key exists.</returns>
    public async Task<bool> ContainsKeyAsync(TKey key, CancellationToken cancellationToken = default)
    {
        var sql = $"SELECT dictHas('{EscapeSqlString(_metadata.Name)}', {{0}})";

        var result = await _context.Database
            .SqlQueryRaw<byte>(sql, key!)
            .FirstOrDefaultAsync(cancellationToken);

        return result == 1;
    }

    /// <summary>
    /// Forces a refresh of the dictionary data.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task RefreshAsync(CancellationToken cancellationToken = default)
    {
        var sql = $"SYSTEM RELOAD DICTIONARY `{EscapeIdentifier(_metadata.Name)}`";
        await _context.Database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    /// <summary>
    /// Gets the dictionary status from system.dictionaries.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The dictionary status information.</returns>
    public async Task<DictionaryStatus?> GetStatusAsync(CancellationToken cancellationToken = default)
    {
        var sql = """
            SELECT
                status,
                element_count,
                bytes_allocated,
                last_successful_update_time,
                last_exception
            FROM system.dictionaries
            WHERE name = {0}
            LIMIT 1
            """;

        return await _context.Database
            .SqlQueryRaw<DictionaryStatus>(sql, _metadata.Name)
            .FirstOrDefaultAsync(cancellationToken);
    }

    private static string GetPropertyName<TAttribute>(Expression<Func<TDictionary, TAttribute>> expression)
    {
        if (expression.Body is MemberExpression member)
        {
            return member.Member.Name;
        }

        throw new ArgumentException("Expression must be a simple member access expression.", nameof(expression));
    }

    private static DictionaryMetadata<TDictionary, TKey> ResolveMetadataFromModel(DbContext context)
    {
        // Fast path: return cached metadata if available (volatile read)
        var cached = Volatile.Read(ref _cachedMetadata);
        if (cached != null)
            return cached;

        // Slow path: resolve from model (may run concurrently on first access)
        var entityType = context.Model.FindEntityType(typeof(TDictionary))
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TDictionary).Name}' is not configured in OnModelCreating.");

        var isDictionary = entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value as bool? ?? false;
        if (!isDictionary)
            throw new InvalidOperationException(
                $"Entity type '{typeof(TDictionary).Name}' is not configured as a dictionary. " +
                "Use AsDictionary<>() in OnModelCreating.");

        var name = entityType.GetTableName() ?? ConvertToSnakeCase(typeof(TDictionary).Name);
        var keyColumns = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns)?.Value as string[]
            ?? throw new InvalidOperationException(
                $"Dictionary '{typeof(TDictionary).Name}' key not configured. " +
                "Use HasKey() or HasCompositeKey() in AsDictionary<>() configuration.");

        var newMetadata = new DictionaryMetadata<TDictionary, TKey>(
            name: name,
            keyType: typeof(TKey),
            entityType: typeof(TDictionary),
            keyPropertyName: keyColumns[0]);

        // Thread-safe assignment: first writer wins, but all threads get valid metadata
        Interlocked.CompareExchange(ref _cachedMetadata, newMetadata, null);
        return _cachedMetadata!;
    }

    private static string ConvertToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var result = new System.Text.StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                    result.Append('_');
                result.Append(char.ToLowerInvariant(c));
            }
            else
            {
                result.Append(c);
            }
        }
        return result.ToString();
    }

    /// <summary>
    /// Escapes a string for use in SQL string literals by doubling single quotes.
    /// </summary>
    private static string EscapeSqlString(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        return value.Replace("'", "''");
    }

    /// <summary>
    /// Escapes a string for use as a ClickHouse identifier by escaping backticks.
    /// </summary>
    private static string EscapeIdentifier(string value)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        return value.Replace("`", "``");
    }
}

/// <summary>
/// Dictionary status information from system.dictionaries.
/// </summary>
public record DictionaryStatus(
    string Status,
    ulong ElementCount,
    ulong BytesAllocated,
    DateTime LastSuccessfulUpdateTime,
    string? LastException);
