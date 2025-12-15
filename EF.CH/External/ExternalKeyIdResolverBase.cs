using System.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace EF.CH.External;

/// <summary>
/// Base implementation for external key-to-ID resolver service.
/// Uses a ClickHouse dictionary for fast lookups and a ReplacingMergeTree table for persistence.
/// </summary>
/// <typeparam name="TKey">The external key type.</typeparam>
/// <typeparam name="TId">The local ID type.</typeparam>
/// <typeparam name="TContext">The DbContext type.</typeparam>
public abstract class ExternalKeyIdResolverBase<TKey, TId, TContext> : IExternalKeyIdResolver<TKey, TId>
    where TKey : notnull
    where TId : struct
    where TContext : DbContext
{
    private readonly TContext _context;
    private readonly ExternalKeyIdResolverOptions _options;

    /// <summary>
    /// Creates a new resolver instance.
    /// </summary>
    /// <param name="context">The DbContext for database operations.</param>
    /// <param name="options">The resolver configuration options.</param>
    protected ExternalKeyIdResolverBase(TContext context, ExternalKeyIdResolverOptions options)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Gets the options for this resolver.
    /// </summary>
    protected ExternalKeyIdResolverOptions Options => _options;

    /// <summary>
    /// Gets the DbContext for database operations.
    /// </summary>
    protected TContext Context => _context;

    /// <inheritdoc />
    public virtual async Task<TId?> TryGetIdAsync(TKey key, CancellationToken cancellationToken = default)
    {
        // Use dictHas to check if key exists
        var hasSql = $"SELECT dictHas('{_options.DictionaryName}', {{key:{GetClickHouseKeyType()}}})";
        var connection = _context.Database.GetDbConnection();

        await connection.OpenAsync(cancellationToken);
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = hasSql;
            AddKeyParameter(command, key);

            var hasResult = await command.ExecuteScalarAsync(cancellationToken);
            if (hasResult is not (byte or sbyte or short or ushort or int or uint or long or ulong) ||
                Convert.ToInt64(hasResult) == 0)
            {
                return null;
            }

            // Key exists, get the ID using dictGet
            var getSql = $"SELECT dictGet('{_options.DictionaryName}', '{_options.IdColumnName}', {{key:{GetClickHouseKeyType()}}})";

            await using var getCommand = connection.CreateCommand();
            getCommand.CommandText = getSql;
            AddKeyParameter(getCommand, key);

            var result = await getCommand.ExecuteScalarAsync(cancellationToken);
            return ConvertToId(result);
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    /// <inheritdoc />
    public virtual async Task<TId> GetOrCreateIdAsync(TKey key, CancellationToken cancellationToken = default)
    {
        // Fast path: try to get from dictionary
        var existingId = await TryGetIdAsync(key, cancellationToken);
        if (existingId.HasValue)
        {
            return existingId.Value;
        }

        // Slow path: key not in dictionary
        // 1. Query external source if enabled
        // 2. Insert into mapping table
        // 3. Reload dictionary
        // 4. Return the new ID

        var newId = await CreateMappingAsync(key, cancellationToken);

        // Reload dictionary with retry
        for (var attempt = 0; attempt <= _options.MaxRetries; attempt++)
        {
            await ReloadDictionaryAsync(cancellationToken);

            // Verify the ID is now in the dictionary
            var verifiedId = await TryGetIdAsync(key, cancellationToken);
            if (verifiedId.HasValue)
            {
                return verifiedId.Value;
            }

            if (attempt < _options.MaxRetries)
            {
                await Task.Delay(_options.RetryDelay, cancellationToken);
            }
        }

        // If we still can't get it from the dictionary, return the ID we created
        // This handles edge cases where the dictionary reload is slow
        return newId;
    }

    /// <inheritdoc />
    public virtual async Task ReloadDictionaryAsync(CancellationToken cancellationToken = default)
    {
        var sql = $"SYSTEM RELOAD DICTIONARY '{_options.DictionaryName}'";
        await _context.Database.ExecuteSqlRawAsync(sql, cancellationToken);
    }

    /// <summary>
    /// Creates a new mapping for the given key.
    /// Override this method to customize ID generation or to integrate with external sources.
    /// </summary>
    /// <param name="key">The external key to create a mapping for.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The newly created ID.</returns>
    protected virtual async Task<TId> CreateMappingAsync(TKey key, CancellationToken cancellationToken)
    {
        // Generate new ID (override this method for custom ID generation)
        var newId = await GenerateNewIdAsync(cancellationToken);

        // Insert into mapping table
        var insertSql = $@"
            INSERT INTO {_options.MappingTableDatabase}.{_options.MappingTableName}
            ({_options.KeyColumnName}, {_options.IdColumnName})
            VALUES ({{key:{GetClickHouseKeyType()}}}, {{id:{_options.IdType}}})";

        var connection = _context.Database.GetDbConnection();
        await connection.OpenAsync(cancellationToken);
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = insertSql;
            AddKeyParameter(command, key);
            AddIdParameter(command, newId);

            await command.ExecuteNonQueryAsync(cancellationToken);
            return newId;
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    /// <summary>
    /// Generates a new unique ID.
    /// Default implementation queries MAX(id) + 1 from the mapping table.
    /// Override for custom ID generation strategies.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A new unique ID.</returns>
    protected virtual async Task<TId> GenerateNewIdAsync(CancellationToken cancellationToken)
    {
        var sql = $"SELECT coalesce(max({_options.IdColumnName}), 0) + 1 FROM {_options.MappingTableDatabase}.{_options.MappingTableName}";

        var connection = _context.Database.GetDbConnection();
        await connection.OpenAsync(cancellationToken);
        try
        {
            await using var command = connection.CreateCommand();
            command.CommandText = sql;

            var result = await command.ExecuteScalarAsync(cancellationToken);
            return ConvertToId(result)
                ?? throw new InvalidOperationException("Failed to generate new ID");
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    /// <summary>
    /// Gets the ClickHouse type name for the key type.
    /// Override for custom key type mappings.
    /// </summary>
    /// <returns>The ClickHouse type name.</returns>
    protected virtual string GetClickHouseKeyType()
    {
        return typeof(TKey) switch
        {
            var t when t == typeof(string) => "String",
            var t when t == typeof(Guid) => "UUID",
            var t when t == typeof(int) => "Int32",
            var t when t == typeof(uint) => "UInt32",
            var t when t == typeof(long) => "Int64",
            var t when t == typeof(ulong) => "UInt64",
            _ => throw new NotSupportedException($"Key type {typeof(TKey).Name} is not supported")
        };
    }

    /// <summary>
    /// Adds a key parameter to a command.
    /// Override for custom parameter handling.
    /// </summary>
    /// <param name="command">The database command.</param>
    /// <param name="key">The key value.</param>
    protected virtual void AddKeyParameter(DbCommand command, TKey key)
    {
        var param = command.CreateParameter();
        param.ParameterName = "key";
        param.Value = key;
        command.Parameters.Add(param);
    }

    /// <summary>
    /// Adds an ID parameter to a command.
    /// Override for custom parameter handling.
    /// </summary>
    /// <param name="command">The database command.</param>
    /// <param name="id">The ID value.</param>
    protected virtual void AddIdParameter(DbCommand command, TId id)
    {
        var param = command.CreateParameter();
        param.ParameterName = "id";
        param.Value = id;
        command.Parameters.Add(param);
    }

    /// <summary>
    /// Converts a database result to the ID type.
    /// Override for custom ID type conversions.
    /// </summary>
    /// <param name="value">The database value.</param>
    /// <returns>The converted ID, or null if conversion fails.</returns>
    protected virtual TId? ConvertToId(object? value)
    {
        if (value is null or DBNull)
        {
            return null;
        }

        try
        {
            return (TId)Convert.ChangeType(value, typeof(TId));
        }
        catch
        {
            return null;
        }
    }
}
