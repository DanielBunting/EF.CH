using EF.CH.Dictionaries;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace EF.CH.Extensions;

/// <summary>
/// Extension methods for managing ClickHouse dictionaries at runtime.
/// </summary>
public static class DictionaryDbContextExtensions
{
    /// <summary>
    /// Ensures all dictionaries with external sources are created in ClickHouse.
    /// Resolves credentials at call time from environment variables or IConfiguration.
    /// Safe to call multiple times (uses CREATE DICTIONARY IF NOT EXISTS).
    /// </summary>
    /// <param name="context">The database context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of dictionaries created or verified.</returns>
    /// <remarks>
    /// <para>
    /// This method should be called at application startup for external dictionaries (PostgreSQL, MySQL, HTTP)
    /// since they are skipped during EF Core migrations to avoid storing credentials in migration files.
    /// </para>
    /// <para>
    /// ClickHouse-sourced dictionaries (using FromTable()) are created via migrations and don't need this method.
    /// </para>
    /// </remarks>
    /// <example>
    /// <code>
    /// // In Program.cs or startup
    /// await using var context = new MyDbContext(options);
    /// await context.EnsureDictionariesAsync();
    /// </code>
    /// </example>
    public static async Task<int> EnsureDictionariesAsync(
        this DbContext context,
        CancellationToken cancellationToken = default)
    {
        var resolver = GetOrCreateResolver(context);
        var count = 0;

        foreach (var entityType in context.Model.GetEntityTypes())
        {
            if (!resolver.IsDictionary(entityType))
                continue;

            // Only process external dictionaries - ClickHouse-sourced ones are handled by migrations
            if (!resolver.IsExternalDictionary(entityType))
                continue;

            var ddl = resolver.GenerateCreateDictionaryDdl(entityType, ifNotExists: true);
            await context.Database.ExecuteSqlRawAsync(ddl, cancellationToken);
            count++;
        }

        return count;
    }

    /// <summary>
    /// Ensures all dictionaries (including ClickHouse-sourced) are created in ClickHouse.
    /// Use this method when bypassing migrations entirely.
    /// </summary>
    /// <param name="context">The database context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of dictionaries created or verified.</returns>
    public static async Task<int> EnsureAllDictionariesAsync(
        this DbContext context,
        CancellationToken cancellationToken = default)
    {
        var resolver = GetOrCreateResolver(context);
        var count = 0;

        foreach (var entityType in context.Model.GetEntityTypes())
        {
            if (!resolver.IsDictionary(entityType))
                continue;

            var ddl = resolver.GenerateCreateDictionaryDdl(entityType, ifNotExists: true);
            await context.Database.ExecuteSqlRawAsync(ddl, cancellationToken);
            count++;
        }

        return count;
    }

    /// <summary>
    /// Recreates all external dictionaries (DROP + CREATE).
    /// Useful when dictionary schema or source configuration has changed.
    /// </summary>
    /// <param name="context">The database context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of dictionaries recreated.</returns>
    public static async Task<int> RecreateDictionariesAsync(
        this DbContext context,
        CancellationToken cancellationToken = default)
    {
        var resolver = GetOrCreateResolver(context);
        var count = 0;

        foreach (var entityType in context.Model.GetEntityTypes())
        {
            if (!resolver.IsDictionary(entityType))
                continue;

            if (!resolver.IsExternalDictionary(entityType))
                continue;

            // DROP IF EXISTS
            var dropDdl = resolver.GenerateDropDictionaryDdl(entityType, ifExists: true);
            await context.Database.ExecuteSqlRawAsync(dropDdl, cancellationToken);

            // CREATE
            var createDdl = resolver.GenerateCreateDictionaryDdl(entityType, ifNotExists: false);
            await context.Database.ExecuteSqlRawAsync(createDdl, cancellationToken);
            count++;
        }

        return count;
    }

    /// <summary>
    /// Reloads a specific dictionary from its source.
    /// </summary>
    /// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
    /// <param name="context">The database context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async Task ReloadDictionaryAsync<TDictionary>(
        this DbContext context,
        CancellationToken cancellationToken = default)
        where TDictionary : class
    {
        var entityType = context.Model.FindEntityType(typeof(TDictionary));
        if (entityType == null)
        {
            throw new InvalidOperationException(
                $"Entity type '{typeof(TDictionary).Name}' is not found in the model.");
        }

        var resolver = GetOrCreateResolver(context);
        if (!resolver.IsDictionary(entityType))
        {
            throw new InvalidOperationException(
                $"Entity type '{typeof(TDictionary).Name}' is not configured as a dictionary.");
        }

        var reloadDdl = resolver.GenerateReloadDictionaryDdl(entityType);
        await context.Database.ExecuteSqlRawAsync(reloadDdl, cancellationToken);
    }

    /// <summary>
    /// Reloads all dictionaries from their sources.
    /// </summary>
    /// <param name="context">The database context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of dictionaries reloaded.</returns>
    public static async Task<int> ReloadAllDictionariesAsync(
        this DbContext context,
        CancellationToken cancellationToken = default)
    {
        var resolver = GetOrCreateResolver(context);
        var count = 0;

        foreach (var entityType in context.Model.GetEntityTypes())
        {
            if (!resolver.IsDictionary(entityType))
                continue;

            var reloadDdl = resolver.GenerateReloadDictionaryDdl(entityType);
            await context.Database.ExecuteSqlRawAsync(reloadDdl, cancellationToken);
            count++;
        }

        return count;
    }

    /// <summary>
    /// Gets the DDL for a specific dictionary without executing it.
    /// Useful for debugging or manual execution.
    /// </summary>
    /// <typeparam name="TDictionary">The dictionary entity type.</typeparam>
    /// <param name="context">The database context.</param>
    /// <returns>The CREATE DICTIONARY DDL statement.</returns>
    public static string GetDictionaryDdl<TDictionary>(this DbContext context)
        where TDictionary : class
    {
        var entityType = context.Model.FindEntityType(typeof(TDictionary));
        if (entityType == null)
        {
            throw new InvalidOperationException(
                $"Entity type '{typeof(TDictionary).Name}' is not found in the model.");
        }

        var resolver = GetOrCreateResolver(context);
        if (!resolver.IsDictionary(entityType))
        {
            throw new InvalidOperationException(
                $"Entity type '{typeof(TDictionary).Name}' is not configured as a dictionary.");
        }

        return resolver.GenerateCreateDictionaryDdl(entityType, ifNotExists: true);
    }

    /// <summary>
    /// Gets all dictionary DDL statements without executing them.
    /// </summary>
    /// <param name="context">The database context.</param>
    /// <param name="externalOnly">If true, only returns DDL for external dictionaries.</param>
    /// <returns>Dictionary DDL statements keyed by entity type name.</returns>
    public static Dictionary<string, string> GetAllDictionaryDdl(
        this DbContext context,
        bool externalOnly = true)
    {
        var resolver = GetOrCreateResolver(context);
        var result = new Dictionary<string, string>();

        foreach (var entityType in context.Model.GetEntityTypes())
        {
            if (!resolver.IsDictionary(entityType))
                continue;

            if (externalOnly && !resolver.IsExternalDictionary(entityType))
                continue;

            var ddl = resolver.GenerateCreateDictionaryDdl(entityType, ifNotExists: true);
            result[entityType.ClrType.Name] = ddl;
        }

        return result;
    }

    private static IDictionaryConfigResolver GetOrCreateResolver(DbContext context)
    {
        // Try to get from DI first
        var resolver = context.GetService<IDictionaryConfigResolver>();
        if (resolver != null)
            return resolver;

        // Fall back to creating one with available IConfiguration
        var configuration = context.GetService<IConfiguration>();
        return new DictionaryConfigResolver(configuration);
    }
}
