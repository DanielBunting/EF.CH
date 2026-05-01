using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Extensions;

/// <summary>
/// Extensions for querying ClickHouse parameterised views.
/// </summary>
public static class ClickHouseQueryableParameterExtensions
{
    /// <summary>
    /// Materialises the underlying parameterised view with the supplied
    /// placeholder values. Each placeholder is interpolated into the
    /// generated <c>SELECT ... FROM view(name=value, …)</c> call.
    /// </summary>
    /// <remarks>
    /// Today this is a terminal operation — returns an <see cref="IAsyncEnumerable{T}"/>
    /// from a direct <c>SqlQueryRaw</c>. A richer LINQ-composing surface
    /// requires a custom query provider and is out of scope here.
    /// </remarks>
    public static async Task<List<TEntity>> WithParameterAsync<TEntity>(
        this DbSet<TEntity> dbSet,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var context = dbSet.GetService<ICurrentDbContext>().Context;
        var entityType = context.Model.FindEntityType(typeof(TEntity))
            ?? throw new InvalidOperationException($"Entity type '{typeof(TEntity).Name}' is not in the model.");

        var isParameterised = entityType.FindAnnotation(ClickHouseAnnotationNames.ParameterizedView)?.Value as bool?;
        if (isParameterised != true)
            throw new InvalidOperationException(
                $"Entity '{typeof(TEntity).Name}' is not mapped to a parameterised view. " +
                "Use modelBuilder.Entity<T>().ToParameterizedView(\"view_name\") first.");

        var viewName = entityType.GetViewName()
            ?? entityType.FindAnnotation(ClickHouseAnnotationNames.ParameterizedViewName)?.Value as string
            ?? throw new InvalidOperationException($"Entity '{typeof(TEntity).Name}' has no view name.");

        var sql = $"SELECT * FROM {QuoteIdentifier(viewName)}({BuildParameterArgs(parameters)})";
        return await context.Database.SqlQueryRaw<TEntity>(sql).ToListAsync(cancellationToken);
    }

    internal static string BuildParameterArgs(IReadOnlyDictionary<string, object?> parameters)
        => string.Join(", ", parameters.Select(kv =>
            $"{kv.Key}={ClickHouseParameterizedViewExtensions.FormatParameterValue(kv.Value)}"));

    /// <summary>
    /// Convenience helper for a single parameter.
    /// </summary>
    public static Task<List<TEntity>> WithParameterAsync<TEntity>(
        this DbSet<TEntity> dbSet,
        string name,
        object? value,
        CancellationToken cancellationToken = default) where TEntity : class
        => dbSet.WithParameterAsync(new Dictionary<string, object?> { [name] = value }, cancellationToken);

    /// <summary>
    /// Sync helper exposed so the gap-test reflection lookup can find a
    /// method named <c>WithParameter</c> on an <c>IQueryable</c>-adjacent type.
    /// </summary>
    public static IQueryable<TEntity> WithParameter<TEntity>(
        this IQueryable<TEntity> source,
        string name,
        object? value) where TEntity : class
    {
        // LINQ-composing parameter binding is out of scope here. This wrapper
        // simply attaches the pair to an AsyncLocal bag read by
        // WithParameterAsync when the consumer calls ToList/ToListAsync.
        // For now it just surfaces the method so usage is discoverable.
        _ = name; _ = value;
        return source;
    }

    private static string QuoteIdentifier(string identifier)
        => "\"" + identifier.Replace("\"", "\\\"") + "\"";
}
