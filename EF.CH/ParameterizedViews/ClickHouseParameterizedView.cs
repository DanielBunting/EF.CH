using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;

namespace EF.CH.ParameterizedViews;

/// <summary>
/// Provides typed access to a ClickHouse parameterized view.
/// </summary>
/// <typeparam name="TResult">The view result entity type.</typeparam>
/// <remarks>
/// <para>
/// This class provides a strongly-typed way to query parameterized views from a DbContext,
/// following the same pattern as <see cref="Dictionaries.ClickHouseDictionary{TDictionary, TKey}"/>.
/// </para>
/// <para>
/// Usage in DbContext:
/// <code>
/// public class AnalyticsDbContext : DbContext
/// {
///     private ClickHouseParameterizedView&lt;UserEventView&gt;? _userEventsView;
///
///     public ClickHouseParameterizedView&lt;UserEventView&gt; UserEventsView
///         => _userEventsView ??= new ClickHouseParameterizedView&lt;UserEventView&gt;(this);
///
///     protected override void OnModelCreating(ModelBuilder modelBuilder)
///     {
///         modelBuilder.Entity&lt;UserEventView&gt;(e => e.HasParameterizedView("user_events_view"));
///     }
/// }
///
/// // Query usage:
/// var events = await db.UserEventsView
///     .Query(new { user_id = 123UL, start_date = DateTime.Now.AddDays(-7) })
///     .Where(e => e.EventType == "purchase")
///     .ToListAsync();
/// </code>
/// </para>
/// </remarks>
public sealed class ClickHouseParameterizedView<TResult>
    where TResult : class
{
    private static ParameterizedViewMetadata<TResult>? _cachedMetadata;

    private readonly DbContext _context;
    private readonly ParameterizedViewMetadata<TResult> _metadata;

    /// <summary>
    /// Creates a new parameterized view accessor with automatic metadata discovery.
    /// </summary>
    /// <param name="context">The DbContext instance.</param>
    /// <remarks>
    /// Metadata is resolved from EF Core model annotations configured via
    /// HasParameterizedView() in OnModelCreating.
    /// The metadata is cached statically for performance.
    /// </remarks>
    public ClickHouseParameterizedView(DbContext context)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _metadata = ResolveMetadataFromModel(context);
    }

    /// <summary>
    /// Creates a new parameterized view accessor with explicit metadata.
    /// </summary>
    /// <param name="context">The DbContext instance.</param>
    /// <param name="metadata">The view metadata.</param>
    public ClickHouseParameterizedView(DbContext context, ParameterizedViewMetadata<TResult> metadata)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
    }

    /// <summary>
    /// Gets the view name in ClickHouse.
    /// </summary>
    public string ViewName => _metadata.ViewName;

    /// <summary>
    /// Queries the parameterized view with the specified parameters.
    /// </summary>
    /// <param name="parameters">
    /// An anonymous object containing parameter names and values.
    /// Property names should match the parameter names in the view definition.
    /// </param>
    /// <returns>An IQueryable that can be further composed with LINQ.</returns>
    /// <example>
    /// <code>
    /// var events = db.UserEventsView
    ///     .Query(new { user_id = 123UL, start_date = DateTime.Now.AddDays(-7) })
    ///     .Where(e => e.EventType == "purchase")
    ///     .OrderByDescending(e => e.Timestamp)
    ///     .ToListAsync();
    /// </code>
    /// </example>
    public IQueryable<TResult> Query(object parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        return _context.FromParameterizedView<TResult>(ViewName, parameters);
    }

    /// <summary>
    /// Queries the parameterized view with dictionary parameters.
    /// </summary>
    /// <param name="parameters">A dictionary containing parameter names and values.</param>
    /// <returns>An IQueryable that can be further composed with LINQ.</returns>
    /// <example>
    /// <code>
    /// var parameters = new Dictionary&lt;string, object?&gt;
    /// {
    ///     ["user_id"] = 123UL,
    ///     ["start_date"] = DateTime.Now.AddDays(-7)
    /// };
    /// var events = db.UserEventsView
    ///     .Query(parameters)
    ///     .ToListAsync();
    /// </code>
    /// </example>
    public IQueryable<TResult> Query(IDictionary<string, object?> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        return _context.FromParameterizedView<TResult>(ViewName, parameters);
    }

    /// <summary>
    /// Convenience method to query and return all results as a list.
    /// </summary>
    /// <param name="parameters">The view parameters.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A list of results from the view.</returns>
    public Task<List<TResult>> ToListAsync(object parameters, CancellationToken cancellationToken = default)
    {
        return Query(parameters).ToListAsync(cancellationToken);
    }

    /// <summary>
    /// Convenience method to query and return all results as a list.
    /// </summary>
    /// <param name="parameters">The view parameters as dictionary.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A list of results from the view.</returns>
    public Task<List<TResult>> ToListAsync(IDictionary<string, object?> parameters, CancellationToken cancellationToken = default)
    {
        return Query(parameters).ToListAsync(cancellationToken);
    }

    /// <summary>
    /// Convenience method to query and return the first result or default.
    /// </summary>
    /// <param name="parameters">The view parameters.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The first result or null if empty.</returns>
    public Task<TResult?> FirstOrDefaultAsync(object parameters, CancellationToken cancellationToken = default)
    {
        return Query(parameters).FirstOrDefaultAsync(cancellationToken);
    }

    /// <summary>
    /// Convenience method to query and return the first result or default.
    /// </summary>
    /// <param name="parameters">The view parameters as dictionary.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The first result or null if empty.</returns>
    public Task<TResult?> FirstOrDefaultAsync(IDictionary<string, object?> parameters, CancellationToken cancellationToken = default)
    {
        return Query(parameters).FirstOrDefaultAsync(cancellationToken);
    }

    /// <summary>
    /// Convenience method to query and return the single result or default.
    /// </summary>
    /// <param name="parameters">The view parameters.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The single result or null if empty.</returns>
    /// <exception cref="InvalidOperationException">More than one result was returned.</exception>
    public Task<TResult?> SingleOrDefaultAsync(object parameters, CancellationToken cancellationToken = default)
    {
        return Query(parameters).SingleOrDefaultAsync(cancellationToken);
    }

    /// <summary>
    /// Convenience method to get the count of results.
    /// </summary>
    /// <param name="parameters">The view parameters.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The count of results.</returns>
    public Task<int> CountAsync(object parameters, CancellationToken cancellationToken = default)
    {
        return Query(parameters).CountAsync(cancellationToken);
    }

    /// <summary>
    /// Convenience method to check if any results exist.
    /// </summary>
    /// <param name="parameters">The view parameters.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>True if any results exist.</returns>
    public Task<bool> AnyAsync(object parameters, CancellationToken cancellationToken = default)
    {
        return Query(parameters).AnyAsync(cancellationToken);
    }

    private static ParameterizedViewMetadata<TResult> ResolveMetadataFromModel(DbContext context)
    {
        // Fast path: return cached metadata if available (volatile read)
        var cached = Volatile.Read(ref _cachedMetadata);
        if (cached != null)
            return cached;

        // Slow path: resolve from model (may run concurrently on first access)
        var entityType = context.Model.FindEntityType(typeof(TResult))
            ?? throw new InvalidOperationException(
                $"Entity type '{typeof(TResult).Name}' is not configured in OnModelCreating.");

        var isParameterizedView = entityType.FindAnnotation(ClickHouseAnnotationNames.ParameterizedView)?.Value as bool? ?? false;
        if (!isParameterizedView)
            throw new InvalidOperationException(
                $"Entity type '{typeof(TResult).Name}' is not configured as a parameterized view. " +
                "Use HasParameterizedView() in OnModelCreating.");

        var viewName = entityType.FindAnnotation(ClickHouseAnnotationNames.ParameterizedViewName)?.Value as string
            ?? throw new InvalidOperationException(
                $"Parameterized view '{typeof(TResult).Name}' does not have a view name configured.");

        var newMetadata = new ParameterizedViewMetadata<TResult>(viewName, typeof(TResult));

        // Thread-safe assignment: first writer wins, but all threads get valid metadata
        Interlocked.CompareExchange(ref _cachedMetadata, newMetadata, null);
        return _cachedMetadata!;
    }
}
