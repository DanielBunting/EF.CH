namespace EF.CH.TempTable;

/// <summary>
/// Manages multiple temporary tables with LIFO disposal order.
/// All tables are dropped when the scope is disposed.
/// </summary>
public sealed class TempTableScope : IAsyncDisposable
{
    private readonly IClickHouseTempTableManager _manager;
    private readonly List<IAsyncDisposable> _handles = new();
    private bool _disposed;

    internal TempTableScope(IClickHouseTempTableManager manager)
    {
        _manager = manager;
    }

    /// <summary>
    /// Creates an empty temporary table within this scope.
    /// </summary>
    public async Task<TempTableHandle<T>> CreateAsync<T>(string? tableName = null, CancellationToken cancellationToken = default) where T : class
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var handle = await _manager.CreateAsync<T>(tableName, cancellationToken);
        _handles.Add(handle);
        return handle;
    }

    /// <summary>
    /// Creates a temporary table populated from a query within this scope.
    /// </summary>
    public async Task<TempTableHandle<T>> CreateFromQueryAsync<T>(IQueryable<T> sourceQuery, string? tableName = null, CancellationToken cancellationToken = default) where T : class
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var handle = await _manager.CreateFromQueryAsync<T>(sourceQuery, tableName, cancellationToken);
        _handles.Add(handle);
        return handle;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Dispose in LIFO order
        for (var i = _handles.Count - 1; i >= 0; i--)
        {
            await _handles[i].DisposeAsync();
        }

        _handles.Clear();
    }
}
