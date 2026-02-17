using System.Data.Common;
using System.Diagnostics;
using ClickHouse.Driver.ADO;
using EF.CH.BulkInsert.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.BulkInsert;

/// <summary>
/// Implementation of bulk insert operations for ClickHouse.
/// Bypasses EF Core change tracking for maximum insert performance.
/// </summary>
public sealed class ClickHouseBulkInserter : IClickHouseBulkInserter
{
    private readonly ICurrentDbContext _currentDbContext;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ISqlGenerationHelper _sqlGenerationHelper;
    private readonly IRelationalConnection _relationalConnection;
    private readonly EntityPropertyCache _propertyCache;
    private readonly BulkInsertSqlBuilder _valuesSqlBuilder;
    private readonly JsonEachRowBuilder _jsonBuilder;

    public ClickHouseBulkInserter(
        ICurrentDbContext currentDbContext,
        IRelationalTypeMappingSource typeMappingSource,
        ISqlGenerationHelper sqlGenerationHelper,
        IRelationalConnection relationalConnection)
    {
        _currentDbContext = currentDbContext;
        _typeMappingSource = typeMappingSource;
        _sqlGenerationHelper = sqlGenerationHelper;
        _relationalConnection = relationalConnection;

        _propertyCache = new EntityPropertyCache(
            typeMappingSource,
            sqlGenerationHelper,
            currentDbContext.Context.Model);

        _valuesSqlBuilder = new BulkInsertSqlBuilder(typeMappingSource);
        _jsonBuilder = new JsonEachRowBuilder();
    }

    /// <inheritdoc />
    public async Task<ClickHouseBulkInsertResult> InsertAsync<TEntity>(
        IEnumerable<TEntity> entities,
        Action<ClickHouseBulkInsertOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var options = new ClickHouseBulkInsertOptions();
        configure?.Invoke(options);

        var propertyInfo = _propertyCache.GetPropertyInfo<TEntity>();
        var settings = options.GetEffectiveSettings();

        var stopwatch = Stopwatch.StartNew();
        long totalRowsInserted = 0;
        var batchesExecuted = 0;

        // Materialize to list for batching
        var entityList = entities as IList<TEntity> ?? entities.ToList();

        if (entityList.Count == 0)
        {
            return ClickHouseBulkInsertResult.Empty;
        }

        // Create batches
        var batches = CreateBatches(entityList, options.BatchSize);

        if (options.MaxDegreeOfParallelism > 1)
        {
            // Parallel execution
            await ExecuteParallelAsync(
                batches,
                propertyInfo,
                settings,
                options,
                r =>
                {
                    Interlocked.Add(ref totalRowsInserted, r.rowCount);
                    Interlocked.Increment(ref batchesExecuted);
                    options.OnBatchCompleted?.Invoke(Interlocked.Read(ref totalRowsInserted));
                },
                cancellationToken);
        }
        else
        {
            // Sequential execution
            foreach (var batch in batches)
            {
                await ExecuteBatchAsync(batch, propertyInfo, settings, options, cancellationToken);
                totalRowsInserted += batch.Count;
                batchesExecuted++;
                options.OnBatchCompleted?.Invoke(totalRowsInserted);
            }
        }

        return ClickHouseBulkInsertResult.Create(stopwatch, totalRowsInserted, batchesExecuted);
    }

    /// <inheritdoc />
    public async Task<ClickHouseBulkInsertResult> InsertStreamingAsync<TEntity>(
        IAsyncEnumerable<TEntity> entities,
        Action<ClickHouseBulkInsertOptions>? configure = null,
        CancellationToken cancellationToken = default) where TEntity : class
    {
        var options = new ClickHouseBulkInsertOptions();
        configure?.Invoke(options);

        var propertyInfo = _propertyCache.GetPropertyInfo<TEntity>();
        var settings = options.GetEffectiveSettings();

        var stopwatch = Stopwatch.StartNew();
        long totalRowsInserted = 0;
        var batchesExecuted = 0;

        var currentBatch = new List<TEntity>(options.BatchSize);

        await foreach (var entity in entities.WithCancellation(cancellationToken))
        {
            currentBatch.Add(entity);

            if (currentBatch.Count >= options.BatchSize)
            {
                await ExecuteBatchAsync(currentBatch, propertyInfo, settings, options, cancellationToken);
                totalRowsInserted += currentBatch.Count;
                batchesExecuted++;
                options.OnBatchCompleted?.Invoke(totalRowsInserted);
                currentBatch.Clear();
            }
        }

        // Insert remaining entities
        if (currentBatch.Count > 0)
        {
            await ExecuteBatchAsync(currentBatch, propertyInfo, settings, options, cancellationToken);
            totalRowsInserted += currentBatch.Count;
            batchesExecuted++;
            options.OnBatchCompleted?.Invoke(totalRowsInserted);
        }

        return ClickHouseBulkInsertResult.Create(stopwatch, totalRowsInserted, batchesExecuted);
    }

    private async Task ExecuteBatchAsync<TEntity>(
        IReadOnlyList<TEntity> batch,
        EntityPropertyInfo propertyInfo,
        Dictionary<string, object> settings,
        ClickHouseBulkInsertOptions options,
        CancellationToken cancellationToken) where TEntity : class
    {
        if (batch.Count == 0)
        {
            return;
        }

        var sql = BuildSql(batch, propertyInfo, settings, options.Format);

        await _relationalConnection.OpenAsync(cancellationToken);
        try
        {
            await using var command = _relationalConnection.DbConnection.CreateCommand();
            command.CommandText = sql;

            if (options.Timeout.HasValue)
            {
                command.CommandTimeout = (int)options.Timeout.Value.TotalSeconds;
            }

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            await _relationalConnection.CloseAsync();
        }
    }

    private async Task ExecuteParallelAsync<TEntity>(
        IEnumerable<List<TEntity>> batches,
        EntityPropertyInfo propertyInfo,
        Dictionary<string, object> settings,
        ClickHouseBulkInsertOptions options,
        Action<(int batchIndex, int rowCount)> onBatchComplete,
        CancellationToken cancellationToken) where TEntity : class
    {
        var connectionString = _relationalConnection.ConnectionString
            ?? throw new InvalidOperationException("Connection string is not available for parallel execution.");

        var semaphore = new SemaphoreSlim(options.MaxDegreeOfParallelism);
        var batchList = batches.ToList();

        var tasks = batchList.Select(async (batch, index) =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                var sql = BuildSql(batch, propertyInfo, settings, options.Format);

                // Create a new connection for parallel execution
                await using var connection = new ClickHouseConnection(connectionString);
                await connection.OpenAsync(cancellationToken);

                await using var command = connection.CreateCommand();
                command.CommandText = sql;

                if (options.Timeout.HasValue)
                {
                    command.CommandTimeout = (int)options.Timeout.Value.TotalSeconds;
                }

                await command.ExecuteNonQueryAsync(cancellationToken);
                onBatchComplete((index, batch.Count));
            }
            finally
            {
                semaphore.Release();
            }
        });

        await Task.WhenAll(tasks);
    }

    private string BuildSql<TEntity>(
        IReadOnlyList<TEntity> batch,
        EntityPropertyInfo propertyInfo,
        Dictionary<string, object> settings,
        ClickHouseBulkInsertFormat format) where TEntity : class
    {
        return format switch
        {
            ClickHouseBulkInsertFormat.JsonEachRow => _jsonBuilder.Build(batch, propertyInfo, settings),
            _ => _valuesSqlBuilder.Build(batch, propertyInfo, settings)
        };
    }

    private static IEnumerable<List<TEntity>> CreateBatches<TEntity>(
        IList<TEntity> entities,
        int batchSize) where TEntity : class
    {
        for (var i = 0; i < entities.Count; i += batchSize)
        {
            var count = Math.Min(batchSize, entities.Count - i);
            var batch = new List<TEntity>(count);

            for (var j = 0; j < count; j++)
            {
                batch.Add(entities[i + j]);
            }

            yield return batch;
        }
    }
}
