using System.Text;
using EF.CH.BulkInsert.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.TempTable;

/// <summary>
/// Implementation of <see cref="IClickHouseTempTableManager"/> that creates and manages
/// ClickHouse temporary tables using Memory engine tables with unique names.
/// Uses regular CREATE TABLE (not CREATE TEMPORARY TABLE) for compatibility with
/// ClickHouse's stateless HTTP protocol where each request is a separate session.
/// </summary>
public sealed class ClickHouseTempTableManager : IClickHouseTempTableManager
{
    private readonly ICurrentDbContext _currentDbContext;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ISqlGenerationHelper _sqlGenerationHelper;
    private readonly IRelationalConnection _relationalConnection;
    private readonly EntityPropertyCache _propertyCache;

    public ClickHouseTempTableManager(
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
    }

    /// <inheritdoc />
    public async Task<TempTableHandle<T>> CreateAsync<T>(string? tableName = null, CancellationToken cancellationToken = default) where T : class
    {
        var propertyInfo = _propertyCache.GetPropertyInfo<T>();

        var name = tableName ?? $"_tmp_{typeof(T).Name}_{Guid.NewGuid().ToString("N")[..8]}";
        var quotedName = _sqlGenerationHelper.DelimitIdentifier(name);

        var ddl = BuildCreateDdl(quotedName, propertyInfo);

        await _relationalConnection.OpenAsync(cancellationToken);

        try
        {
            await using var command = _relationalConnection.DbConnection.CreateCommand();
            command.CommandText = ddl;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch
        {
            await _relationalConnection.CloseAsync();
            throw;
        }

        return new TempTableHandle<T>(
            name,
            quotedName,
            propertyInfo,
            _relationalConnection,
            _typeMappingSource,
            _currentDbContext.Context);
    }

    /// <inheritdoc />
    public async Task<TempTableHandle<T>> CreateFromQueryAsync<T>(IQueryable<T> sourceQuery, string? tableName = null, CancellationToken cancellationToken = default) where T : class
    {
        var handle = await CreateAsync<T>(tableName, cancellationToken);

        try
        {
            await handle.InsertFromQueryAsync(sourceQuery, cancellationToken);
        }
        catch
        {
            await handle.DisposeAsync();
            throw;
        }

        return handle;
    }

    private static string BuildCreateDdl(string quotedName, EntityPropertyInfo propertyInfo)
    {
        var sb = new StringBuilder();
        sb.Append("CREATE TABLE ");
        sb.Append(quotedName);
        sb.Append(" (");

        for (var i = 0; i < propertyInfo.Properties.Count; i++)
        {
            if (i > 0) sb.Append(", ");

            var prop = propertyInfo.Properties[i];
            sb.Append(prop.QuotedColumnName);
            sb.Append(' ');
            sb.Append(prop.TypeMapping.StoreType);
        }

        sb.Append(") ENGINE = Memory");
        return sb.ToString();
    }
}
