using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal;

/// <summary>
/// ClickHouse implementation of IRelationalDatabaseCreator.
/// ClickHouse databases are created automatically, so most operations are no-ops or simplified.
/// </summary>
public class ClickHouseDatabaseCreator : RelationalDatabaseCreator
{
    private readonly IRelationalConnection _connection;

    public ClickHouseDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IRelationalConnection connection)
        : base(dependencies)
    {
        _connection = connection;
    }

    public override bool Exists()
    {
        try
        {
            _connection.Open();
            return true;
        }
        catch
        {
            return false;
        }
        finally
        {
            _connection.Close();
        }
    }

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await _connection.OpenAsync(cancellationToken);
            return true;
        }
        catch
        {
            return false;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public override bool HasTables()
    {
        using var command = _connection.DbConnection.CreateCommand();
        command.CommandText = "SELECT count() FROM system.tables WHERE database = currentDatabase()";

        _connection.Open();
        try
        {
            var result = command.ExecuteScalar();
            return result is not null && Convert.ToInt64(result) > 0;
        }
        finally
        {
            _connection.Close();
        }
    }

    public override async Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _connection.DbConnection.CreateCommand();
        command.CommandText = "SELECT count() FROM system.tables WHERE database = currentDatabase()";

        await _connection.OpenAsync(cancellationToken);
        try
        {
            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is not null && Convert.ToInt64(result) > 0;
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }

    public override void Create()
    {
        // ClickHouse databases are typically created via connection string or externally
        // For now, we'll try to create it using a command
        using var command = _connection.DbConnection.CreateCommand();

        var database = _connection.DbConnection.Database;
        if (!string.IsNullOrEmpty(database))
        {
            command.CommandText = $"CREATE DATABASE IF NOT EXISTS \"{database}\"";

            // Need to connect to default database first
            _connection.Open();
            try
            {
                command.ExecuteNonQuery();
            }
            finally
            {
                _connection.Close();
            }
        }
    }

    public override async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _connection.DbConnection.CreateCommand();

        var database = _connection.DbConnection.Database;
        if (!string.IsNullOrEmpty(database))
        {
            command.CommandText = $"CREATE DATABASE IF NOT EXISTS \"{database}\"";

            await _connection.OpenAsync(cancellationToken);
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }
    }

    public override void Delete()
    {
        using var command = _connection.DbConnection.CreateCommand();

        var database = _connection.DbConnection.Database;
        if (!string.IsNullOrEmpty(database))
        {
            command.CommandText = $"DROP DATABASE IF EXISTS \"{database}\"";

            _connection.Open();
            try
            {
                command.ExecuteNonQuery();
            }
            finally
            {
                _connection.Close();
            }
        }
    }

    public override async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        await using var command = _connection.DbConnection.CreateCommand();

        var database = _connection.DbConnection.Database;
        if (!string.IsNullOrEmpty(database))
        {
            command.CommandText = $"DROP DATABASE IF EXISTS \"{database}\"";

            await _connection.OpenAsync(cancellationToken);
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            finally
            {
                await _connection.CloseAsync();
            }
        }
    }
}
