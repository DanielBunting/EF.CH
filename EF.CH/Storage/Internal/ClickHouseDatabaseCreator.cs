using System.Data.Common;
using ClickHouse.Driver.ADO;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal;

/// <summary>
/// ClickHouse implementation of IRelationalDatabaseCreator.
/// ClickHouse databases are created automatically, so most operations are no-ops or simplified.
/// ClickHouse does not support transactions, so we execute DDL commands directly.
/// </summary>
public class ClickHouseDatabaseCreator : RelationalDatabaseCreator
{
    private readonly IRelationalConnection _connection;
    private readonly IDbContextOptions _contextOptions;

    public ClickHouseDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IRelationalConnection connection,
        IDbContextOptions contextOptions)
        : base(dependencies)
    {
        _connection = connection;
        _contextOptions = contextOptions;
    }

    /// <summary>
    /// Gets the cluster name if configured for ON CLUSTER operations.
    /// </summary>
    private string? GetClusterName()
    {
        var extension = _contextOptions.FindExtension<ClickHouseOptionsExtension>();
        return extension?.ClusterName;
    }

    /// <summary>
    /// Gets the ON CLUSTER clause if a cluster is configured.
    /// </summary>
    private string GetOnClusterClause()
    {
        var clusterName = GetClusterName();
        return string.IsNullOrEmpty(clusterName) ? "" : $" ON CLUSTER {clusterName}";
    }

    /// <summary>
    /// Creates a connection to the server without specifying a database,
    /// for operations like CREATE/DROP DATABASE.
    /// </summary>
    private DbConnection CreateMasterConnection()
    {
        var connectionString = _connection.ConnectionString;
        // Replace the Database parameter to connect to system database
        var builder = new ClickHouseConnectionStringBuilder(connectionString)
        {
            Database = "system"
        };
        return new ClickHouseConnection(builder.ToString());
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
        var database = _connection.DbConnection.Database;
        if (string.IsNullOrEmpty(database))
            return;

        // Use a master connection (connected to 'system' database) to create the target database
        using var masterConnection = CreateMasterConnection();
        using var command = masterConnection.CreateCommand();
        var onCluster = GetOnClusterClause();
        command.CommandText = $"CREATE DATABASE IF NOT EXISTS \"{database}\"{onCluster}";

        masterConnection.Open();
        try
        {
            command.ExecuteNonQuery();
        }
        finally
        {
            masterConnection.Close();
        }
    }

    public override async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        var database = _connection.DbConnection.Database;
        if (string.IsNullOrEmpty(database))
            return;

        // Use a master connection (connected to 'system' database) to create the target database
        await using var masterConnection = CreateMasterConnection();
        await using var command = masterConnection.CreateCommand();
        var onCluster = GetOnClusterClause();
        command.CommandText = $"CREATE DATABASE IF NOT EXISTS \"{database}\"{onCluster}";

        await masterConnection.OpenAsync(cancellationToken);
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            await masterConnection.CloseAsync();
        }
    }

    public override void Delete()
    {
        var database = _connection.DbConnection.Database;
        if (string.IsNullOrEmpty(database))
            return;

        // Use a master connection (connected to 'system' database) to drop the target database
        using var masterConnection = CreateMasterConnection();
        using var command = masterConnection.CreateCommand();
        var onCluster = GetOnClusterClause();
        command.CommandText = $"DROP DATABASE IF EXISTS \"{database}\"{onCluster}";

        masterConnection.Open();
        try
        {
            command.ExecuteNonQuery();
        }
        finally
        {
            masterConnection.Close();
        }
    }

    public override async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        var database = _connection.DbConnection.Database;
        if (string.IsNullOrEmpty(database))
            return;

        // Use a master connection (connected to 'system' database) to drop the target database
        await using var masterConnection = CreateMasterConnection();
        await using var command = masterConnection.CreateCommand();
        var onCluster = GetOnClusterClause();
        command.CommandText = $"DROP DATABASE IF EXISTS \"{database}\"{onCluster}";

        await masterConnection.OpenAsync(cancellationToken);
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            await masterConnection.CloseAsync();
        }
    }

    /// <summary>
    /// Creates all tables for the current model in the database.
    /// ClickHouse does not support transactions, so we execute each command directly.
    /// </summary>
    public override void CreateTables()
    {
        var commands = GetCreateTablesCommands();

        _connection.Open();
        try
        {
            foreach (var command in commands)
            {
                using var dbCommand = _connection.DbConnection.CreateCommand();
                dbCommand.CommandText = command.CommandText;
                dbCommand.ExecuteNonQuery();
            }
        }
        finally
        {
            _connection.Close();
        }
    }

    /// <summary>
    /// Asynchronously creates all tables for the current model in the database.
    /// ClickHouse does not support transactions, so we execute each command directly.
    /// </summary>
    public override async Task CreateTablesAsync(CancellationToken cancellationToken = default)
    {
        var commands = GetCreateTablesCommands();

        await _connection.OpenAsync(cancellationToken);
        try
        {
            foreach (var command in commands)
            {
                await using var dbCommand = _connection.DbConnection.CreateCommand();
                dbCommand.CommandText = command.CommandText;
                await dbCommand.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        finally
        {
            await _connection.CloseAsync();
        }
    }
}
