using System.Data.Common;
using ClickHouse.Client.ADO;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Storage.Internal;

/// <summary>
/// Represents a connection to a ClickHouse database.
/// </summary>
public class ClickHouseRelationalConnection : RelationalConnection
{
    /// <summary>
    /// Creates a new instance of <see cref="ClickHouseRelationalConnection"/>.
    /// </summary>
    public ClickHouseRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies)
    {
    }

    /// <summary>
    /// Creates a new ClickHouse database connection.
    /// </summary>
    protected override DbConnection CreateDbConnection()
    {
        var connectionString = ConnectionString
            ?? throw new InvalidOperationException("Connection string is not set.");

        return new ClickHouseConnection(connectionString);
    }
}
