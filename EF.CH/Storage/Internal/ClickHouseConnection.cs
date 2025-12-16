using System.Data;
using System.Data.Common;
using ClickHouse.Driver.ADO;
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

    /// <summary>
    /// ClickHouse does not support transactions. This returns a no-op transaction
    /// to allow migration commands to execute without errors.
    /// </summary>
    public override IDbContextTransaction BeginTransaction()
    {
        return new ClickHouseNoOpTransaction();
    }

    /// <summary>
    /// ClickHouse does not support transactions. This returns a no-op transaction
    /// to allow migration commands to execute without errors.
    /// </summary>
    public override IDbContextTransaction BeginTransaction(IsolationLevel isolationLevel)
    {
        return new ClickHouseNoOpTransaction();
    }

    /// <summary>
    /// ClickHouse does not support transactions. This returns a completed task with a no-op transaction.
    /// </summary>
    public override Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IDbContextTransaction>(new ClickHouseNoOpTransaction());
    }

    /// <summary>
    /// ClickHouse does not support transactions. This returns a completed task with a no-op transaction.
    /// </summary>
    public override Task<IDbContextTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IDbContextTransaction>(new ClickHouseNoOpTransaction());
    }
}

/// <summary>
/// A no-op transaction for ClickHouse that doesn't actually do anything.
/// ClickHouse doesn't support transactions, but EF Core's migration executor
/// expects to be able to wrap operations in transactions.
/// </summary>
internal sealed class ClickHouseNoOpTransaction : IDbContextTransaction
{
    public Guid TransactionId { get; } = Guid.NewGuid();

    public void Commit()
    {
        // No-op: ClickHouse doesn't support transactions
    }

    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    public void Rollback()
    {
        // No-op: ClickHouse doesn't support transactions
    }

    public Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        // Nothing to dispose
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
