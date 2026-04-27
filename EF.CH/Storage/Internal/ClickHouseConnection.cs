using System.Data;
using System.Data.Common;
using ClickHouse.Driver.ADO;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace EF.CH.Storage.Internal;

/// <summary>
/// Represents a connection to a ClickHouse database.
/// </summary>
public class ClickHouseRelationalConnection : RelationalConnection
{
    private readonly IDiagnosticsLogger<DbLoggerCategory.Database.Transaction>? _transactionLogger;

    // Track per-connection-instance whether the no-op-transaction warning has fired.
    // ClickHouse does not support transactions; we want one warning per connection
    // on first BeginTransaction, regardless of how many transactions get opened.
    private bool _transactionWarningFired;

    /// <summary>
    /// Creates a new instance of <see cref="ClickHouseRelationalConnection"/>.
    /// </summary>
    public ClickHouseRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies)
    {
        _transactionLogger = dependencies.TransactionLogger;
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
        WarnTransactionsNotSupportedOnce();
        return new ClickHouseNoOpTransaction();
    }

    /// <summary>
    /// ClickHouse does not support transactions. This returns a no-op transaction
    /// to allow migration commands to execute without errors.
    /// </summary>
    public override IDbContextTransaction BeginTransaction(IsolationLevel isolationLevel)
    {
        WarnTransactionsNotSupportedOnce();
        return new ClickHouseNoOpTransaction();
    }

    /// <summary>
    /// ClickHouse does not support transactions. This returns a completed task with a no-op transaction.
    /// </summary>
    public override Task<IDbContextTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        WarnTransactionsNotSupportedOnce();
        return Task.FromResult<IDbContextTransaction>(new ClickHouseNoOpTransaction());
    }

    /// <summary>
    /// ClickHouse does not support transactions. This returns a completed task with a no-op transaction.
    /// </summary>
    public override Task<IDbContextTransaction> BeginTransactionAsync(IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        WarnTransactionsNotSupportedOnce();
        return Task.FromResult<IDbContextTransaction>(new ClickHouseNoOpTransaction());
    }

    private void WarnTransactionsNotSupportedOnce()
    {
        if (_transactionWarningFired)
            return;
        _transactionWarningFired = true;

        ClickHouseConnectionLog.LogTransactionsNotSupported(_transactionLogger);
    }
}

/// <summary>
/// Static logger helpers for <see cref="ClickHouseRelationalConnection"/>.
/// </summary>
internal static class ClickHouseConnectionLog
{
    private static readonly Action<ILogger, Exception?> _txNotSupported =
        LoggerMessage.Define(
            LogLevel.Warning,
            new EventId(1001, "ClickHouseTransactionsNotSupported"),
            "BeginTransaction was called on a ClickHouse connection. " +
            "ClickHouse does not support transactions; commit and rollback are no-ops. " +
            "This warning is logged once per connection.");

    public static void LogTransactionsNotSupported(
        IDiagnosticsLogger<DbLoggerCategory.Database.Transaction>? logger)
    {
        if (logger is null) return;
        _txNotSupported(logger.Logger, null);
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
