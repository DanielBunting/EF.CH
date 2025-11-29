using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Infrastructure;

/// <summary>
/// Transaction factory for ClickHouse that throws <see cref="NotSupportedException"/>
/// when transactions are requested.
/// </summary>
/// <remarks>
/// ClickHouse does not support ACID transactions. Writes are atomic at the block level
/// but there is no rollback capability. This factory ensures users get a clear error
/// when attempting to use transactions.
/// </remarks>
public class ClickHouseTransactionFactory : IRelationalTransactionFactory
{
    public RelationalTransaction Create(
        IRelationalConnection connection,
        DbTransaction transaction,
        Guid transactionId,
        IDiagnosticsLogger<DbLoggerCategory.Database.Transaction> logger,
        bool transactionOwned)
    {
        throw new NotSupportedException(
            "ClickHouse does not support ACID transactions. " +
            "Writes are atomic at the block level but there is no rollback capability. " +
            "Consider using idempotent operations or implementing saga patterns for complex workflows. " +
            "For bulk operations, use the ClickHouse-specific batch insert functionality.");
    }
}

/// <summary>
/// Exception thrown when a transaction operation is attempted on ClickHouse.
/// </summary>
public class ClickHouseTransactionNotSupportedException : NotSupportedException
{
    public ClickHouseTransactionNotSupportedException()
        : base("ClickHouse does not support transactions.")
    {
    }

    public ClickHouseTransactionNotSupportedException(string message)
        : base(message)
    {
    }

    public ClickHouseTransactionNotSupportedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
