using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace EF.CH.Infrastructure;

/// <summary>
/// Transaction factory invoked when EF Core needs to wrap an existing
/// <see cref="DbTransaction"/> (for example via
/// <c>DatabaseFacade.UseTransaction(externalTx)</c>). Throws
/// <see cref="ClickHouseUnsupportedOperationException"/> because we cannot honour
/// the user's intent of enlisting a real transaction in a ClickHouse connection.
/// </summary>
/// <remarks>
/// This is intentionally separate from
/// <c>ClickHouseRelationalConnection.BeginTransaction</c>, which returns a
/// <c>ClickHouseNoOpTransaction</c> so that EF Core's migration executor and user
/// code that opens a transaction-scoped block continue to function. The two
/// behaviours are layered, not contradictory:
/// <list type="bullet">
///   <item><description><c>BeginTransaction</c> on the DbContext: returns a no-op (no real tx exists, but EF Core's plumbing keeps working).</description></item>
///   <item><description><c>UseTransaction(externalTx)</c>: throws via this factory, because the user is explicitly handing us a real tx and we cannot enlist it.</description></item>
///   <item><description>Raw <c>DbConnection.BeginTransaction</c>: throws (driver-level, ClickHouse does not support transactions).</description></item>
/// </list>
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
        throw ClickHouseUnsupportedOperationException.Transaction();
    }
}
