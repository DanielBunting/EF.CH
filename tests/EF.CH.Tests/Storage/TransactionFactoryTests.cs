using EF.CH.Infrastructure;
using Xunit;

namespace EF.CH.Tests.Storage;

/// <summary>
/// Pins the layered transaction semantics in EF.CH:
/// <list type="bullet">
///   <item><description><c>BeginTransaction</c> on the DbContext returns a no-op (covered in <see cref="NoOpTransactionTests"/>).</description></item>
///   <item><description><c>UseTransaction(externalTx)</c> goes through <see cref="ClickHouseTransactionFactory.Create"/> and throws clearly — see this file.</description></item>
/// </list>
/// The two are intentionally different, not contradictory: the no-op exists so EF Core's
/// migration executor and user code that wraps work in a transaction-scoped block keep
/// functioning; the throw exists because <c>UseTransaction</c> is a request to enlist a
/// real <see cref="System.Data.Common.DbTransaction"/> in the connection, which we cannot honour.
/// </summary>
public class TransactionFactoryTests
{
    [Fact]
    public void Create_ThrowsClickHouseUnsupportedOperationException()
    {
        var factory = new ClickHouseTransactionFactory();

        var ex = Assert.Throws<ClickHouseUnsupportedOperationException>(() =>
            factory.Create(
                connection: null!,
                transaction: null!,
                transactionId: Guid.NewGuid(),
                logger: null!,
                transactionOwned: false));

        Assert.Contains("does not support", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
