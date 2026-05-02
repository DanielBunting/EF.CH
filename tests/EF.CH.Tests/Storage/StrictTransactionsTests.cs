using EF.CH.Extensions;
using EF.CH.Infrastructure;
using EF.CH.Tests.Sql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Logging;
using Xunit;

namespace EF.CH.Tests.Storage;

/// <summary>
/// Pins the opt-in strict-transaction mode. Default behaviour stays a
/// warn-once no-op (existing NoOpTransactionTests cover that). When the
/// caller opts into strict mode, BeginTransaction* throws so that code
/// paths assuming real transactional semantics can't accidentally rely on
/// a no-op.
/// </summary>
public class StrictTransactionsTests
{
    [Fact]
    public void DefaultBehavior_BeginTransaction_ReturnsNoOp()
    {
        using var ctx = NewContext(strict: false);
        using var tx = ctx.Database.BeginTransaction();
        Assert.NotNull(tx);
        tx.Commit(); // no-op, must not throw
    }

    [Fact]
    public void Strict_BeginTransaction_Throws()
    {
        using var ctx = NewContext(strict: true);
        var ex = Assert.Throws<ClickHouseUnsupportedOperationException>(
            () => ctx.Database.BeginTransaction());
        Assert.Contains("transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Strict_BeginTransactionAsync_Throws()
    {
        using var ctx = NewContext(strict: true);
        await Assert.ThrowsAsync<ClickHouseUnsupportedOperationException>(
            () => ctx.Database.BeginTransactionAsync());
    }

    private static TestDbContext NewContext(bool strict)
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse("Host=localhost;Database=test", o =>
            {
                if (strict) o.UseStrictTransactions();
            })
            .Options;
        return new TestDbContext(options);
    }
}
