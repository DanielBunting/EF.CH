using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Probe for a potential data-loss bug: <c>VisitDelete</c> emits
/// <c>WHERE 1</c> when <c>SelectExpression.Predicate</c> is null. If EF Core's
/// optimiser folds an always-false predicate (e.g. <c>.Where(x => false)</c>)
/// into a null Predicate, the resulting SQL would silently delete the entire
/// table instead of zero rows. This test seeds rows, runs the false-predicate
/// delete, and asserts the row count is unchanged. If it fails, we have a
/// real data-loss bug and need to add a guard in <c>VisitDelete</c>.
/// </summary>
public class ExecuteDeleteFalsePredicateProbeTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public Task InitializeAsync() => _container.StartAsync();
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    [Fact]
    public async Task ExecuteDeleteAsync_WithAlwaysFalsePredicate_DoesNotDeleteAnyRows()
    {
        await using var ctx = Create();
        await ctx.Database.EnsureCreatedAsync();

        for (uint i = 1; i <= 100; i++)
        {
            ctx.Rows.Add(new ProbeRow { Id = i, Value = (int)i });
        }
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var before = await ctx.Rows.LongCountAsync();
        Assert.Equal(100, before);

        await ctx.Rows.Where(_ => false).ExecuteDeleteAsync();

        // Allow async ALTER TABLE DELETE to settle if it ran.
        await Task.Delay(500);

        var after = await ctx.Rows.LongCountAsync();
        Assert.Equal(100, after);
    }

    [Fact]
    public async Task ExecuteDeleteAsync_WithNoPredicate_DeletesEverything()
    {
        // Sanity contrast: explicit no-predicate delete still works (this is
        // the deliberate-truncate path documented in limitations.md).
        await using var ctx = Create();
        await ctx.Database.EnsureCreatedAsync();

        for (uint i = 1; i <= 50; i++)
        {
            ctx.Rows.Add(new ProbeRow { Id = i, Value = (int)i });
        }
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        await ctx.Rows.ExecuteDeleteAsync();
        await Task.Delay(500);

        var after = await ctx.Rows.LongCountAsync();
        Assert.Equal(0, after);
    }

    private ProbeCtx Create() =>
        new(new DbContextOptionsBuilder<ProbeCtx>()
            .UseClickHouse(_container.GetConnectionString())
            .Options);

    public sealed class ProbeRow
    {
        public uint Id { get; set; }
        public int Value { get; set; }
    }

    public sealed class ProbeCtx(DbContextOptions<ProbeCtx> o) : DbContext(o)
    {
        public DbSet<ProbeRow> Rows => Set<ProbeRow>();
        protected override void OnModelCreating(ModelBuilder mb)
            => mb.Entity<ProbeRow>(e =>
            {
                e.ToTable("delete_probe_rows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
    }
}
