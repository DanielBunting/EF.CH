using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>Unary NOT and Negate operators in MV expressions.</summary>
[Collection(SingleNodeCollection.Name)]
public class MvUnaryExpressionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvUnaryExpressionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Not_InWherePredicate()
    {
        await using var ctx = TestContextFactory.Create<NotCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new Row { Id = 1, IsActive = true,  N = 10 },
            new Row { Id = 2, IsActive = false, N = 20 },
            new Row { Id = 3, IsActive = true,  N = 30 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvUnaryNotTarget");

        // !IsActive → only row 2
        Assert.Equal(1UL, await RawClickHouse.RowCountAsync(Conn, "MvUnaryNotTarget"));
        var rows = await RawClickHouse.RowsAsync(Conn, "SELECT toInt64(N) AS N FROM \"MvUnaryNotTarget\"");
        Assert.Equal(20L, Convert.ToInt64(rows[0]["N"]));
    }

    [Fact]
    public async Task Negate_InAggregateSelector()
    {
        await using var ctx = TestContextFactory.Create<NegCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new Row { Id = 1, IsActive = false, N =  5 },
            new Row { Id = 2, IsActive = false, N = 10 },
            new Row { Id = 3, IsActive = false, N = 15 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvUnaryNegTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(NegSum) AS NegSum FROM \"MvUnaryNegTarget\" FINAL");
        Assert.Equal(-30L, Convert.ToInt64(rows[0]["NegSum"]));
    }

    public sealed class Row { public long Id { get; set; } public bool IsActive { get; set; } public long N { get; set; } }
    public sealed class FilterTarget { public long Id { get; set; } public long N { get; set; } }
    public sealed class AggTarget { public long Bucket { get; set; } public long NegSum { get; set; } }

    public sealed class NotCtx(DbContextOptions<NotCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<FilterTarget> Target => Set<FilterTarget>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvUnaryNotSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<FilterTarget>(e =>
            {
                e.ToTable("MvUnaryNotTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);

            });
            mb.MaterializedView<FilterTarget>().From<Row>().DefinedAs(rows => rows
                    .Where(r => !r.IsActive)
                    .Select(r => new FilterTarget { Id = r.Id, N = r.N }));
        }
    }

    public sealed class NegCtx(DbContextOptions<NegCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<AggTarget> Target => Set<AggTarget>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvUnaryNegSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<AggTarget>(e =>
            {
                e.ToTable("MvUnaryNegTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<AggTarget>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(_ => 1L)
                    .Select(g => new AggTarget { Bucket = g.Key, NegSum = g.Sum(r => -r.N) }));
        }
    }
}
