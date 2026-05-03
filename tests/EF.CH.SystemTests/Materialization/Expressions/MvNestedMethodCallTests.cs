using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Nested method calls inside MV selectors / GROUP BY — e.g. composing two
/// ClickHouseFunctions, or wrapping an aggregate around a DateTime member.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvNestedMethodCallTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvNestedMethodCallTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task ToStartOfDay_Of_ToStartOfHour_Composed()
    {
        await using var ctx = TestContextFactory.Create<ComposedCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2026, 4, 25, 10, 0, 0, DateTimeKind.Utc);
        ctx.Source.AddRange(
            new Row { Id = 1, At = t.AddHours(0), Hits = 1 },
            new Row { Id = 2, At = t.AddHours(5), Hits = 1 },
            new Row { Id = 3, At = t.AddDays(1),  Hits = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvNestedComposedTarget");

        // toStartOfDay(toStartOfHour(At)) collapses both same-day rows to the same key.
        Assert.Equal(2UL, await RawClickHouse.RowCountAsync(Conn, "MvNestedComposedTarget", final: true));
    }

    [Fact]
    public async Task Aggregate_Of_DateTimeMember()
    {
        await using var ctx = TestContextFactory.Create<AggMemberCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new Row { Id = 1, At = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), Hits = 1 },
            new Row { Id = 2, At = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc), Hits = 1 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvNestedAggMemberTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(MaxYear) AS MaxYear FROM \"MvNestedAggMemberTarget\" FINAL");
        Assert.Equal(2026L, Convert.ToInt64(rows[0]["MaxYear"]));
    }

    public sealed class Row { public long Id { get; set; } public DateTime At { get; set; } public long Hits { get; set; } }
    public sealed class ComposedTgt { public DateTime Bucket { get; set; } public long Hits { get; set; } }
    public sealed class AggMemberTgt { public long Bucket { get; set; } public long MaxYear { get; set; } }

    public sealed class ComposedCtx(DbContextOptions<ComposedCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<ComposedTgt> Target => Set<ComposedTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvNestedComposedSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<ComposedTgt>(e =>
            {
                e.ToTable("MvNestedComposedTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<ComposedTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(r => ClickHouseFunctions.ToStartOfDay(ClickHouseFunctions.ToStartOfHour(r.At)))
                    .Select(g => new ComposedTgt { Bucket = g.Key, Hits = g.Sum(r => r.Hits) }));
        }
    }

    public sealed class AggMemberCtx(DbContextOptions<AggMemberCtx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>(); public DbSet<AggMemberTgt> Target => Set<AggMemberTgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvNestedAggMemberSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<AggMemberTgt>(e =>
            {
                e.ToTable("MvNestedAggMemberTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<AggMemberTgt>().From<Row>().DefinedAs(rows => rows
                    .GroupBy(_ => 1L)
                    .Select(g => new AggMemberTgt { Bucket = g.Key, MaxYear = g.Max(r => r.At.Year) }));
        }
    }
}
