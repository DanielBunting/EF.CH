using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// LINQ ternary (<c>?:</c>) inside MV definitions — translates to ClickHouse
/// <c>if(cond, t, f)</c>. Covers ternary in both the aggregate selector and
/// the GROUP BY key.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvConditionalExpressionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvConditionalExpressionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Ternary_InAggregateSelector()
    {
        await using var ctx = TestContextFactory.Create<TernarySelector.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new TernarySelector.Row { Id = 1, Bucket = "a", Amount = 100, IsRefund = false },
            new TernarySelector.Row { Id = 2, Bucket = "a", Amount =  40, IsRefund = true  },
            new TernarySelector.Row { Id = 3, Bucket = "b", Amount =  25, IsRefund = false });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvTernarySelectorTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Bucket, toInt64(NetTotal) AS Net FROM \"MvTernarySelectorTarget\" FINAL ORDER BY Bucket");
        Assert.Equal(2, rows.Count);
        Assert.Equal( 60L, Convert.ToInt64(rows[0]["Net"])); // 100 - 40
        Assert.Equal( 25L, Convert.ToInt64(rows[1]["Net"]));
    }

    [Fact]
    public async Task Ternary_InGroupByKey()
    {
        await using var ctx = TestContextFactory.Create<TernaryKey.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new TernaryKey.Row { Id = 1, Score = 95, Hits = 1 },
            new TernaryKey.Row { Id = 2, Score = 80, Hits = 1 },
            new TernaryKey.Row { Id = 3, Score = 30, Hits = 1 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvTernaryKeyTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Tier, toInt64(Total) AS Total FROM \"MvTernaryKeyTarget\" FINAL ORDER BY Tier");
        Assert.Equal(2, rows.Count);
        Assert.Equal("high", (string)rows[0]["Tier"]!); Assert.Equal(2L, Convert.ToInt64(rows[0]["Total"]));
        Assert.Equal("low",  (string)rows[1]["Tier"]!); Assert.Equal(1L, Convert.ToInt64(rows[1]["Total"]));
    }

    public static class TernarySelector
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MvTernarySelectorSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MvTernarySelectorTarget"); e.HasNoKey();
                    e.UseSummingMergeTree(x => x.Bucket);
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .GroupBy(r => r.Bucket)
                        .Select(g => new Target
                        {
                            Bucket = g.Key,
                            NetTotal = g.Sum(r => r.IsRefund ? -r.Amount : r.Amount),
                        }));
                });
            }
        }
        public class Row { public long Id { get; set; } public string Bucket { get; set; } = ""; public long Amount { get; set; } public bool IsRefund { get; set; } }
        public class Target { public string Bucket { get; set; } = ""; public long NetTotal { get; set; } }
    }

    public static class TernaryKey
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MvTernaryKeySource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MvTernaryKeyTarget"); e.HasNoKey();
                    e.UseSummingMergeTree(x => x.Tier);
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .GroupBy(r => r.Score >= 50 ? "high" : "low")
                        .Select(g => new Target { Tier = g.Key, Total = g.Sum(r => r.Hits) }));
                });
            }
        }
        public class Row { public long Id { get; set; } public int Score { get; set; } public long Hits { get; set; } }
        public class Target { public string Tier { get; set; } = ""; public long Total { get; set; } }
    }
}
