using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Aggregation;

[Collection(SingleNodeCollection.Name)]
public class SimpleAggregateFunctionRollupTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public SimpleAggregateFunctionRollupTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Max_FoldsToLargestValuePerKey_AfterOptimize()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Scores.Add(new Score { Player = "alice", Best = 10, Total = 100, Floor = 5 });
        ctx.Scores.Add(new Score { Player = "bob",   Best = 7,  Total = 70,  Floor = 2 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        ctx.Scores.Add(new Score { Player = "alice", Best = 25, Total = 200, Floor = 3 });
        ctx.Scores.Add(new Score { Player = "bob",   Best = 4,  Total = 40,  Floor = 1 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn, "OPTIMIZE TABLE \"Scores\" FINAL");
        await RawClickHouse.WaitForMutationsAsync(Conn, "Scores");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Player, toInt64(Best) AS Best, toInt64(Total) AS Total, toInt64(Floor) AS Floor " +
            "FROM \"Scores\" FINAL ORDER BY Player");

        Assert.Equal(2, rows.Count);
        Assert.Equal("alice", (string)rows[0]["Player"]!);
        Assert.Equal(25L, Convert.ToInt64(rows[0]["Best"]));
        Assert.Equal(300L, Convert.ToInt64(rows[0]["Total"]));
        Assert.Equal(3L, Convert.ToInt64(rows[0]["Floor"]));
        Assert.Equal("bob", (string)rows[1]["Player"]!);
        Assert.Equal(7L, Convert.ToInt64(rows[1]["Best"]));
        Assert.Equal(110L, Convert.ToInt64(rows[1]["Total"]));
        Assert.Equal(1L, Convert.ToInt64(rows[1]["Floor"]));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Score> Scores => Set<Score>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Score>(e =>
            {
                e.ToTable("Scores"); e.HasKey(x => x.Player);
                e.UseAggregatingMergeTree(x => x.Player);
                e.Property(x => x.Best).HasSimpleAggregateFunction("max");
                e.Property(x => x.Total).HasSimpleAggregateFunction("sum");
                e.Property(x => x.Floor).HasSimpleAggregateFunction("min");
            });
    }

    public class Score { public string Player { get; set; } = ""; public long Best { get; set; } public long Total { get; set; } public long Floor { get; set; } }
}
