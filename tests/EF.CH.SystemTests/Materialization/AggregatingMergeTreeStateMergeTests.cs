using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// Flagship materialization test — EF deploys a raw-events source feeding a
/// "smart" AggregatingMergeTree target declared with
/// <c>HasAggregateFunction</c> columns, and the assertions read the merged
/// states back through ClickHouse.Driver directly with <c>-Merge</c>
/// wrappers.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class AggregatingMergeTreeStateMergeTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public AggregatingMergeTreeStateMergeTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task SourceEvents_FeedAggregatingMergeTree_AndMergeReconstitutesAggregates()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "CategoryStats"));

        // Column types should be AggregateFunction(...) for each state blob.
        var cols = (await RawClickHouse.RowsAsync(Conn,
            "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'CategoryStats'"))
            .ToDictionary(r => (string)r["name"]!, r => (string)r["type"]!);
        Assert.Contains("AggregateFunction(count", cols["EventCount"]);
        Assert.Contains("AggregateFunction(sum", cols["TotalValue"]);
        Assert.Contains("AggregateFunction(uniq", cols["UniqueUsers"]);
        Assert.Contains("AggregateFunction(avg", cols["AverageValue"]);

        var rng = new Random(1234);
        var raw = new List<RawEvent>();
        var baseTime = new DateTime(2026, 2, 1, 0, 0, 0, DateTimeKind.Utc);
        string[] categories = { "alpha", "beta", "gamma" };
        for (int i = 0; i < 500; i++)
        {
            raw.Add(new RawEvent
            {
                Id = Guid.NewGuid(),
                Timestamp = baseTime.AddSeconds(i),
                Category = categories[rng.Next(categories.Length)],
                UserId = rng.Next(1, 40),
                Value = rng.NextDouble() * 1000,
            });
        }

        ctx.RawEvents.AddRange(raw);
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "CategoryStats");

        var expected = raw
            .GroupBy(r => r.Category)
            .Select(g => (
                Category: g.Key,
                Count: g.LongCount(),
                Sum: g.Sum(x => x.Value),
                Uniq: g.Select(x => x.UserId).Distinct().LongCount(),
                Avg: g.Average(x => x.Value)))
            .OrderBy(x => x.Category)
            .ToArray();

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Category,
                   toInt64(countMerge(EventCount)) AS EventCount,
                   toFloat64(sumMerge(TotalValue)) AS TotalValue,
                   toInt64(uniqMerge(UniqueUsers)) AS UniqueUsers,
                   toFloat64(avgMerge(AverageValue)) AS AverageValue
            FROM "CategoryStats"
            GROUP BY Category
            ORDER BY Category
            """);

        Assert.Equal(expected.Length, rows.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Category, (string)rows[i]["Category"]!);
            Assert.Equal(expected[i].Count, Convert.ToInt64(rows[i]["EventCount"]));
            Assert.Equal(expected[i].Sum, Convert.ToDouble(rows[i]["TotalValue"]), 1);
            Assert.Equal(expected[i].Uniq, Convert.ToInt64(rows[i]["UniqueUsers"]));
            Assert.Equal(expected[i].Avg, Convert.ToDouble(rows[i]["AverageValue"]), 3);
        }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RawEvent> RawEvents => Set<RawEvent>();
        public DbSet<CategoryStat> CategoryStats => Set<CategoryStat>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawEvent>(e =>
            {
                e.ToTable("RawEvents"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.Timestamp, x.Id });
            });

            mb.Entity<CategoryStat>(e =>
            {
                e.ToTable("CategoryStats"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Category);
                e.Property(x => x.EventCount).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.TotalValue).HasAggregateFunction("sum", typeof(double));
                e.Property(x => x.UniqueUsers).HasAggregateFunction("uniq", typeof(long));
                e.Property(x => x.AverageValue).HasAggregateFunction("avg", typeof(double));


            });
            mb.MaterializedView<CategoryStat>().From<RawEvent>().DefinedAs(events => events
                    .GroupBy(x => x.Category)
                    .Select(g => new CategoryStat
                    {
                        Category = g.Key,
                        EventCount = g.CountState(),
                        TotalValue = g.SumState(x => x.Value),
                        UniqueUsers = g.UniqState(x => x.UserId),
                        AverageValue = g.AvgState(x => x.Value),
                    }));
        }
    }

    public class RawEvent
    {
        public Guid Id { get; set; }
        public DateTime Timestamp { get; set; }
        public string Category { get; set; } = "";
        public long UserId { get; set; }
        public double Value { get; set; }
    }

    public class CategoryStat
    {
        public string Category { get; set; } = "";
        public byte[] EventCount { get; set; } = Array.Empty<byte>();
        public byte[] TotalValue { get; set; } = Array.Empty<byte>();
        public byte[] UniqueUsers { get; set; } = Array.Empty<byte>();
        public byte[] AverageValue { get; set; } = Array.Empty<byte>();
    }
}
