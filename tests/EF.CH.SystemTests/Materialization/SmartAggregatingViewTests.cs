using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

[Collection(SingleNodeCollection.Name)]
public class SmartAggregatingViewTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public SmartAggregatingViewTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MinMax_AcrossPartitions_RollUpViaAggregatingMergeTree()
    {
        await using var ctx = TestContextFactory.Create<MinMaxCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rng = new Random(11);
        var data = Enumerable.Range(0, 300).Select(_ => new Measurement
        {
            Id = Guid.NewGuid(),
            Sensor = "sensor-" + rng.Next(0, 4),
            Reading = Math.Round(rng.NextDouble() * 100, 4),
            At = new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(rng.Next(0, 1_000_000)),
        }).ToList();

        ctx.Measurements.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "SensorStats");

        var expected = data.GroupBy(d => d.Sensor)
            .Select(g => (Sensor: g.Key, Min: g.Min(x => x.Reading), Max: g.Max(x => x.Reading)))
            .OrderBy(x => x.Sensor).ToArray();

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Sensor, toFloat64(minMerge(Lowest)) AS Lowest, toFloat64(maxMerge(Highest)) AS Highest
            FROM "SensorStats" GROUP BY Sensor ORDER BY Sensor
            """);

        Assert.Equal(expected.Length, rows.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Sensor, (string)rows[i]["Sensor"]!);
            Assert.Equal(expected[i].Min, Convert.ToDouble(rows[i]["Lowest"]), 4);
            Assert.Equal(expected[i].Max, Convert.ToDouble(rows[i]["Highest"]), 4);
        }
    }

    [Fact]
    public async Task QuantileState_RollsUp_ApproximatelyMatchesInMemoryQuantile()
    {
        await using var ctx = TestContextFactory.Create<QuantileCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rng = new Random(22);
        var data = Enumerable.Range(0, 5000)
            .Select(_ => new Sample { Id = Guid.NewGuid(), Group = "uniform", Value = rng.NextDouble() })
            .ToList();

        ctx.Samples.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "GroupQuantiles");

        var row = (await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Group,
                   toFloat64(quantileMerge(0.5)(MedianValue)) AS P50,
                   toFloat64(quantileMerge(0.95)(MedianValue)) AS P95
            FROM "GroupQuantiles" GROUP BY Group
            """)).Single();

        Assert.InRange(Convert.ToDouble(row["P50"]), 0.45, 0.55);
        Assert.InRange(Convert.ToDouble(row["P95"]), 0.90, 0.99);
    }

    [Fact]
    public async Task TopKState_RollsUp_IntoMostFrequentValues()
    {
        await using var ctx = TestContextFactory.Create<TopKCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = Enumerable.Range(0, 100).Select(_ => new Visit { Id = Guid.NewGuid(), Page = "/home", Tag = "x" })
            .Concat(Enumerable.Range(0, 30).Select(_ => new Visit { Id = Guid.NewGuid(), Page = "/home", Tag = "y" }))
            .Concat(Enumerable.Range(0, 5).Select(_ => new Visit { Id = Guid.NewGuid(), Page = "/home", Tag = "z" }))
            .ToList();

        ctx.Visits.AddRange(rows);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "PageTopTags");

        var top = (await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Page, toString(arrayElement(topKMerge(3)(TopTags), 1)) AS First
            FROM "PageTopTags" GROUP BY Page
            """)).Single();

        Assert.Equal("/home", (string)top["Page"]!);
        Assert.Equal("x", (string)top["First"]!);
    }

    // ── Contexts ────────────────────────────────────────────────────────────

    public sealed class MinMaxCtx(DbContextOptions<MinMaxCtx> o) : DbContext(o)
    {
        public DbSet<Measurement> Measurements => Set<Measurement>();
        public DbSet<SensorStat> SensorStats => Set<SensorStat>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Measurement>(e =>
            {
                e.ToTable("Measurements"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.At, x.Id });
            });
            mb.Entity<SensorStat>(e =>
            {
                e.ToTable("SensorStats"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Sensor);
                e.Property(x => x.Lowest).HasAggregateFunction("min", typeof(double));
                e.Property(x => x.Highest).HasAggregateFunction("max", typeof(double));
                e.AsMaterializedView<SensorStat, Measurement>(src => src
                    .GroupBy(x => x.Sensor)
                    .Select(g => new SensorStat
                    {
                        Sensor = g.Key,
                        Lowest = g.MinState(x => x.Reading),
                        Highest = g.MaxState(x => x.Reading),
                    }));
            });
        }
    }

    public sealed class QuantileCtx(DbContextOptions<QuantileCtx> o) : DbContext(o)
    {
        public DbSet<Sample> Samples => Set<Sample>();
        public DbSet<GroupQuantile> GroupQuantiles => Set<GroupQuantile>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Sample>(e =>
            {
                e.ToTable("Samples"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
            mb.Entity<GroupQuantile>(e =>
            {
                e.ToTable("GroupQuantiles"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Group);
                e.Property(x => x.MedianValue).HasColumnType("AggregateFunction(quantile(0.5), Float64)");
                e.AsMaterializedView<GroupQuantile, Sample>(src => src
                    .GroupBy(x => x.Group)
                    .Select(g => new GroupQuantile
                    {
                        Group = g.Key,
                        MedianValue = g.QuantileState(0.5, x => x.Value),
                    }));
            });
        }
    }

    public sealed class TopKCtx(DbContextOptions<TopKCtx> o) : DbContext(o)
    {
        public DbSet<Visit> Visits => Set<Visit>();
        public DbSet<PageTopTags> PageTopTags => Set<PageTopTags>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Visit>(e =>
            {
                e.ToTable("Visits"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
            mb.Entity<PageTopTags>(e =>
            {
                e.ToTable("PageTopTags"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Page);
                e.Property(x => x.TopTags).HasColumnType("AggregateFunction(topK(3), String)");
                e.AsMaterializedView<PageTopTags, Visit>(src => src
                    .GroupBy(x => x.Page)
                    .Select(g => new PageTopTags
                    {
                        Page = g.Key,
                        TopTags = g.TopKState(3, x => x.Tag),
                    }));
            });
        }
    }

    public class Measurement { public Guid Id { get; set; } public string Sensor { get; set; } = ""; public double Reading { get; set; } public DateTime At { get; set; } }
    public class SensorStat { public string Sensor { get; set; } = ""; public byte[] Lowest { get; set; } = Array.Empty<byte>(); public byte[] Highest { get; set; } = Array.Empty<byte>(); }
    public class Sample { public Guid Id { get; set; } public string Group { get; set; } = ""; public double Value { get; set; } }
    public class GroupQuantile { public string Group { get; set; } = ""; public byte[] MedianValue { get; set; } = Array.Empty<byte>(); }
    public class Visit { public Guid Id { get; set; } public string Page { get; set; } = ""; public string Tag { get; set; } = ""; }
    public class PageTopTags { public string Page { get; set; } = ""; public byte[] TopTags { get; set; } = Array.Empty<byte>(); }
}
