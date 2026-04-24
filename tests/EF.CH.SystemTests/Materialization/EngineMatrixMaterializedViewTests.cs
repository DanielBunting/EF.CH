using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// MV source-engine × target-engine matrix. EF deploys, EF inserts (where
/// keyable), raw SQL verifies.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class EngineMatrixMaterializedViewTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public EngineMatrixMaterializedViewTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task MergeTree_To_SummingMergeTree()
    {
        await using var ctx = TestContextFactory.Create<MergeTreeToSumming.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new MergeTreeToSumming.Row { Id = 1, Bucket = "a", Value = 5 },
            new MergeTreeToSumming.Row { Id = 2, Bucket = "a", Value = 7 },
            new MergeTreeToSumming.Row { Id = 3, Bucket = "b", Value = 3 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MergeToSumTarget");

        var totals = await RawClickHouse.RowsAsync(Conn,
            "SELECT Bucket, toInt64(Total) AS Total FROM \"MergeToSumTarget\" FINAL ORDER BY Bucket");
        Assert.Equal(new[] { 12L, 3L }, totals.Select(r => Convert.ToInt64(r["Total"])).ToArray());
    }

    [Fact]
    public async Task NullEngine_To_AggregatingMergeTree()
    {
        await using var ctx = TestContextFactory.Create<NullToAgg.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rng = new Random(7);
        var data = Enumerable.Range(0, 200).Select(_ => new NullToAgg.Row
        {
            Key = _ % 5,
            UserId = rng.Next(1, 20),
            Amount = rng.NextDouble() * 100,
        }).ToList();

        // Null engine source is keyless — insert via hand-rolled SQL.
        foreach (var r in data)
            await RawClickHouse.ExecuteAsync(Conn,
                $"INSERT INTO \"Incoming\" (\"Key\", \"UserId\", \"Amount\") VALUES ({r.Key}, {r.UserId}, {r.Amount.ToString(System.Globalization.CultureInfo.InvariantCulture)})");

        await RawClickHouse.SettleMaterializationAsync(Conn, "NullAggTarget");
        Assert.Equal(0UL, await RawClickHouse.RowCountAsync(Conn, "Incoming"));

        var expected = data.GroupBy(d => d.Key)
            .Select(g => (Key: g.Key, Amt: g.Sum(x => x.Amount), Uniq: g.Select(x => x.UserId).Distinct().LongCount()))
            .OrderBy(x => x.Key).ToArray();

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Key, toFloat64(sumMerge(AmountTotal)) AS Amt, toInt64(uniqMerge(UserCount)) AS Uniq
            FROM "NullAggTarget" GROUP BY Key ORDER BY Key
            """);

        Assert.Equal(expected.Length, rows.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Key, Convert.ToInt64(rows[i]["Key"]));
            Assert.Equal(expected[i].Amt, Convert.ToDouble(rows[i]["Amt"]), 1);
            Assert.Equal(expected[i].Uniq, Convert.ToInt64(rows[i]["Uniq"]));
        }
    }

    [Fact]
    public async Task NullEngine_To_SummingMergeTree()
    {
        await using var ctx = TestContextFactory.Create<NullToSum.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"Ingest\" (\"Region\", \"Hits\") VALUES ('eu', 5), ('eu', 12), ('us', 8)");

        await RawClickHouse.SettleMaterializationAsync(Conn, "NullSumTarget");
        Assert.Equal(0UL, await RawClickHouse.RowCountAsync(Conn, "Ingest"));

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Region, toInt64(Hits) AS Hits FROM \"NullSumTarget\" FINAL ORDER BY Region");
        Assert.Equal("eu", (string)rows[0]["Region"]!); Assert.Equal(17L, Convert.ToInt64(rows[0]["Hits"]));
        Assert.Equal("us", (string)rows[1]["Region"]!); Assert.Equal(8L,  Convert.ToInt64(rows[1]["Hits"]));
    }

    [Fact]
    public async Task MergeTree_To_ReplacingMergeTree()
    {
        await using var ctx = TestContextFactory.Create<MergeTreeToReplacing.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new MergeTreeToReplacing.Row { Id = 1, Version = 1, Name = "first" },
            new MergeTreeToReplacing.Row { Id = 1, Version = 3, Name = "third" },
            new MergeTreeToReplacing.Row { Id = 1, Version = 2, Name = "second" },
            new MergeTreeToReplacing.Row { Id = 2, Version = 1, Name = "alpha" });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MergeToReplacingTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Id, Name, toInt64(Version) AS Version FROM \"MergeToReplacingTarget\" FINAL ORDER BY Id");
        Assert.Equal(2, rows.Count);
        Assert.Equal("third", (string)rows[0]["Name"]!); Assert.Equal(3L, Convert.ToInt64(rows[0]["Version"]));
        Assert.Equal("alpha", (string)rows[1]["Name"]!); Assert.Equal(1L, Convert.ToInt64(rows[1]["Version"]));
    }

    [Fact]
    public async Task MergeTree_To_MergeTree_PassThrough()
    {
        await using var ctx = TestContextFactory.Create<MergeTreeToMergeTree.Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new MergeTreeToMergeTree.Row { Id = 1, Level = "info", Message = "a" },
            new MergeTreeToMergeTree.Row { Id = 2, Level = "warn", Message = "b" },
            new MergeTreeToMergeTree.Row { Id = 3, Level = "error", Message = "c" });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "ErrorsOnly");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Id, Message FROM \"ErrorsOnly\" ORDER BY Id");
        Assert.Single(rows);
        Assert.Equal(3L, Convert.ToInt64(rows[0]["Id"]));
        Assert.Equal("c", (string)rows[0]["Message"]!);
    }

    // ── Contexts per case ────────────────────────────────────────────────

    public static class MergeTreeToSumming
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("MergeSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MergeToSumTarget"); e.HasNoKey();
                    e.UseSummingMergeTree(x => x.Bucket);
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .GroupBy(r => r.Bucket)
                        .Select(g => new Target { Bucket = g.Key, Total = g.Sum(r => r.Value) }));
                });
            }
        }
        public class Row { public long Id { get; set; } public string Bucket { get; set; } = ""; public long Value { get; set; } }
        public class Target { public string Bucket { get; set; } = ""; public long Total { get; set; } }
    }

    public static class NullToAgg
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Incoming => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("Incoming"); e.HasNoKey(); e.UseNullEngine(); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("NullAggTarget"); e.HasNoKey();
                    e.UseAggregatingMergeTree(x => x.Key);
                    e.Property(x => x.AmountTotal).HasAggregateFunction("sum", typeof(double));
                    e.Property(x => x.UserCount).HasAggregateFunction("uniq", typeof(long));
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .GroupBy(r => r.Key)
                        .Select(g => new Target
                        {
                            Key = g.Key,
                            AmountTotal = g.SumState(r => r.Amount),
                            UserCount = g.UniqState(r => r.UserId),
                        }));
                });
            }
        }
        public class Row { public long Key { get; set; } public long UserId { get; set; } public double Amount { get; set; } }
        public class Target { public long Key { get; set; } public byte[] AmountTotal { get; set; } = Array.Empty<byte>(); public byte[] UserCount { get; set; } = Array.Empty<byte>(); }
    }

    public static class NullToSum
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Ingest => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("Ingest"); e.HasNoKey(); e.UseNullEngine(); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("NullSumTarget"); e.HasNoKey();
                    e.UseSummingMergeTree(x => x.Region);
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .GroupBy(r => r.Region)
                        .Select(g => new Target { Region = g.Key, Hits = g.Sum(r => r.Hits) }));
                });
            }
        }
        public class Row { public string Region { get; set; } = ""; public long Hits { get; set; } }
        public class Target { public string Region { get; set; } = ""; public long Hits { get; set; } }
    }

    public static class MergeTreeToReplacing
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("ReplacingSource"); e.HasKey(x => new { x.Id, x.Version }); e.UseMergeTree(x => new { x.Id, x.Version }); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("MergeToReplacingTarget"); e.HasNoKey();
                    e.UseReplacingMergeTree("Version", "Id");
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .Select(r => new Target { Id = r.Id, Name = r.Name, Version = r.Version }));
                });
            }
        }
        public class Row { public long Id { get; set; } public long Version { get; set; } public string Name { get; set; } = ""; }
        public class Target { public long Id { get; set; } public string Name { get; set; } = ""; public long Version { get; set; } }
    }

    public static class MergeTreeToMergeTree
    {
        public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
        {
            public DbSet<Row> Source => Set<Row>();
            public DbSet<Target> Target => Set<Target>();
            protected override void OnModelCreating(ModelBuilder mb)
            {
                mb.Entity<Row>(e => { e.ToTable("LogSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
                mb.Entity<Target>(e =>
                {
                    e.ToTable("ErrorsOnly"); e.HasNoKey();
                    e.UseMergeTree(x => x.Id);
                    e.AsMaterializedView<Target, Row>(rows => rows
                        .Where(r => r.Level == "error")
                        .Select(r => new Target { Id = r.Id, Message = r.Message }));
                });
            }
        }
        public class Row { public long Id { get; set; } public string Level { get; set; } = ""; public string Message { get; set; } = ""; }
        public class Target { public long Id { get; set; } public string Message { get; set; } = ""; }
    }
}
