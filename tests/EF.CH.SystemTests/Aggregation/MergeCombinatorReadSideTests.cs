using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Aggregation;

/// <summary>
/// Exercises the <c>-Merge</c> combinators on the *query* side — i.e. reading
/// from an AggregatingMergeTree populated via <c>-State</c> combinators and
/// finalising the state blobs back into scalar values.
///
/// ClickHouseAggregates declares CountMerge / SumMerge / AvgMerge / etc., and
/// <c>ClickHouseAggregateMethodTranslator</c> emits the right SQL for them.
/// However, EF Core's <c>NavigationExpandingExpressionVisitor</c> — which runs
/// before the aggregate translator — rejects <c>g.CountMerge(...)</c> on a
/// grouping parameter with
///   "Translation of 'Select' which contains grouping parameter without
///    composition is not supported".
/// These tests document the end-to-end behaviour. Tests that currently need
/// raw SQL are explicit about why; the LINQ versions should start passing once
/// <c>ClickHouseQueryableMethodTranslatingExpressionVisitor</c> surfaces
/// <see cref="ClickHouseAggregates"/> grouping methods as recognised aggregates.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MergeCombinatorReadSideTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MergeCombinatorReadSideTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    private static IReadOnlyList<Raw> MakeData()
    {
        var rng = new Random(909);
        return Enumerable.Range(0, 300).Select(i => new Raw
        {
            Id = Guid.NewGuid(),
            Bucket = i % 3 == 0 ? "A" : (i % 3 == 1 ? "B" : "C"),
            UserId = rng.Next(1, 40),
            Value = Math.Round(rng.NextDouble() * 100, 4),
        }).ToList();
    }

    /// <summary>
    /// Raw-SQL path (baseline) — this is the pattern every sample currently uses
    /// to read an AMT back. Proves the data pipeline is correct so the later
    /// tests can focus on LINQ translation regressions.
    /// </summary>
    [Fact]
    public async Task RawSql_CountMerge_SumMerge_UniqMerge_ReturnsCorrectRollups()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var data = MakeData();
        ctx.Rows.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "Stats");

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Bucket,
                   toInt64(countMerge(Count_s)) AS c,
                   toFloat64(sumMerge(Sum_s)) AS s,
                   toInt64(uniqMerge(Uniq_s)) AS u
            FROM "Stats" GROUP BY Bucket ORDER BY Bucket
            """);

        var expected = data.GroupBy(r => r.Bucket)
            .ToDictionary(g => g.Key, g => (
                C: g.LongCount(),
                S: g.Sum(r => r.Value),
                U: g.Select(r => r.UserId).Distinct().LongCount()));

        Assert.Equal(3, rows.Count);
        foreach (var row in rows)
        {
            var b = (string)row["Bucket"]!;
            Assert.Equal(expected[b].C, Convert.ToInt64(row["c"]));
            Assert.Equal(expected[b].S, Convert.ToDouble(row["s"]), 3);
            Assert.Equal(expected[b].U, Convert.ToInt64(row["u"]));
        }
    }

    /// <summary>
    /// Typed LINQ path — uses extension-method syntax on an IGrouping and
    /// relies on ClickHouseQueryTranslationPreprocessor to rewrite the
    /// <c>-Merge</c> calls into a shape EF's navigation expander accepts.
    /// </summary>
    [Fact]
    public async Task Linq_CountMerge_SumMerge_UniqMerge_ReturnsCorrectRollups()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(MakeData());
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "Stats");

        // Does the typed LINQ form work end-to-end after the preprocessor rewrite?
        var rows = await ctx.Stats
            .GroupBy(s => s.Bucket)
            .Select(g => new Merged
            {
                Bucket = g.Key,
                Count = g.CountMerge(s => s.Count_s),
                Total = g.SumMerge<string, Stat, double>(s => s.Sum_s),
                Uniq = g.UniqMerge(s => s.Uniq_s),
            })
            .OrderBy(r => r.Bucket)
            .ToListAsync();

        var expected = MakeData().GroupBy(r => r.Bucket)
            .ToDictionary(g => g.Key, g => (
                C: g.LongCount(),
                S: g.Sum(r => r.Value),
                U: (ulong)g.Select(r => r.UserId).Distinct().LongCount()));

        Assert.Equal(3, rows.Count);
        foreach (var row in rows)
        {
            Assert.Equal(expected[row.Bucket].C, row.Count);
            Assert.Equal(expected[row.Bucket].S, row.Total, 3);
            Assert.Equal(expected[row.Bucket].U, row.Uniq);
        }
    }

    /// <summary>
    /// Exercises the broader <c>-Merge</c> family through typed LINQ to confirm
    /// every variant wired by the preprocessor round-trips correctly.
    /// </summary>
    [Fact]
    public async Task Linq_AllMergeVariants_RoundTrip()
    {
        await using var ctx = TestContextFactory.Create<BroadCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var data = MakeData();
        ctx.Rows.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "BroadStats");

        var rows = await ctx.BroadStats
            .GroupBy(s => s.Bucket)
            .Select(g => new BroadMerged
            {
                Bucket = g.Key,
                Count = g.CountMerge(s => s.Count_s),
                Sum = g.SumMerge<string, BroadStat, double>(s => s.Sum_s),
                Avg = g.AvgMerge(s => s.Avg_s),
                Min = g.MinMerge<string, BroadStat, double>(s => s.Min_s),
                Max = g.MaxMerge<string, BroadStat, double>(s => s.Max_s),
                Uniq = g.UniqMerge(s => s.Uniq_s),
                UniqExact = g.UniqExactMerge(s => s.UniqExact_s),
            })
            .OrderBy(r => r.Bucket)
            .ToListAsync();

        var expected = MakeData().GroupBy(r => r.Bucket)
            .ToDictionary(g => g.Key, g => (
                C: g.LongCount(),
                S: g.Sum(r => r.Value),
                Avg: g.Average(r => r.Value),
                Min: g.Min(r => r.Value),
                Max: g.Max(r => r.Value),
                U: (ulong)g.Select(r => r.UserId).Distinct().LongCount()));

        Assert.Equal(3, rows.Count);
        foreach (var row in rows)
        {
            var exp = expected[row.Bucket];
            Assert.Equal(exp.C, row.Count);
            Assert.Equal(exp.S, row.Sum, 3);
            Assert.Equal(exp.Avg, row.Avg, 3);
            Assert.Equal(exp.Min, row.Min, 3);
            Assert.Equal(exp.Max, row.Max, 3);
            Assert.Equal(exp.U, row.Uniq);
            Assert.Equal(exp.U, row.UniqExact);
        }
    }

    /// <summary>
    /// QuantileMerge uses the parametric sentinel path.
    /// </summary>
    [Fact]
    public async Task Linq_QuantileMerge_UsesParametricSentinel()
    {
        await using var ctx = TestContextFactory.Create<QuantileCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(MakeData());
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "QuantileStats");

        var rows = await ctx.QuantileStats
            .GroupBy(s => s.Bucket)
            .Select(g => new { Bucket = g.Key, P50 = g.QuantileMerge(0.5, s => s.Median_s) })
            .OrderBy(r => r.Bucket)
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.InRange(r.P50, 0.0, 100.0));
    }

    /// <summary>
    /// Without an outer GroupBy, reading a single state with <c>-Merge</c>
    /// through FromSqlInterpolated is functionally identical to using a raw
    /// SELECT — the state columns already carry the grouping from the MV.
    /// </summary>
    [Fact]
    public async Task FromSqlInterpolated_SurfacesTheMergedRollupAsAnEfProjection()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(MakeData());
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "Stats");

        var rows = await ctx.Database.SqlQueryRaw<Merged>(
            """
            SELECT Bucket AS Bucket,
                   toInt64(countMerge(Count_s)) AS Count,
                   toFloat64(sumMerge(Sum_s)) AS Total,
                   toUInt64(uniqMerge(Uniq_s)) AS Uniq
            FROM "Stats" GROUP BY Bucket ORDER BY Bucket
            """).ToListAsync();

        Assert.Equal(3, rows.Count);
    }

    // ── Entities / Context ────────────────────────────────────────────────

    public class Raw
    {
        public Guid Id { get; set; }
        public string Bucket { get; set; } = "";
        public long UserId { get; set; }
        public double Value { get; set; }
    }

    public class Stat
    {
        public string Bucket { get; set; } = "";
        public byte[] Count_s { get; set; } = Array.Empty<byte>();
        public byte[] Sum_s { get; set; } = Array.Empty<byte>();
        public byte[] Uniq_s { get; set; } = Array.Empty<byte>();
    }

    public class Merged
    {
        public string Bucket { get; set; } = "";
        public long Count { get; set; }
        public double Total { get; set; }
        public ulong Uniq { get; set; }
    }

    public class BroadStat
    {
        public string Bucket { get; set; } = "";
        public byte[] Count_s { get; set; } = Array.Empty<byte>();
        public byte[] Sum_s { get; set; } = Array.Empty<byte>();
        public byte[] Avg_s { get; set; } = Array.Empty<byte>();
        public byte[] Min_s { get; set; } = Array.Empty<byte>();
        public byte[] Max_s { get; set; } = Array.Empty<byte>();
        public byte[] Uniq_s { get; set; } = Array.Empty<byte>();
        public byte[] UniqExact_s { get; set; } = Array.Empty<byte>();
    }

    public class BroadMerged
    {
        public string Bucket { get; set; } = "";
        public long Count { get; set; }
        public double Sum { get; set; }
        public double Avg { get; set; }
        public double Min { get; set; }
        public double Max { get; set; }
        public ulong Uniq { get; set; }
        public ulong UniqExact { get; set; }
    }

    public class QuantileStat
    {
        public string Bucket { get; set; } = "";
        public byte[] Median_s { get; set; } = Array.Empty<byte>();
    }

    public sealed class BroadCtx(DbContextOptions<BroadCtx> o) : DbContext(o)
    {
        public DbSet<Raw> Rows => Set<Raw>();
        public DbSet<BroadStat> BroadStats => Set<BroadStat>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Raw>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<BroadStat>(e =>
            {
                e.ToTable("BroadStats"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Count_s).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.Sum_s).HasAggregateFunction("sum", typeof(double));
                e.Property(x => x.Avg_s).HasAggregateFunction("avg", typeof(double));
                e.Property(x => x.Min_s).HasAggregateFunction("min", typeof(double));
                e.Property(x => x.Max_s).HasAggregateFunction("max", typeof(double));
                e.Property(x => x.Uniq_s).HasAggregateFunction("uniq", typeof(long));
                e.Property(x => x.UniqExact_s).HasAggregateFunction("uniqExact", typeof(long));

            });
            mb.MaterializedView<BroadStat>().From<Raw>().DefinedAs(src => src
                    .GroupBy(x => x.Bucket)
                    .Select(g => new BroadStat
                    {
                        Bucket = g.Key,
                        Count_s = g.CountState(),
                        Sum_s = g.SumState(x => x.Value),
                        Avg_s = g.AvgState(x => x.Value),
                        Min_s = g.MinState(x => x.Value),
                        Max_s = g.MaxState(x => x.Value),
                        Uniq_s = g.UniqState(x => x.UserId),
                        UniqExact_s = g.UniqExactState(x => x.UserId),
                    }));
        }
    }

    public sealed class QuantileCtx(DbContextOptions<QuantileCtx> o) : DbContext(o)
    {
        public DbSet<Raw> Rows => Set<Raw>();
        public DbSet<QuantileStat> QuantileStats => Set<QuantileStat>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Raw>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<QuantileStat>(e =>
            {
                e.ToTable("QuantileStats"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Median_s).HasColumnType("AggregateFunction(quantile(0.5), Float64)");

            });
            mb.MaterializedView<QuantileStat>().From<Raw>().DefinedAs(src => src
                    .GroupBy(x => x.Bucket)
                    .Select(g => new QuantileStat { Bucket = g.Key, Median_s = g.QuantileState(0.5, x => x.Value) }));
        }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Raw> Rows => Set<Raw>();
        public DbSet<Stat> Stats => Set<Stat>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Raw>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<Stat>(e =>
            {
                e.ToTable("Stats"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Count_s).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.Sum_s).HasAggregateFunction("sum", typeof(double));
                e.Property(x => x.Uniq_s).HasAggregateFunction("uniq", typeof(long));

            });
            mb.MaterializedView<Stat>().From<Raw>().DefinedAs(src => src
                    .GroupBy(x => x.Bucket)
                    .Select(g => new Stat
                    {
                        Bucket = g.Key,
                        Count_s = g.CountState(),
                        Sum_s = g.SumState(x => x.Value),
                        Uniq_s = g.UniqState(x => x.UserId),
                    }));
        }
    }
}
