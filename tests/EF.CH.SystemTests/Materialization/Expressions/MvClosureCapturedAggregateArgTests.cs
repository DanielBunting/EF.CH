using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// Pins <c>ExtractConstantValue</c>'s closure-capture arm: parametric aggregate
/// arguments (<c>level</c>, <c>k</c>, <c>accuracy</c>, <c>levels[]</c>) read
/// from a captured local rather than an inline literal. The compiler lowers
/// closure captures to <c>MemberExpression</c> on a <c>ConstantExpression</c>
/// carrying the closure-display-class instance; the translator must reflect
/// the field/property to extract the value.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvClosureCapturedAggregateArgTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvClosureCapturedAggregateArgTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Level_FromClosureLocal()
    {
        double level = 0.95;
        await using var ctx = new LevelCtx(LevelCtx.Build(Conn), level);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        for (int i = 0; i < 50; i++)
            ctx.Source.Add(new Row { Id = i + 1, Latency = i * 2.0 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvClosureLevelTarget");

        var p = await RawClickHouse.ScalarAsync<double>(Conn,
            "SELECT toFloat64(Q) FROM \"MvClosureLevelTarget\" FINAL");
        Assert.InRange(p, 80.0, 100.0); // 0.95 quantile of 0..98
    }

    [Fact]
    public async Task K_FromClosureLocal()
    {
        int k = 3;
        await using var ctx = new KCtx(KCtx.Build(Conn), k);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        for (int i = 0; i < 20; i++)
            ctx.Source.Add(new Row { Id = i + 1, Latency = i % 5 });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvClosureKTarget");

        var len = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT length(Top) FROM \"MvClosureKTarget\" FINAL");
        Assert.Equal(3UL, len);
    }

    [Fact]
    public async Task Accuracy_FromClosureLocal()
    {
        double accuracy = 0.01;
        double level = 0.5;
        await using var ctx = new AccuracyCtx(AccuracyCtx.Build(Conn), accuracy, level);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        for (int i = 0; i < 40; i++)
            ctx.Source.Add(new Row { Id = i + 1, Latency = i });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvClosureAccTarget");

        var p = await RawClickHouse.ScalarAsync<double>(Conn,
            "SELECT toFloat64(Q) FROM \"MvClosureAccTarget\" FINAL");
        Assert.InRange(p, 15.0, 25.0); // median of 0..39 ~ 19.5
    }

    [Fact]
    public async Task LevelsArray_FromClosureLocal()
    {
        double[] levels = new[] { 0.5, 0.9 };
        await using var ctx = new LevelsArrayCtx(LevelsArrayCtx.Build(Conn), levels);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();
        for (int i = 0; i < 30; i++)
            ctx.Source.Add(new Row { Id = i + 1, Latency = i });
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "MvClosureLevelsTarget");

        var len = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT length(Quantiles) FROM \"MvClosureLevelsTarget\" FINAL");
        Assert.Equal(2UL, len);
    }

    public sealed class Row { public long Id { get; set; } public double Latency { get; set; } }
    public sealed class Tgt { public long Bucket { get; set; } public double Q { get; set; } }
    public sealed class TopTgt { public long Bucket { get; set; } public double[] Top { get; set; } = Array.Empty<double>(); }
    public sealed class QsTgt { public long Bucket { get; set; } public double[] Quantiles { get; set; } = Array.Empty<double>(); }

    public sealed class LevelCtx(DbContextOptions<LevelCtx> o, double level) : DbContext(o)
    {
        private readonly double _level = level;
        public DbSet<Row> Source => Set<Row>();
        public DbSet<Tgt> Target => Set<Tgt>();
        public static DbContextOptions<LevelCtx> Build(string conn) =>
            new DbContextOptionsBuilder<LevelCtx>().UseClickHouse(conn).Options;
        protected override void OnModelCreating(ModelBuilder mb)
        {
            var capturedLevel = _level;
            mb.Entity<Row>(e => { e.ToTable("MvClosureLevelSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvClosureLevelTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);
                e.AsMaterializedView<Tgt, Row>(rows => rows
                    .GroupBy(_ => 1L)
                    .Select(g => new Tgt
                    {
                        Bucket = g.Key,
                        Q = g.Quantile(capturedLevel, r => r.Latency),
                    }));
            });
        }
    }

    public sealed class KCtx(DbContextOptions<KCtx> o, int k) : DbContext(o)
    {
        private readonly int _k = k;
        public DbSet<Row> Source => Set<Row>();
        public DbSet<TopTgt> Target => Set<TopTgt>();
        public static DbContextOptions<KCtx> Build(string conn) =>
            new DbContextOptionsBuilder<KCtx>().UseClickHouse(conn).Options;
        protected override void OnModelCreating(ModelBuilder mb)
        {
            var capturedK = _k;
            mb.Entity<Row>(e => { e.ToTable("MvClosureKSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<TopTgt>(e =>
            {
                e.ToTable("MvClosureKTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);
                e.AsMaterializedView<TopTgt, Row>(rows => rows
                    .GroupBy(_ => 1L)
                    .Select(g => new TopTgt
                    {
                        Bucket = g.Key,
                        Top = g.TopK(capturedK, r => r.Latency),
                    }));
            });
        }
    }

    public sealed class AccuracyCtx(DbContextOptions<AccuracyCtx> o, double accuracy, double level) : DbContext(o)
    {
        private readonly double _accuracy = accuracy;
        private readonly double _level = level;
        public DbSet<Row> Source => Set<Row>();
        public DbSet<Tgt> Target => Set<Tgt>();
        public static DbContextOptions<AccuracyCtx> Build(string conn) =>
            new DbContextOptionsBuilder<AccuracyCtx>().UseClickHouse(conn).Options;
        protected override void OnModelCreating(ModelBuilder mb)
        {
            var capturedAccuracy = _accuracy;
            var capturedLevel = _level;
            mb.Entity<Row>(e => { e.ToTable("MvClosureAccSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvClosureAccTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);
                e.AsMaterializedView<Tgt, Row>(rows => rows
                    .GroupBy(_ => 1L)
                    .Select(g => new Tgt
                    {
                        Bucket = g.Key,
                        Q = g.QuantileDD(capturedAccuracy, capturedLevel, r => r.Latency),
                    }));
            });
        }
    }

    public sealed class LevelsArrayCtx(DbContextOptions<LevelsArrayCtx> o, double[] levels) : DbContext(o)
    {
        private readonly double[] _levels = levels;
        public DbSet<Row> Source => Set<Row>();
        public DbSet<QsTgt> Target => Set<QsTgt>();
        public static DbContextOptions<LevelsArrayCtx> Build(string conn) =>
            new DbContextOptionsBuilder<LevelsArrayCtx>().UseClickHouse(conn).Options;
        protected override void OnModelCreating(ModelBuilder mb)
        {
            var capturedLevels = _levels;
            mb.Entity<Row>(e => { e.ToTable("MvClosureLevelsSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<QsTgt>(e =>
            {
                e.ToTable("MvClosureLevelsTarget"); e.HasNoKey(); e.UseSummingMergeTree(x => x.Bucket);
                e.AsMaterializedView<QsTgt, Row>(rows => rows
                    .GroupBy(_ => 1L)
                    .Select(g => new QsTgt
                    {
                        Bucket = g.Key,
                        Quantiles = g.Quantiles(capturedLevels, r => r.Latency),
                    }));
            });
        }
    }
}
