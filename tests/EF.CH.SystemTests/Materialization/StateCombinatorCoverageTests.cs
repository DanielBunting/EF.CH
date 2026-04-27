using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// Dense coverage for every <c>-State</c> and <c>-StateIf</c> combinator
/// declared on <see cref="ClickHouseAggregates"/>. Each test deploys an AMT
/// with state columns populated via the typed <c>MaterializedView&lt;T&gt;().From&lt;S&gt;().DefinedAs(...)</c> —
/// so any missing wiring in <c>MaterializedViewSqlTranslator</c> throws at
/// <c>EnsureCreatedAsync</c> time — and reads back via raw SQL + matching
/// <c>-Merge</c>, comparing to in-memory ground truth where feasible.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class StateCombinatorCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public StateCombinatorCoverageTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    private static IReadOnlyList<Row> MakeData()
    {
        var rng = new Random(2026);
        return Enumerable.Range(0, 400).Select(_ => new Row
        {
            Id = Guid.NewGuid(),
            Bucket = _ % 3 == 0 ? "A" : (_ % 3 == 1 ? "B" : "C"),
            UserId = rng.Next(1, 40),
            Value = Math.Round(rng.NextDouble() * 100, 4),
            Weight = rng.Next(1, 5),
            IsError = rng.NextDouble() < 0.25,
        }).ToList();
    }

    [Fact]
    public async Task EveryStateCombinator_TranslatesAndRoundTrips()
    {
        await using var ctx = TestContextFactory.Create<StateCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var data = MakeData();
        ctx.Rows.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "AllStates");

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Bucket,
                   toInt64(countMerge(Count_s)) AS Count_s,
                   toFloat64(sumMerge(Sum_s)) AS Sum_s,
                   toFloat64(avgMerge(Avg_s)) AS Avg_s,
                   toFloat64(minMerge(Min_s)) AS Min_s,
                   toFloat64(maxMerge(Max_s)) AS Max_s,
                   toInt64(uniqMerge(Uniq_s)) AS Uniq_s,
                   toInt64(uniqExactMerge(UniqExact_s)) AS UniqExact_s,
                   toInt64(uniqCombinedMerge(UniqCombined_s)) AS UniqCombined_s,
                   toInt64(uniqCombined64Merge(UniqCombined64_s)) AS UniqCombined64_s,
                   toInt64(uniqHLL12Merge(UniqHLL12_s)) AS UniqHLL12_s,
                   toFloat64(anyMerge(Any_s)) AS Any_s,
                   toFloat64(anyLastMerge(AnyLast_s)) AS AnyLast_s,
                   toFloat64(medianMerge(Median_s)) AS Median_s,
                   toFloat64(stddevPopMerge(StddevPop_s)) AS StddevPop_s,
                   toFloat64(stddevSampMerge(StddevSamp_s)) AS StddevSamp_s,
                   toFloat64(varPopMerge(VarPop_s)) AS VarPop_s,
                   toFloat64(varSampMerge(VarSamp_s)) AS VarSamp_s
            FROM "AllStates"
            GROUP BY Bucket ORDER BY Bucket
            """);
        Assert.Equal(3, rows.Count);

        var expected = data.GroupBy(r => r.Bucket)
            .ToDictionary(g => g.Key, g => (
                Count: g.LongCount(),
                Sum: g.Sum(r => r.Value),
                Avg: g.Average(r => r.Value),
                Min: g.Min(r => r.Value),
                Max: g.Max(r => r.Value),
                Uniq: g.Select(r => r.UserId).Distinct().LongCount()));

        foreach (var row in rows)
        {
            var bucket = (string)row["Bucket"]!;
            var exp = expected[bucket];
            Assert.Equal(exp.Count, Convert.ToInt64(row["Count_s"]));
            // LINQ Sum and ClickHouse sum accumulate in different orders — tolerate drift.
            Assert.InRange(Convert.ToDouble(row["Sum_s"]), exp.Sum - 0.5, exp.Sum + 0.5);
            Assert.Equal(exp.Avg, Convert.ToDouble(row["Avg_s"]), 3);
            Assert.Equal(exp.Min, Convert.ToDouble(row["Min_s"]), 3);
            Assert.Equal(exp.Max, Convert.ToDouble(row["Max_s"]), 3);
            Assert.Equal(exp.Uniq, Convert.ToInt64(row["Uniq_s"]));
            Assert.Equal(exp.Uniq, Convert.ToInt64(row["UniqExact_s"]));
        }
    }

    [Fact]
    public async Task ParametricStates_TranslateAndRoundTrip()
    {
        await using var ctx = TestContextFactory.Create<ParametricCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(MakeData());
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "ParametricStates");

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Bucket,
                   toFloat64(quantileMerge(0.5)(Quantile_s)) AS Q,
                   toFloat64(quantileTDigestMerge(0.9)(QTD_s)) AS QTD,
                   toFloat64(quantileExactMerge(0.5)(QE_s)) AS QE,
                   toFloat64(quantileTimingMerge(0.95)(QT_s)) AS QT,
                   toFloat64(quantileDDMerge(0.5)(QDD_s)) AS QDD,
                   length(quantilesMerge(0.5, 0.95)(Qs_s)) AS QsLen,
                   length(quantilesTDigestMerge(0.5, 0.95)(QsTD_s)) AS QsTDLen
            FROM "ParametricStates" GROUP BY Bucket ORDER BY Bucket
            """);
        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.Equal(2UL, Convert.ToUInt64(r["QsLen"])));
        Assert.All(rows, r => Assert.Equal(2UL, Convert.ToUInt64(r["QsTDLen"])));
    }

    [Fact]
    public async Task ArrayAndArgStates_TranslateAndRoundTrip()
    {
        await using var ctx = TestContextFactory.Create<ArrayArgCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(MakeData());
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "ArrayArgStates");

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Bucket,
                   length(groupArrayMerge(GA_s)) AS GA,
                   length(groupUniqArrayMerge(GUA_s)) AS GUA,
                   length(topKMerge(3)(TK_s)) AS TK,
                   length(topKWeightedMerge(2)(TKW_s)) AS TKW,
                   toInt64(argMaxMerge(AM_s)) AS AM,
                   toInt64(argMinMerge(AN_s)) AS AN
            FROM "ArrayArgStates" GROUP BY Bucket ORDER BY Bucket
            """);
        Assert.Equal(3, rows.Count);
        Assert.All(rows, r => Assert.True(Convert.ToUInt64(r["TK"]) <= 3));
        Assert.All(rows, r => Assert.True(Convert.ToUInt64(r["TKW"]) <= 2));
    }

    [Fact]
    public async Task EveryStateIfCombinator_TranslatesAndRoundTrips()
    {
        await using var ctx = TestContextFactory.Create<StateIfCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var data = MakeData();
        ctx.Rows.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "AllStateIfs");

        var rows = await RawClickHouse.RowsAsync(Conn,
            """
            SELECT Bucket,
                   toInt64(countMerge(Count_sif)) AS Count_sif,
                   toFloat64(sumMerge(Sum_sif)) AS Sum_sif,
                   toFloat64(avgMerge(Avg_sif)) AS Avg_sif,
                   toFloat64(minMerge(Min_sif)) AS Min_sif,
                   toFloat64(maxMerge(Max_sif)) AS Max_sif,
                   toInt64(uniqMerge(Uniq_sif)) AS Uniq_sif,
                   toInt64(uniqExactMerge(UniqExact_sif)) AS UniqExact_sif
            FROM "AllStateIfs"
            GROUP BY Bucket ORDER BY Bucket
            """);
        Assert.Equal(3, rows.Count);

        var expected = data.GroupBy(r => r.Bucket)
            .ToDictionary(g => g.Key, g => (
                Count: g.LongCount(r => r.IsError),
                Sum: g.Where(r => r.Value > 50).Sum(r => r.Value),
                Uniq: g.Where(r => r.IsError).Select(r => r.UserId).Distinct().LongCount()));

        foreach (var row in rows)
        {
            var bucket = (string)row["Bucket"]!;
            var exp = expected[bucket];
            Assert.Equal(exp.Count, Convert.ToInt64(row["Count_sif"]));
            Assert.InRange(Convert.ToDouble(row["Sum_sif"]), exp.Sum - 1.0, exp.Sum + 1.0);
            Assert.Equal(exp.Uniq, Convert.ToInt64(row["Uniq_sif"]));
        }
    }

    [Fact]
    public async Task ParametricAndArrayStateIfs_Translate()
    {
        await using var ctx = TestContextFactory.Create<ParametricIfCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.AddRange(MakeData());
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "ParamIfs");

        // Just confirm the MV deployed and rows exist; parametric If-states are
        // approximate and this test exists to prove translator acceptance.
        var count = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT count() FROM \"ParamIfs\"");
        Assert.True(count > 0);
    }

    [Fact]
    public async Task Where_FilterBeforeGroupBy_IsAppliedInMvSql()
    {
        await using var ctx = TestContextFactory.Create<WhereCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var data = MakeData();
        ctx.Rows.AddRange(data);
        await ctx.SaveChangesAsync();
        await RawClickHouse.SettleMaterializationAsync(Conn, "ErrsOnlySum");

        // Expected: only error rows folded into the Summing target.
        var expected = data.Where(r => r.IsError).GroupBy(r => r.Bucket)
            .ToDictionary(g => g.Key, g => g.Sum(r => r.Value));
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Bucket, toFloat64(Total) AS Total FROM \"ErrsOnlySum\" FINAL ORDER BY Bucket");
        Assert.Equal(expected.Count, rows.Count);
        foreach (var row in rows)
        {
            var bucket = (string)row["Bucket"]!;
            Assert.InRange(Convert.ToDouble(row["Total"]),
                expected[bucket] - 0.5, expected[bucket] + 0.5);
        }
    }

    [Fact]
    public async Task UnsupportedLinqOperator_ThrowsAtModelBuildTime()
    {
        // OrderBy inside an MV is not a supported operator — the hardened default
        // arm in VisitMethodCall must fail loudly rather than silently drop it.
        var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await using var ctx = TestContextFactory.Create<UnsupportedOpCtx>(Conn);
            _ = ctx.Model; // force OnModelCreating
        });
        Assert.Contains("not supported in materialized view", ex.Message);
    }

    // ── Entities ───────────────────────────────────────────────────────────

    public class Row
    {
        public Guid Id { get; set; }
        public string Bucket { get; set; } = "";
        public long UserId { get; set; }
        public double Value { get; set; }
        public long Weight { get; set; }
        public bool IsError { get; set; }
    }

    public class AllStatesRow
    {
        public string Bucket { get; set; } = "";
        public byte[] Count_s { get; set; } = Array.Empty<byte>();
        public byte[] Sum_s { get; set; } = Array.Empty<byte>();
        public byte[] Avg_s { get; set; } = Array.Empty<byte>();
        public byte[] Min_s { get; set; } = Array.Empty<byte>();
        public byte[] Max_s { get; set; } = Array.Empty<byte>();
        public byte[] Uniq_s { get; set; } = Array.Empty<byte>();
        public byte[] UniqExact_s { get; set; } = Array.Empty<byte>();
        public byte[] UniqCombined_s { get; set; } = Array.Empty<byte>();
        public byte[] UniqCombined64_s { get; set; } = Array.Empty<byte>();
        public byte[] UniqHLL12_s { get; set; } = Array.Empty<byte>();
        public byte[] UniqTheta_s { get; set; } = Array.Empty<byte>();
        public byte[] Any_s { get; set; } = Array.Empty<byte>();
        public byte[] AnyLast_s { get; set; } = Array.Empty<byte>();
        public byte[] Median_s { get; set; } = Array.Empty<byte>();
        public byte[] StddevPop_s { get; set; } = Array.Empty<byte>();
        public byte[] StddevSamp_s { get; set; } = Array.Empty<byte>();
        public byte[] VarPop_s { get; set; } = Array.Empty<byte>();
        public byte[] VarSamp_s { get; set; } = Array.Empty<byte>();
    }

    public class ParametricRow
    {
        public string Bucket { get; set; } = "";
        public byte[] Quantile_s { get; set; } = Array.Empty<byte>();
        public byte[] QTD_s { get; set; } = Array.Empty<byte>();
        public byte[] QE_s { get; set; } = Array.Empty<byte>();
        public byte[] QT_s { get; set; } = Array.Empty<byte>();
        public byte[] QDD_s { get; set; } = Array.Empty<byte>();
        public byte[] Qs_s { get; set; } = Array.Empty<byte>();
        public byte[] QsTD_s { get; set; } = Array.Empty<byte>();
    }

    public class ArrayArgRow
    {
        public string Bucket { get; set; } = "";
        public byte[] GA_s { get; set; } = Array.Empty<byte>();
        public byte[] GUA_s { get; set; } = Array.Empty<byte>();
        public byte[] TK_s { get; set; } = Array.Empty<byte>();
        public byte[] TKW_s { get; set; } = Array.Empty<byte>();
        public byte[] AM_s { get; set; } = Array.Empty<byte>();
        public byte[] AN_s { get; set; } = Array.Empty<byte>();
    }

    public class AllStateIfsRow
    {
        public string Bucket { get; set; } = "";
        public byte[] Count_sif { get; set; } = Array.Empty<byte>();
        public byte[] Sum_sif { get; set; } = Array.Empty<byte>();
        public byte[] Avg_sif { get; set; } = Array.Empty<byte>();
        public byte[] Min_sif { get; set; } = Array.Empty<byte>();
        public byte[] Max_sif { get; set; } = Array.Empty<byte>();
        public byte[] Uniq_sif { get; set; } = Array.Empty<byte>();
        public byte[] UniqExact_sif { get; set; } = Array.Empty<byte>();
    }

    public class ParametricIfRow
    {
        public string Bucket { get; set; } = "";
        public byte[] Q_sif { get; set; } = Array.Empty<byte>();
        public byte[] GA_sif { get; set; } = Array.Empty<byte>();
        public byte[] TK_sif { get; set; } = Array.Empty<byte>();
    }

    public class WhereRow { public string Bucket { get; set; } = ""; public double Total { get; set; } }

    // ── Contexts ───────────────────────────────────────────────────────────

    public sealed class StateCtx(DbContextOptions<StateCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        public DbSet<AllStatesRow> Stats => Set<AllStatesRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<AllStatesRow>(e =>
            {
                e.ToTable("AllStates"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Count_s).HasAggregateFunction("count", typeof(ulong));
                e.Property(x => x.Sum_s).HasAggregateFunction("sum", typeof(double));
                e.Property(x => x.Avg_s).HasAggregateFunction("avg", typeof(double));
                e.Property(x => x.Min_s).HasAggregateFunction("min", typeof(double));
                e.Property(x => x.Max_s).HasAggregateFunction("max", typeof(double));
                e.Property(x => x.Uniq_s).HasAggregateFunction("uniq", typeof(long));
                e.Property(x => x.UniqExact_s).HasAggregateFunction("uniqExact", typeof(long));
                e.Property(x => x.UniqCombined_s).HasAggregateFunction("uniqCombined", typeof(long));
                e.Property(x => x.UniqCombined64_s).HasAggregateFunction("uniqCombined64", typeof(long));
                e.Property(x => x.UniqHLL12_s).HasAggregateFunction("uniqHLL12", typeof(long));
                e.Property(x => x.UniqTheta_s).HasColumnType("AggregateFunction(uniqTheta, Int64)");
                e.Property(x => x.Any_s).HasAggregateFunction("any", typeof(double));
                e.Property(x => x.AnyLast_s).HasAggregateFunction("anyLast", typeof(double));
                e.Property(x => x.Median_s).HasAggregateFunction("median", typeof(double));
                e.Property(x => x.StddevPop_s).HasAggregateFunction("stddevPop", typeof(double));
                e.Property(x => x.StddevSamp_s).HasAggregateFunction("stddevSamp", typeof(double));
                e.Property(x => x.VarPop_s).HasAggregateFunction("varPop", typeof(double));
                e.Property(x => x.VarSamp_s).HasAggregateFunction("varSamp", typeof(double));


            });
            mb.MaterializedView<AllStatesRow>().From<Row>().DefinedAs(src => src
                    .GroupBy(x => x.Bucket)
                    .Select(g => new AllStatesRow
                    {
                        Bucket = g.Key,
                        Count_s = g.CountState(),
                        Sum_s = g.SumState(x => x.Value),
                        Avg_s = g.AvgState(x => x.Value),
                        Min_s = g.MinState(x => x.Value),
                        Max_s = g.MaxState(x => x.Value),
                        Uniq_s = g.UniqState(x => x.UserId),
                        UniqExact_s = g.UniqExactState(x => x.UserId),
                        UniqCombined_s = g.UniqCombinedState(x => x.UserId),
                        UniqCombined64_s = g.UniqCombined64State(x => x.UserId),
                        UniqHLL12_s = g.UniqHLL12State(x => x.UserId),
                        UniqTheta_s = g.UniqThetaState(x => x.UserId),
                        Any_s = g.AnyState(x => x.Value),
                        AnyLast_s = g.AnyLastState(x => x.Value),
                        Median_s = g.MedianState(x => x.Value),
                        StddevPop_s = g.StddevPopState(x => x.Value),
                        StddevSamp_s = g.StddevSampState(x => x.Value),
                        VarPop_s = g.VarPopState(x => x.Value),
                        VarSamp_s = g.VarSampState(x => x.Value),
                    }));
        }
    }

    public sealed class ParametricCtx(DbContextOptions<ParametricCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        public DbSet<ParametricRow> Stats => Set<ParametricRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<ParametricRow>(e =>
            {
                e.ToTable("ParametricStates"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Quantile_s).HasColumnType("AggregateFunction(quantile(0.5), Float64)");
                e.Property(x => x.QTD_s).HasColumnType("AggregateFunction(quantileTDigest(0.9), Float64)");
                e.Property(x => x.QE_s).HasColumnType("AggregateFunction(quantileExact(0.5), Float64)");
                e.Property(x => x.QT_s).HasColumnType("AggregateFunction(quantileTiming(0.95), Float64)");
                e.Property(x => x.QDD_s).HasColumnType("AggregateFunction(quantileDD(0.01, 0.5), Float64)");
                e.Property(x => x.Qs_s).HasColumnType("AggregateFunction(quantiles(0.5, 0.95), Float64)");
                e.Property(x => x.QsTD_s).HasColumnType("AggregateFunction(quantilesTDigest(0.5, 0.95), Float64)");


            });
            mb.MaterializedView<ParametricRow>().From<Row>().DefinedAs(src => src
                    .GroupBy(x => x.Bucket)
                    .Select(g => new ParametricRow
                    {
                        Bucket = g.Key,
                        Quantile_s = g.QuantileState(0.5, x => x.Value),
                        QTD_s = g.QuantileTDigestState(0.9, x => x.Value),
                        QE_s = g.QuantileExactState(0.5, x => x.Value),
                        QT_s = g.QuantileTimingState(0.95, x => x.Value),
                        QDD_s = g.QuantileDDState(0.01, 0.5, x => x.Value),
                        Qs_s = g.QuantilesState(new[] { 0.5, 0.95 }, x => x.Value),
                        QsTD_s = g.QuantilesTDigestState(new[] { 0.5, 0.95 }, x => x.Value),
                    }));
        }
    }

    public sealed class ArrayArgCtx(DbContextOptions<ArrayArgCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        public DbSet<ArrayArgRow> Stats => Set<ArrayArgRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<ArrayArgRow>(e =>
            {
                e.ToTable("ArrayArgStates"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.GA_s).HasAggregateFunction("groupArray", typeof(long));
                e.Property(x => x.GUA_s).HasAggregateFunction("groupUniqArray", typeof(long));
                e.Property(x => x.TK_s).HasColumnType("AggregateFunction(topK(3), Int64)");
                e.Property(x => x.TKW_s).HasColumnType("AggregateFunction(topKWeighted(2), Int64, Int64)");
                e.Property(x => x.AM_s).HasAggregateFunction("argMax", typeof(long));
                e.Property(x => x.AN_s).HasAggregateFunction("argMin", typeof(long));


            });
            mb.MaterializedView<ArrayArgRow>().From<Row>().DefinedAs(src => src
                    .GroupBy(x => x.Bucket)
                    .Select(g => new ArrayArgRow
                    {
                        Bucket = g.Key,
                        GA_s = g.GroupArrayState(x => x.UserId),
                        GUA_s = g.GroupUniqArrayState(x => x.UserId),
                        TK_s = g.TopKState(3, x => x.UserId),
                        TKW_s = g.TopKWeightedState(2, x => x.UserId, x => x.Weight),
                        AM_s = g.ArgMaxState(x => x.UserId, x => x.Value),
                        AN_s = g.ArgMinState(x => x.UserId, x => x.Value),
                    }));
        }
    }

    public sealed class StateIfCtx(DbContextOptions<StateIfCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        public DbSet<AllStateIfsRow> Stats => Set<AllStateIfsRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<AllStateIfsRow>(e =>
            {
                e.ToTable("AllStateIfs"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Count_sif).HasAggregateFunction("countIf", typeof(ulong));
                e.Property(x => x.Sum_sif).HasAggregateFunction("sumIf", typeof(double));
                e.Property(x => x.Avg_sif).HasAggregateFunction("avgIf", typeof(double));
                e.Property(x => x.Min_sif).HasAggregateFunction("minIf", typeof(double));
                e.Property(x => x.Max_sif).HasAggregateFunction("maxIf", typeof(double));
                e.Property(x => x.Uniq_sif).HasAggregateFunction("uniqIf", typeof(long));
                e.Property(x => x.UniqExact_sif).HasAggregateFunction("uniqExactIf", typeof(long));


            });
            mb.MaterializedView<AllStateIfsRow>().From<Row>().DefinedAs(src => src
                    .GroupBy(x => x.Bucket)
                    .Select(g => new AllStateIfsRow
                    {
                        Bucket = g.Key,
                        Count_sif = g.CountStateIf(x => x.IsError),
                        Sum_sif = g.SumStateIf(x => x.Value, x => x.Value > 50),
                        Avg_sif = g.AvgStateIf(x => x.Value, x => x.IsError),
                        Min_sif = g.MinStateIf(x => x.Value, x => x.IsError),
                        Max_sif = g.MaxStateIf(x => x.Value, x => x.IsError),
                        Uniq_sif = g.UniqStateIf(x => x.UserId, x => x.IsError),
                        UniqExact_sif = g.UniqExactStateIf(x => x.UserId, x => x.IsError),
                    }));
        }
    }

    public sealed class ParametricIfCtx(DbContextOptions<ParametricIfCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        public DbSet<ParametricIfRow> Stats => Set<ParametricIfRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<ParametricIfRow>(e =>
            {
                e.ToTable("ParamIfs"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.Q_sif).HasColumnType("AggregateFunction(quantileIf(0.9), Float64, UInt8)");
                e.Property(x => x.GA_sif).HasColumnType("AggregateFunction(groupArrayIf, Int64, UInt8)");
                e.Property(x => x.TK_sif).HasColumnType("AggregateFunction(topKIf(3), Int64, UInt8)");


            });
            mb.MaterializedView<ParametricIfRow>().From<Row>().DefinedAs(src => src
                    .GroupBy(x => x.Bucket)
                    .Select(g => new ParametricIfRow
                    {
                        Bucket = g.Key,
                        Q_sif = g.QuantileStateIf(0.9, x => x.Value, x => x.IsError),
                        GA_sif = g.GroupArrayStateIf(x => x.UserId, x => x.IsError),
                        TK_sif = g.TopKStateIf(3, x => x.UserId, x => x.IsError),
                    }));
        }
    }

    public sealed class WhereCtx(DbContextOptions<WhereCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        public DbSet<WhereRow> Stats => Set<WhereRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<WhereRow>(e =>
            {
                e.ToTable("ErrsOnlySum"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Bucket);

            });
            mb.MaterializedView<WhereRow>().From<Row>().DefinedAs(src => src
                    .Where(x => x.IsError)
                    .GroupBy(x => x.Bucket)
                    .Select(g => new WhereRow { Bucket = g.Key, Total = g.Sum(x => x.Value) }));
        }
    }

    public sealed class UnsupportedOpCtx(DbContextOptions<UnsupportedOpCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        public DbSet<WhereRow> Stats => Set<WhereRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Bucket, x.Id }); });
            mb.Entity<WhereRow>(e =>
            {
                e.ToTable("Unsupported"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Bucket);
                // OrderBy inside an MV is not supported — the translator must throw.

            });
            mb.MaterializedView<WhereRow>().From<Row>().DefinedAs(src => src
                    .OrderBy(x => x.Id)
                    .GroupBy(x => x.Bucket)
                    .Select(g => new WhereRow { Bucket = g.Key, Total = g.Sum(x => x.Value) }));
        }
    }
}
