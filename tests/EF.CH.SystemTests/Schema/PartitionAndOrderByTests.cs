using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EF.CH.Metadata;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Coverage of <c>HasPartitionByMonth/Day/Year</c> and free-form <c>HasPartitionBy</c>.
/// Asserts the partition expression appears in <c>engine_full</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class PartitionAndOrderByTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public PartitionAndOrderByTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task PartitionByMonth_RendersToYYYYMMInEngineFull()
    {
        await using var ctx = TestContextFactory.Create<MonthlyCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var ef = await RawClickHouse.EngineFullAsync(Conn, "PartMonth_Rows");
        Assert.Contains("toYYYYMM", ef);
        Assert.Contains("PARTITION BY", ef);
    }

    [Fact]
    public async Task PartitionByDay_RendersToYYYYMMDDInEngineFull()
    {
        await using var ctx = TestContextFactory.Create<DailyCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var ef = await RawClickHouse.EngineFullAsync(Conn, "PartDay_Rows");
        Assert.Contains("toYYYYMMDD", ef);
    }

    [Fact]
    public async Task PartitionByYear_RendersToYearInEngineFull()
    {
        await using var ctx = TestContextFactory.Create<YearlyCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var ef = await RawClickHouse.EngineFullAsync(Conn, "PartYear_Rows");
        Assert.Contains("toYear", ef);
    }

    [Fact]
    public async Task OrderByCompoundKey_RendersExactTupleInEngineFull()
    {
        await using var ctx = TestContextFactory.Create<CompoundCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        var ef = await RawClickHouse.EngineFullAsync(Conn, "PartCompound_Rows");
        // Must be the exact compound tuple — substring "Region" alone could be matched
        // by an unrelated mention or a single-column ORDER BY (Id) over a Region-bearing table.
        Assert.Matches(@"ORDER BY \(Region, Id\)", ef);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public DateTime At { get; set; }
        public string Region { get; set; } = "";
    }

    public sealed class MonthlyCtx(DbContextOptions<MonthlyCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("PartMonth_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.HasPartitionBy(x => x.At, PartitionGranularity.Month);
            });
    }
    public sealed class DailyCtx(DbContextOptions<DailyCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("PartDay_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.HasPartitionBy(x => x.At, PartitionGranularity.Day);
            });
    }
    public sealed class YearlyCtx(DbContextOptions<YearlyCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("PartYear_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.HasPartitionBy(x => x.At, PartitionGranularity.Year);
            });
    }
    public sealed class CompoundCtx(DbContextOptions<CompoundCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("PartCompound_Rows"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.Region, x.Id });
            });
    }
}
