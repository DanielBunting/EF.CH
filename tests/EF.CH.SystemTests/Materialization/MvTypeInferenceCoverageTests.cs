using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// Covers MV target column-type inference for non-primitive CLR types — the
/// risk class that originally surfaced as <c>Byte → Int64</c> in
/// EventAnalyticsSample. ClickHouse's <c>CREATE MV ... ENGINE = ... AS SELECT</c>
/// derives column types from the SELECT, so without a CLR-aware cast wrapper
/// the entity's declared property types are silently overridden.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvTypeInferenceCoverageTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MvTypeInferenceCoverageTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task DateTime_NumericLiteralProjection_PersistsAsExpectedType()
    {
        await using var ctx = TestContextFactory.Create<MvCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var t = new DateTime(2024, 6, 15, 10, 0, 0, DateTimeKind.Utc);
        ctx.Sources.AddRange(
            new SourceRow { Id = 1, Group = "a", Amount = 1.50m, Created = t },
            new SourceRow { Id = 2, Group = "a", Amount = 2.25m, Created = t.AddHours(1) },
            new SourceRow { Id = 3, Group = "b", Amount = 9.99m, Created = t.AddHours(2) });
        await ctx.SaveChangesAsync();

        await ctx.Database.ExecuteSqlRawAsync("OPTIMIZE TABLE mv_targets FINAL");

        var rows = await ctx.Targets.OrderBy(t => t.Group).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("a", rows[0].Group);
        Assert.Equal(2, rows[0].Count);
        Assert.Equal(3.75m, rows[0].Total);
        Assert.True(rows[0].LatestSeen >= t, $"Expected DateTime to round-trip; got {rows[0].LatestSeen}");
    }

    public sealed class SourceRow
    {
        public uint Id { get; set; }
        public string Group { get; set; } = "";
        public decimal Amount { get; set; }
        public DateTime Created { get; set; }
    }

    public sealed class MvTarget
    {
        public string Group { get; set; } = "";
        public long Count { get; set; }
        public decimal Total { get; set; }
        public DateTime LatestSeen { get; set; }
    }

    public sealed class MvCtx(DbContextOptions<MvCtx> o) : DbContext(o)
    {
        public DbSet<SourceRow> Sources => Set<SourceRow>();
        public DbSet<MvTarget> Targets => Set<MvTarget>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<SourceRow>(e =>
            {
                e.ToTable("mv_source"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
                e.Property(x => x.Amount).HasColumnType("Decimal(18, 4)");
            });
            mb.Entity<MvTarget>(e =>
            {
                e.ToTable("mv_targets"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Group);
                e.Property(x => x.Total).HasColumnType("Decimal(18, 4)");
                e.Property(x => x.LatestSeen).HasSimpleAggregateFunction("max");

            });
            mb.MaterializedView<MvTarget>().From<SourceRow>().DefinedAs(src => src
                    .GroupBy(s => s.Group)
                    .Select(g => new MvTarget
                    {
                        Group = g.Key,
                        Count = g.Count(),
                        Total = g.Sum(x => x.Amount),
                        LatestSeen = g.Max(x => x.Created),
                    }));
        }
    }
}
