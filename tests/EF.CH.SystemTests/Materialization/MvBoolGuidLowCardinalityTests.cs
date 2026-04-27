using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization;

/// <summary>
/// MV target round-trip coverage for non-numeric CLR types: bool, Guid,
/// LowCardinality(String). Each is at risk of the same column-type-inference
/// drift that hit numeric columns (e.g. ClickHouse stores bool as UInt8 by
/// default, so the natural column type may not match what the entity declared).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvBoolGuidLowCardinalityTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public MvBoolGuidLowCardinalityTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task BoolGuidLowCardinality_RoundTripThroughMv()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var g = Guid.NewGuid();
        ctx.Sources.AddRange(
            new Src { Id = 1, Region = "us", Active = true, Owner = g, Amount = 100 },
            new Src { Id = 2, Region = "us", Active = true, Owner = g, Amount = 200 },
            new Src { Id = 3, Region = "us", Active = false, Owner = g, Amount = 50 },
            new Src { Id = 4, Region = "eu", Active = true, Owner = Guid.NewGuid(), Amount = 75 });
        await ctx.SaveChangesAsync();

        await ctx.Database.ExecuteSqlRawAsync("OPTIMIZE TABLE mv_bg_target FINAL");

        var rows = await ctx.Targets.OrderBy(t => t.Region).ToListAsync();
        Assert.Equal(2, rows.Count);

        var us = rows.Single(r => r.Region == "us");
        Assert.True(us.AnyActive);  // any() over (true, true, false) yields a bool
        Assert.Equal(g, us.AnyOwner); // any() picks one Guid value
        Assert.Equal(350, us.Total);

        var eu = rows.Single(r => r.Region == "eu");
        Assert.True(eu.AnyActive);
        Assert.Equal(75, eu.Total);
    }

    public sealed class Src
    {
        public uint Id { get; set; }
        public string Region { get; set; } = "";
        public bool Active { get; set; }
        public Guid Owner { get; set; }
        public long Amount { get; set; }
    }

    public sealed class Tgt
    {
        public string Region { get; set; } = "";
        public bool AnyActive { get; set; }
        public Guid AnyOwner { get; set; }
        public long Total { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("mv_bg_src"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Region).HasLowCardinality();
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("mv_bg_target"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Region);
                e.Property(x => x.Region).HasLowCardinality();
                e.Property(x => x.AnyActive).HasSimpleAggregateFunction("any");
                e.Property(x => x.AnyOwner).HasSimpleAggregateFunction("any");
                e.Property(x => x.Total).HasSimpleAggregateFunction("sum");

            });
            mb.MaterializedView<Tgt>().From<Src>().DefinedAs(src => src
                    .GroupBy(s => s.Region)
                    .Select(g => new Tgt
                    {
                        Region = g.Key,
                        AnyActive = g.AnyValue(x => x.Active),
                        AnyOwner = g.AnyValue(x => x.Owner),
                        Total = g.Sum(x => x.Amount),
                    }));
        }
    }
}
