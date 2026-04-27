using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>
/// MV with <c>Guid</c>/<c>UUID</c> as the GROUP BY key. Existing
/// MvBoolGuidLowCardinalityTests covers Guid as an aggregated value via
/// <c>any()</c>; this complements it by exercising the key role.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvUuidKeyRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvUuidKeyRoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Uuid_GroupByKey_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var a = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var b = Guid.Parse("22222222-2222-2222-2222-222222222222");
        ctx.Sources.AddRange(
            new Src { Id = 1, Tenant = a, Hits = 10 },
            new Src { Id = 2, Tenant = a, Hits = 15 },
            new Src { Id = 3, Tenant = b, Hits =  7 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvUuidKeyTarget");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toString(Tenant) AS Tenant, toInt64(Hits) AS Hits FROM \"MvUuidKeyTarget\" FINAL ORDER BY Tenant");
        Assert.Equal(2, rows.Count);
        Assert.Equal(a.ToString(), (string)rows[0]["Tenant"]!); Assert.Equal(25L, Convert.ToInt64(rows[0]["Hits"]));
        Assert.Equal(b.ToString(), (string)rows[1]["Tenant"]!); Assert.Equal( 7L, Convert.ToInt64(rows[1]["Hits"]));
    }

    public sealed class Src { public uint Id { get; set; } public Guid Tenant { get; set; } public long Hits { get; set; } }
    public sealed class Tgt { public Guid Tenant { get; set; } public long Hits { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvUuidKeySource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvUuidKeyTarget"); e.HasNoKey();
                e.UseSummingMergeTree(x => x.Tenant);

            });
            mb.MaterializedView<Tgt>().From<Src>().DefinedAs(rows => rows
                    .GroupBy(r => r.Tenant)
                    .Select(g => new Tgt { Tenant = g.Key, Hits = g.Sum(r => r.Hits) }));
        }
    }
}
