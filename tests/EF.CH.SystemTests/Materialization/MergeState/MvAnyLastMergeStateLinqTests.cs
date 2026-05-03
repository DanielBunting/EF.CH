using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.MergeState;

/// <summary>
/// LINQ <c>g.AnyLastMergeState(...)</c> — Hourly populated via direct
/// INSERT-SELECT to fire Daily's MV through the LINQ <c>anyLastMergeState</c> arm.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvAnyLastMergeStateLinqTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvAnyLastMergeStateLinqTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task LinqAnyLastMergeState_ProducesDailyState()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Raw.AddRange(
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", Tag = "alpha" },
            new RawRow { Id = Guid.NewGuid(), Bucket = "a", Tag = "beta" });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"MsAnyLastHourly\" SELECT Bucket, anyLastState(Tag) FROM \"MsAnyLastRaw\" GROUP BY Bucket");
        await RawClickHouse.SettleMaterializationAsync(Conn, "MsAnyLastDaily");

        Assert.True(await RawClickHouse.RowCountAsync(Conn, "MsAnyLastDaily") > 0);
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RawRow> Raw => Set<RawRow>();
        public DbSet<HourlyRow> Hourly => Set<HourlyRow>();
        public DbSet<DailyRow> Daily => Set<DailyRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RawRow>(e => { e.ToTable("MsAnyLastRaw"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<HourlyRow>(e =>
            {
                e.ToTable("MsAnyLastHourly"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.LastTag).HasAggregateFunction("anyLast", typeof(string));
            });
            mb.Entity<DailyRow>(e =>
            {
                e.ToTable("MsAnyLastDaily"); e.HasNoKey();
                e.UseAggregatingMergeTree(x => x.Bucket);
                e.Property(x => x.LastTag).HasAggregateFunction("anyLast", typeof(string));

            });
            mb.MaterializedView<DailyRow>().From<HourlyRow>().DefinedAs(rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new DailyRow { Bucket = g.Key, LastTag = g.AnyLastMergeState(r => r.LastTag) }));
        }
    }

    public sealed class RawRow { public Guid Id { get; set; } public string Bucket { get; set; } = ""; public string Tag { get; set; } = ""; }
    public sealed class HourlyRow { public string Bucket { get; set; } = ""; public byte[] LastTag { get; set; } = Array.Empty<byte>(); }
    public sealed class DailyRow { public string Bucket { get; set; } = ""; public byte[] LastTag { get; set; } = Array.Empty<byte>(); }
}
