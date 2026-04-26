using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.Expressions;

/// <summary>
/// <c>ClickHouseFunctions.CityHash64</c> applied to a string column inside
/// an MV selector — translates to <c>cityHash64(...)</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class MvClickHouseHashFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvClickHouseHashFunctionTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task CityHash64_InSelector()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync(); await ctx.Database.EnsureCreatedAsync();

        ctx.Source.AddRange(
            new Row { Id = 1, Name = "alpha" },
            new Row { Id = 2, Name = "beta" },
            new Row { Id = 3, Name = "alpha" });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvChHashTarget");

        // Same source name should hash to same value.
        var hashes = await RawClickHouse.RowsAsync(Conn,
            "SELECT toInt64(Id) AS Id, toUInt64(NameHash) AS H FROM \"MvChHashTarget\" ORDER BY Id");
        Assert.Equal(3, hashes.Count);
        Assert.Equal(Convert.ToUInt64(hashes[0]["H"]), Convert.ToUInt64(hashes[2]["H"]));
        Assert.NotEqual(Convert.ToUInt64(hashes[0]["H"]), Convert.ToUInt64(hashes[1]["H"]));
    }

    public sealed class Row { public long Id { get; set; } public string Name { get; set; } = ""; }
    public sealed class Tgt { public long Id { get; set; } public ulong NameHash { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Source => Set<Row>();
        public DbSet<Tgt> Target => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Row>(e => { e.ToTable("MvChHashSource"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvChHashTarget"); e.HasNoKey(); e.UseMergeTree(x => x.Id);
                e.AsMaterializedView<Tgt, Row>(rows => rows
                    .Select(r => new Tgt { Id = r.Id, NameHash = ClickHouseFunctions.CityHash64(r.Name) }));
            });
        }
    }
}
