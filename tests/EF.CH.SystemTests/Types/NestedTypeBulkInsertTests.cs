using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Bulk-insert / SaveChanges round-trip for Nested columns with embedded
/// colon-strings. Adjacent to the Map(K,V) parameter-scanner bug — Nested
/// emits parallel arrays in <c>(...)</c> parens form, so it shouldn't trip
/// the same scanner, but worth a guard.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class NestedTypeBulkInsertTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public NestedTypeBulkInsertTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Nested_RoundTripsWithEmbeddedColons()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.Add(new Row
        {
            Id = 1,
            Players = new List<Player>
            {
                new() { Name = "alice:n", Score = 10 },
                new() { Name = "bob:s", Score = 20 },
            },
        });
        ctx.Rows.Add(new Row
        {
            Id = 2,
            Players = new List<Player>
            {
                new() { Name = "k:v", Score = 99 },
            },
        });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0].Players.Count);
        Assert.Contains(rows[0].Players, p => p.Name == "alice:n" && p.Score == 10);
        Assert.Contains(rows[0].Players, p => p.Name == "bob:s" && p.Score == 20);
        Assert.Single(rows[1].Players);
        Assert.Equal("k:v", rows[1].Players[0].Name);
    }

    public sealed class Player { public string Name { get; set; } = ""; public uint Score { get; set; } }
    public sealed class Row
    {
        public ulong Id { get; set; }
        public List<Player> Players { get; set; } = new();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("NestedColonRows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.HasNested<Row, Player>(x => x.Players);
            });
    }
}
