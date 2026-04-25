using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for <c>UUID</c> column ↔ .NET <see cref="Guid"/>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class UuidRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public UuidRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Guid_RoundTripsThroughUuidColumn()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var g = Guid.NewGuid();
        ctx.Rows.Add(new Row { Id = 1, Token = g });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal(g, read.Token);

        var t = await RawClickHouse.ColumnTypeAsync(Conn, "UuidRoundTrip_Rows", "Token");
        Assert.Equal("UUID", t);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public Guid Token { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("UuidRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
