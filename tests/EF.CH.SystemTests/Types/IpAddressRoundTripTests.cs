using System.Net;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for <c>IPv4</c> / <c>IPv6</c> columns. The provider should map .NET
/// <see cref="IPAddress"/> to the corresponding ClickHouse IP type.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class IpAddressRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public IpAddressRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task IPv4_AndIPv6_RoundTrip()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var row = new Row
        {
            Id = 1,
            V4 = IPAddress.Parse("192.168.1.10"),
            V6 = IPAddress.Parse("2001:db8::1"),
        };
        ctx.Rows.Add(row);
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal(row.V4, read.V4);
        Assert.Equal(row.V6, read.V6);

        var v4Type = await RawClickHouse.ColumnTypeAsync(Conn, "IpAddressRoundTrip_Rows", "V4");
        var v6Type = await RawClickHouse.ColumnTypeAsync(Conn, "IpAddressRoundTrip_Rows", "V6");
        Assert.Equal("IPv4", v4Type);
        Assert.Equal("IPv6", v6Type);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public IPAddress V4 { get; set; } = IPAddress.None;
        public IPAddress V6 { get; set; } = IPAddress.IPv6None;
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("IpAddressRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.V4).HasColumnType("IPv4");
                e.Property(x => x.V6).HasColumnType("IPv6");
            });
    }
}
