using System.Net;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>MV pass-through for the <c>IPv6</c> column type.</summary>
[Collection(SingleNodeCollection.Name)]
public class MvIpv6RoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvIpv6RoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Ipv6_PassesThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Address = IPAddress.Parse("2001:db8::1"), N = 1 },
            new Src { Id = 2, Address = IPAddress.Parse("fe80::abcd"),  N = 2 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvIpv6Target");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toString(Address) AS Address, toInt64(N) AS N FROM \"MvIpv6Target\" ORDER BY N");
        Assert.Equal(2, rows.Count);
        Assert.Equal("2001:db8::1", (string)rows[0]["Address"]!);
        Assert.Equal("fe80::abcd",  (string)rows[1]["Address"]!);

        Assert.Equal("IPv6", await RawClickHouse.ColumnTypeAsync(Conn, "MvIpv6Target", "Address"));
    }

    public sealed class Src { public uint Id { get; set; } public IPAddress Address { get; set; } = IPAddress.IPv6None; public long N { get; set; } }
    public sealed class Tgt { public IPAddress Address { get; set; } = IPAddress.IPv6None; public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvIpv6Source"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Address).HasColumnType("IPv6");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvIpv6Target"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.Property(x => x.Address).HasColumnType("IPv6");

            });
            mb.MaterializedView<Tgt>().From<Src>().DefinedAs(rows => rows
                    .Select(r => new Tgt { Address = r.Address, N = r.N }));
        }
    }
}
