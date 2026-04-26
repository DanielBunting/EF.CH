using System.Net;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Materialization.ValueTypes;

/// <summary>MV pass-through for the <c>IPv4</c> column type.</summary>
[Collection(SingleNodeCollection.Name)]
public class MvIpv4RoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public MvIpv4RoundTripTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task Ipv4_PassesThrough()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Sources.AddRange(
            new Src { Id = 1, Address = IPAddress.Parse("10.0.0.1"),       N = 1 },
            new Src { Id = 2, Address = IPAddress.Parse("192.168.1.100"),  N = 2 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.SettleMaterializationAsync(Conn, "MvIpv4Target");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT toString(Address) AS Address, toInt64(N) AS N FROM \"MvIpv4Target\" ORDER BY N");
        Assert.Equal(2, rows.Count);
        Assert.Equal("10.0.0.1",      (string)rows[0]["Address"]!);
        Assert.Equal("192.168.1.100", (string)rows[1]["Address"]!);

        Assert.Equal("IPv4", await RawClickHouse.ColumnTypeAsync(Conn, "MvIpv4Target", "Address"));
    }

    public sealed class Src { public uint Id { get; set; } public IPAddress Address { get; set; } = IPAddress.None; public long N { get; set; } }
    public sealed class Tgt { public IPAddress Address { get; set; } = IPAddress.None; public long N { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Src> Sources => Set<Src>();
        public DbSet<Tgt> Targets => Set<Tgt>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Src>(e =>
            {
                e.ToTable("MvIpv4Source"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Address).HasColumnType("IPv4");
            });
            mb.Entity<Tgt>(e =>
            {
                e.ToTable("MvIpv4Target"); e.HasNoKey();
                e.UseMergeTree(x => x.N);
                e.Property(x => x.Address).HasColumnType("IPv4");
                e.AsMaterializedView<Tgt, Src>(rows => rows
                    .Select(r => new Tgt { Address = r.Address, N = r.N }));
            });
        }
    }
}
