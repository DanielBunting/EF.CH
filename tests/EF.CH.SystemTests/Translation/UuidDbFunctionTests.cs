using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <see cref="ClickHouseUuidDbFunctionsExtensions"/>: pin the
/// translated SQL output for canonical inputs against a live ClickHouse
/// server. UUIDv4 is asserted on structural properties (length + version
/// nibble) since it's non-deterministic; UUIDv7 is asserted on time-ordering.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class UuidDbFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public UuidDbFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        // Two seeded rows so we can compare two generated UUIDs / probe round-trip.
        ctx.Rows.Add(new Row { Id = 1, KnownUuid = "12345678-1234-1234-1234-123456789abc" });
        ctx.Rows.Add(new Row { Id = 2, KnownUuid = "12345678-1234-1234-1234-123456789abc" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task GenerateUUIDv4_ReturnsValidUuid()
    {
        await using var ctx = await SeededAsync();
        var g = await ctx.Rows.Select(x => EfClass.Functions.NewGuidV4()).FirstAsync();
        // V4 means version nibble is 0x4 in byte 6 (.NET layout is mixed-endian
        // for the first three groups, but the version nibble is preserved).
        var bytes = g.ToByteArray();
        Assert.Equal(16, bytes.Length);
        Assert.Equal(0x40, bytes[7] & 0xF0);
        Assert.NotEqual(Guid.Empty, g);
    }

    [Fact]
    public async Task GenerateUUIDv7_TimeOrdered()
    {
        await using var ctx = await SeededAsync();
        // Generate two UUIDv7 values back to back; the second should sort
        // strictly greater than the first because v7 leads with a 48-bit Unix
        // millisecond timestamp.
        var pair = await ctx.Rows.OrderBy(r => r.Id).Select(x => new
        {
            x.Id,
            U = EfClass.Functions.NewGuidV7(),
        }).ToListAsync();

        Assert.True(pair[1].U.CompareTo(pair[0].U) > 0,
            $"expected UUIDv7 to be time-ordered; got {pair[0].U} then {pair[1].U}");
    }

    [Fact]
    public async Task UUIDStringToNum_AndBack_RoundTrips()
    {
        await using var ctx = await SeededAsync();
        var roundTrip = await ctx.Rows.Select(x =>
            EfClass.Functions.UUIDNumToString(EfClass.Functions.UUIDStringToNum(x.KnownUuid))).FirstAsync();
        Assert.Equal("12345678-1234-1234-1234-123456789abc", roundTrip);
    }

    [Fact]
    public async Task ToUUIDOrNull_ReturnsNullForInvalid()
    {
        await using var ctx = await SeededAsync();
        var maybe = await ctx.Rows.Select(x =>
            EfClass.Functions.ToUUIDOrNull("not-a-uuid")).FirstAsync();
        Assert.Null(maybe);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string KnownUuid { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("UuidFn_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
