using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Codec-chain round-trip: each chain produces the expected ClickHouse
/// <c>compression_codec</c> string and round-trips data correctly. The existing
/// <c>CodecAttributeTests</c> covers single-codec attributes; this surface
/// covers the multi-codec fluent builder which composes
/// <c>Delta + LZ4</c>, <c>DoubleDelta + ZSTD</c>, and <c>Gorilla + ZSTD</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class CodecChainRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public CodecChainRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task DeltaLz4Chain_AppliesAndRoundTrips()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var codec = await RawClickHouse.ColumnCompressionCodecAsync(Conn, "Codec_Rows", "Counter");
        Assert.Contains("Delta", codec, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LZ4", codec, StringComparison.OrdinalIgnoreCase);

        ctx.Rows.AddRange(Enumerable.Range(1, 20).Select(i => new Row
        {
            Id = (uint)i,
            Counter = i * 1_000L,
            SensorTime = DateTime.UnixEpoch.AddSeconds(i * 30),
            Pressure = 1013.25 + (i * 0.5),
        }));
        await ctx.SaveChangesAsync();

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(20, rows.Count);
        Assert.Equal(1_000L, rows[0].Counter);
        Assert.Equal(20_000L, rows[19].Counter);
    }

    [Fact]
    public async Task DoubleDeltaZstdChain_AppliesAndRoundTrips()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var codec = await RawClickHouse.ColumnCompressionCodecAsync(Conn, "Codec_Rows", "SensorTime");
        Assert.Contains("DoubleDelta", codec, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ZSTD", codec, StringComparison.OrdinalIgnoreCase);

        var baseTime = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        ctx.Rows.AddRange(Enumerable.Range(1, 50).Select(i => new Row
        {
            Id = (uint)i,
            Counter = i,
            SensorTime = baseTime.AddSeconds(i),
            Pressure = i * 0.1,
        }));
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var roundTripped = await ctx.Rows.OrderBy(r => r.Id).Select(r => r.SensorTime).ToListAsync();
        Assert.Equal(baseTime.AddSeconds(1), roundTripped[0]);
        Assert.Equal(baseTime.AddSeconds(50), roundTripped[49]);
    }

    [Fact]
    public async Task GorillaZstdChain_AppliesAndRoundTrips_ForFloats()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var codec = await RawClickHouse.ColumnCompressionCodecAsync(Conn, "Codec_Rows", "Pressure");
        Assert.Contains("Gorilla", codec, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ZSTD", codec, StringComparison.OrdinalIgnoreCase);

        ctx.Rows.AddRange(Enumerable.Range(1, 30).Select(i => new Row
        {
            Id = (uint)i,
            Counter = i,
            SensorTime = DateTime.UnixEpoch.AddSeconds(i),
            Pressure = 1000.0 + (i * 0.001),
        }));
        await ctx.SaveChangesAsync();

        var roundTripped = await ctx.Rows.OrderBy(r => r.Id).Select(r => r.Pressure).ToListAsync();
        Assert.Equal(1000.001, roundTripped[0], 5);
        Assert.Equal(1000.030, roundTripped[29], 5);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public long Counter { get; set; }
        public DateTime SensorTime { get; set; }
        public double Pressure { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("Codec_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Counter).HasCodec(c => c.Delta().LZ4());
                e.Property(x => x.SensorTime).HasCodec(c => c.DoubleDelta().ZSTD(3));
                e.Property(x => x.Pressure).HasCodec(c => c.Gorilla().ZSTD());
            });
    }
}
