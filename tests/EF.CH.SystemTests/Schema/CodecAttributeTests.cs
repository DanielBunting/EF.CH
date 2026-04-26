using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Codec configuration via <c>HasCodec(...)</c> appears in <c>system.columns.compression_codec</c>
/// or in the table's <c>create_table_query</c>. We assert the codec chain is present.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class CodecAttributeTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public CodecAttributeTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task HasCodec_PerColumn_RecordedInSystemColumns()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT name, compression_codec FROM system.columns " +
            "WHERE database = currentDatabase() AND table = 'CodecAttribute_Rows'");
        var codecByName = rows.ToDictionary(r => (string)r["name"]!, r => (string)r["compression_codec"]!);

        // T: Delta + ZSTD(3); Blob: ZSTD(9). Substring asserts per-column so swapping
        // codecs across columns would fail.
        Assert.True(codecByName.TryGetValue("T", out var tCodec));
        Assert.Contains("Delta", tCodec, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ZSTD(3)", tCodec, StringComparison.Ordinal);

        Assert.True(codecByName.TryGetValue("Blob", out var blobCodec));
        Assert.Contains("ZSTD(9)", blobCodec, StringComparison.Ordinal);
        Assert.DoesNotContain("Delta", blobCodec, StringComparison.OrdinalIgnoreCase);

        // Id (no codec) should be empty / no compression configured.
        Assert.True(codecByName.TryGetValue("Id", out var idCodec));
        Assert.True(string.IsNullOrEmpty(idCodec) || !idCodec.Contains("ZSTD", StringComparison.OrdinalIgnoreCase),
            $"Id should have no codec, got '{idCodec}'");
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public DateTime T { get; set; }
        public byte[] Blob { get; set; } = Array.Empty<byte>();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("CodecAttribute_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.T).HasCodec(c => c.Delta().ZSTD(3));
                e.Property(x => x.Blob).HasCodec(c => c.ZSTD(9));
            });
    }
}
