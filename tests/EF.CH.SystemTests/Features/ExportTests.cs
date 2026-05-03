using System.Text.Json;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Features;

/// <summary>
/// Integration tests for the HTTP-interface export APIs in
/// <see cref="ClickHouseExportExtensions"/> (CSV / JSON / JSONEachRow / streaming format).
///
/// The export code talks HTTP, but the EF connection string carries the native protocol port
/// (typically 9000). Exports therefore need the HTTP port supplied either via
/// <c>UseHttpPort()</c> on the options builder, or via an <c>HttpPort=</c> field in the
/// connection string. Both pathways are exercised here.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class ExportTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ExportTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private Ctx CreateContext()
        => TestContextFactory.Create<Ctx>(Conn, o => o.UseHttpPort(_fx.HttpPort));

    [Fact]
    public async Task ToCsvAsync_ReturnsHeaderAndRows()
    {
        await using var ctx = CreateContext();
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Items.AddRange(MakeItems());
        await ctx.SaveChangesAsync();

        var csv = await ctx.Items.OrderBy(i => i.Id).ToCsvAsync(ctx);

        var lines = csv.Trim('\n').Split('\n');
        Assert.True(lines.Length >= 4); // header + 3 rows
        // CSVWithNames header line contains the column names. Quote characters appear around names
        // when they contain special chars; for simple identifiers ClickHouse may emit unquoted.
        Assert.Contains("Id", lines[0]);
        Assert.Contains("Label", lines[0]);
        Assert.Contains("alpha", csv);
        Assert.Contains("bravo", csv);
        Assert.Contains("charlie", csv);
    }

    [Fact]
    public async Task ToJsonAsync_ReturnsValidJsonWithDataArray()
    {
        await using var ctx = CreateContext();
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Items.AddRange(MakeItems());
        await ctx.SaveChangesAsync();

        var json = await ctx.Items.OrderBy(i => i.Id).ToJsonAsync(ctx);

        using var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.TryGetProperty("data", out var data));
        Assert.Equal(JsonValueKind.Array, data.ValueKind);
        Assert.Equal(3, data.GetArrayLength());

        var first = data[0];
        Assert.Equal(1, first.GetProperty("Id").GetInt32());
        Assert.Equal("alpha", first.GetProperty("Label").GetString());
    }

    [Fact]
    public async Task ToJsonLinesAsync_ReturnsOneObjectPerLine()
    {
        await using var ctx = CreateContext();
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Items.AddRange(MakeItems());
        await ctx.SaveChangesAsync();

        var jsonl = await ctx.Items.OrderBy(i => i.Id).ToJsonLinesAsync(ctx);

        var lines = jsonl.Trim('\n').Split('\n');
        Assert.Equal(3, lines.Length);
        for (int i = 0; i < lines.Length; i++)
        {
            using var doc = JsonDocument.Parse(lines[i]);
            Assert.True(doc.RootElement.TryGetProperty("Id", out _));
            Assert.True(doc.RootElement.TryGetProperty("Label", out _));
        }
    }

    [Fact]
    public async Task ToFormatStreamAsync_StreamsCsvToProvidedStream()
    {
        await using var ctx = CreateContext();
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Items.AddRange(MakeItems());
        await ctx.SaveChangesAsync();

        await using var ms = new MemoryStream();
        await ctx.Items.OrderBy(i => i.Id).ToFormatStreamAsync(ctx, "CSVWithNames", ms);

        Assert.True(ms.Length > 0);
        var content = System.Text.Encoding.UTF8.GetString(ms.ToArray());
        Assert.Contains("Id", content);
        Assert.Contains("alpha", content);
    }

    [Fact]
    public async Task ToFormatAsync_RespectsLinqFilter()
    {
        await using var ctx = CreateContext();
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Items.AddRange(MakeItems());
        await ctx.SaveChangesAsync();

        var jsonl = await ctx.Items
            .Where(i => i.Score >= 20)
            .OrderBy(i => i.Id)
            .ToJsonLinesAsync(ctx);

        var lines = jsonl.Trim('\n').Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, lines.Length); // bravo (20) + charlie (30)
    }

    [Fact]
    public async Task ExportWorks_WhenHttpPortIsSpecifiedInConnectionString()
    {
        var connWithHttpPort = $"{_fx.ConnectionString};HttpPort={_fx.HttpPort}";
        await using var ctx = TestContextFactory.Create<Ctx>(connWithHttpPort);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Items.AddRange(MakeItems());
        await ctx.SaveChangesAsync();

        var jsonl = await ctx.Items.OrderBy(i => i.Id).ToJsonLinesAsync(ctx);

        var lines = jsonl.Trim('\n').Split('\n');
        Assert.Equal(3, lines.Length);
    }

    private static IEnumerable<Item> MakeItems() => new[]
    {
        new Item { Id = 1, Label = "alpha", Score = 10 },
        new Item { Id = 2, Label = "bravo", Score = 20 },
        new Item { Id = 3, Label = "charlie", Score = 30 },
    };

    public sealed class Item
    {
        public uint Id { get; set; }
        public string Label { get; set; } = string.Empty;
        public uint Score { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Item> Items => Set<Item>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Item>(e =>
            {
                e.ToTable("export_items");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
        }
    }
}
