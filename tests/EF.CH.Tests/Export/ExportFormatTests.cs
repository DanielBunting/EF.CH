using EF.CH.Export;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Export;

public class ExportFormatTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    [Fact]
    public async Task ToCsvAsync_ReturnsValidCsv()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'Alpha', 10.5), (2, 'Beta', 20.75), (3, 'Gamma', 30.0)
            """);

        var csv = await context.ExportItems.OrderBy(x => x.Id).ToCsvAsync();

        Assert.NotNull(csv);
        Assert.Contains("Id", csv);
        Assert.Contains("Name", csv);
        Assert.Contains("Value", csv);
        Assert.Contains("Alpha", csv);
        Assert.Contains("Beta", csv);
        Assert.Contains("Gamma", csv);
    }

    [Fact]
    public async Task ToJsonAsync_ReturnsValidJson()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'Alpha', 10.5), (2, 'Beta', 20.75)
            """);

        var json = await context.ExportItems.OrderBy(x => x.Id).ToJsonAsync();

        Assert.NotNull(json);
        // JSONEachRow format produces one JSON object per line
        Assert.Contains("\"Id\"", json);
        Assert.Contains("\"Name\"", json);
        Assert.Contains("Alpha", json);
        Assert.Contains("Beta", json);
    }

    [Fact]
    public async Task ToFormatAsync_TabSeparatedWithNames_ReturnsValidTsv()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'Test', 99.9)
            """);

        var tsv = await context.ExportItems.ToFormatAsync(ClickHouseExportFormat.TabSeparatedWithNames);

        Assert.NotNull(tsv);
        Assert.Contains("Id", tsv);
        Assert.Contains("Name", tsv);
        Assert.Contains("Test", tsv);
        Assert.Contains("\t", tsv); // Tab separator
    }

    [Fact]
    public async Task ToMarkdownAsync_ReturnsValidMarkdown()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'First', 1.0), (2, 'Second', 2.0)
            """);

        var markdown = await context.ExportItems.OrderBy(x => x.Id).ToMarkdownAsync();

        Assert.NotNull(markdown);
        // Markdown format includes | separators and header row
        Assert.Contains("|", markdown);
        Assert.Contains("Id", markdown);
        Assert.Contains("First", markdown);
        Assert.Contains("Second", markdown);
    }

    [Fact]
    public async Task ToXmlAsync_ReturnsValidXml()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'XmlTest', 42.0)
            """);

        var xml = await context.ExportItems.ToXmlAsync();

        Assert.NotNull(xml);
        Assert.Contains("<?xml", xml);
        Assert.Contains("<result>", xml);
        Assert.Contains("XmlTest", xml);
    }

    [Fact]
    public async Task ToFormatAsync_JSON_ReturnsJsonWithMetadata()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'JsonMeta', 100.0)
            """);

        var json = await context.ExportItems.ToFormatAsync(ClickHouseExportFormat.JSON);

        Assert.NotNull(json);
        // JSON format includes metadata about rows
        Assert.Contains("\"meta\"", json);
        Assert.Contains("\"data\"", json);
        Assert.Contains("JsonMeta", json);
    }

    [Fact]
    public async Task ToFormatStreamAsync_CSV_WritesToStream()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'StreamTest', 55.5)
            """);

        using var stream = new MemoryStream();
        await context.ExportItems.ToCsvStreamAsync(stream);

        stream.Position = 0;
        using var reader = new StreamReader(stream);
        var content = await reader.ReadToEndAsync();

        Assert.NotEmpty(content);
        Assert.Contains("StreamTest", content);
    }

    [Fact]
    public async Task ToParquetAsync_WritesToStream()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'ParquetTest', 77.7)
            """);

        using var stream = new MemoryStream();
        await context.ExportItems.ToParquetAsync(stream);

        // Parquet files start with "PAR1" magic bytes
        Assert.True(stream.Length > 0, "Parquet output should not be empty");

        stream.Position = 0;
        var buffer = new byte[4];
        await stream.ReadAsync(buffer.AsMemory(0, 4));
        var magic = System.Text.Encoding.ASCII.GetString(buffer);
        Assert.Equal("PAR1", magic);
    }

    [Fact]
    public async Task ToCsv_WithFilters_ExportsFilteredData()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'Include', 10.0), (2, 'Exclude', 20.0), (3, 'Include', 30.0)
            """);

        var csv = await context.ExportItems
            .Where(x => x.Name == "Include")
            .OrderBy(x => x.Id)
            .ToCsvAsync();

        Assert.NotNull(csv);
        Assert.Contains("Include", csv);
        Assert.DoesNotContain("Exclude", csv);
    }

    [Fact]
    public async Task ToCsv_WithProjection_ExportsProjectedColumns()
    {
        await using var context = CreateContext<ExportTestContext>();

        await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS \"ExportItems\"");
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'ProjectionTest', 123.456)
            """);

        var csv = await context.ExportItems
            .Select(x => new { x.Name, x.Value })
            .ToCsvAsync();

        Assert.NotNull(csv);
        Assert.Contains("Name", csv);
        Assert.Contains("Value", csv);
        Assert.Contains("ProjectionTest", csv);
        // Should not contain Id since it's not in the projection
        // (Header might not have it, but we check the key columns)
    }

    [Fact]
    public void ToCsv_SynchronousVersion_Works()
    {
        using var context = CreateContext<ExportTestContext>();

        context.Database.ExecuteSqlRaw("DROP TABLE IF EXISTS \"ExportItems\"");
        context.Database.ExecuteSqlRaw("""
            CREATE TABLE "ExportItems" (
                "Id" Int32,
                "Name" String,
                "Value" Float64
            )
            ENGINE = MergeTree()
            ORDER BY ("Id")
            """);

        context.Database.ExecuteSqlRaw("""
            INSERT INTO "ExportItems" ("Id", "Name", "Value")
            VALUES (1, 'SyncTest', 42.0)
            """);

        var csv = context.ExportItems.ToCsv();

        Assert.NotNull(csv);
        Assert.Contains("SyncTest", csv);
    }
}

#region Test Entities

public class ExportTestContext : DbContext
{
    public ExportTestContext(DbContextOptions<ExportTestContext> options) : base(options) { }

    public DbSet<ExportItem> ExportItems => Set<ExportItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ExportItem>(entity =>
        {
            entity.ToTable("ExportItems");
            entity.HasNoKey();
            entity.UseMergeTree("Id");
        });
    }
}

public class ExportItem
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
}

#endregion
