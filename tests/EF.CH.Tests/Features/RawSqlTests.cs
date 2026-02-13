using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class RawSqlTests : IAsyncLifetime
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

    #region WithRawFilter Tests

    [Fact]
    public async Task WithRawFilter_GeneratesAndCondition()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var sql = context.RawSqlEvents
            .Where(e => e.Category == "electronics")
            .WithRawFilter("\"Amount\" > 100")
            .ToQueryString();

        Assert.Contains("\"Category\"", sql);
        Assert.Contains("\"Amount\" > 100", sql);
    }

    [Fact]
    public async Task WithRawFilter_ExecutesCorrectly()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var results = await context.RawSqlEvents
            .Where(e => e.Category == "electronics")
            .WithRawFilter("\"Amount\" > 100")
            .ToListAsync();

        Assert.All(results, e =>
        {
            Assert.Equal("electronics", e.Category);
            Assert.True(e.Amount > 100);
        });
    }

    [Fact]
    public async Task WithRawFilter_WithoutLinqWhere()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var results = await context.RawSqlEvents
            .WithRawFilter("\"Amount\" < 20")
            .ToListAsync();

        Assert.All(results, e => Assert.True(e.Amount < 20));
    }

    [Fact]
    public void WithRawFilter_NullSql_Throws()
    {
        using var context = CreateContext();

        Assert.ThrowsAny<ArgumentException>(() =>
            context.RawSqlEvents.WithRawFilter(null!));
    }

    [Fact]
    public void WithRawFilter_EmptySql_Throws()
    {
        using var context = CreateContext();

        Assert.ThrowsAny<ArgumentException>(() =>
            context.RawSqlEvents.WithRawFilter(""));
    }

    #endregion

    #region RawSql<T> Tests

    [Fact]
    public async Task RawSql_InWhereProjection_GeneratesCorrectSql()
    {
        await using var context = CreateContext();
        await SetupTable(context);

        // RawSql in a Where clause via WithRawFilter (covered above) and
        // in a simple Select projection
        var sql = context.RawSqlEvents
            .Where(e => e.Category == "electronics")
            .Select(e => new
            {
                e.Name,
                Constant = ClickHouseFunctions.RawSql<long>("1")
            })
            .ToQueryString();

        // The raw SQL "1" should appear in the generated query
        Assert.Contains("1", sql);
    }

    [Fact]
    public async Task RawSql_SimpleExpression_ReturnsResults()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        // Use RawSql to embed a simple raw expression in projection
        var results = await context.RawSqlEvents
            .Select(e => new
            {
                e.Name,
                NameLength = ClickHouseFunctions.RawSql<ulong>("length(\"Name\")")
            })
            .ToListAsync();

        Assert.True(results.Count > 0);
        Assert.All(results, r => Assert.True(r.NameLength > 0));
    }

    #endregion

    #region Table Function Tests

    [Fact]
    public async Task FromUrl_GeneratesCorrectSql()
    {
        await using var context = CreateContext();

        var query = context.FromUrl<RawSqlEvent>(
            "http://example.com/data.csv",
            "CSVWithNames");

        var sql = query.ToQueryString();

        Assert.Contains("url(", sql);
        Assert.Contains("http://example.com/data.csv", sql);
        Assert.Contains("CSVWithNames", sql);
    }

    [Fact]
    public async Task FromS3_GeneratesCorrectSql()
    {
        await using var context = CreateContext();

        var query = context.FromS3<RawSqlEvent>(
            "https://my-bucket.s3.amazonaws.com/data.parquet",
            "Parquet",
            "AKID",
            "SECRET");

        var sql = query.ToQueryString();

        Assert.Contains("s3(", sql);
        Assert.Contains("https://my-bucket.s3.amazonaws.com/data.parquet", sql);
        Assert.Contains("Parquet", sql);
    }

    [Fact]
    public async Task FromFile_GeneratesCorrectSql()
    {
        await using var context = CreateContext();

        var query = context.FromFile<RawSqlEvent>(
            "/data/export.csv",
            "CSVWithNames");

        var sql = query.ToQueryString();

        Assert.Contains("file(", sql);
        Assert.Contains("/data/export.csv", sql);
    }

    [Fact]
    public async Task FromRemote_GeneratesCorrectSql()
    {
        await using var context = CreateContext();

        var query = context.FromRemote<RawSqlEvent>(
            "remote-host:9000",
            "default",
            "events");

        var sql = query.ToQueryString();

        Assert.Contains("remote(", sql);
        Assert.Contains("remote-host:9000", sql);
    }

    [Fact]
    public async Task FromCluster_GeneratesCorrectSql()
    {
        await using var context = CreateContext();

        var query = context.FromCluster<RawSqlEvent>(
            "my_cluster",
            "default",
            "events");

        var sql = query.ToQueryString();

        Assert.Contains("cluster(", sql);
        Assert.Contains("my_cluster", sql);
    }

    [Fact]
    public void InferStructure_IncludesModelColumns()
    {
        using var context = CreateContext();

        var query = context.FromUrl<RawSqlEvent>(
            "http://example.com/data.csv",
            "CSVWithNames");

        var sql = query.ToQueryString();

        // Structure should contain column definitions from the model
        Assert.Contains("Id", sql);
        Assert.Contains("Name", sql);
    }

    #endregion

    #region Export Format Tests

    [Fact]
    public async Task ToCsvAsync_ReturnsHeaderAndRows()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var csv = await context.RawSqlEvents
            .Where(e => e.Category == "electronics")
            .ToCsvAsync(context);

        Assert.NotNull(csv);
        Assert.NotEmpty(csv);
        // CSV with names should have a header row
        var lines = csv.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.True(lines.Length > 1, "Expected header + data rows");
    }

    [Fact]
    public async Task ToJsonAsync_ReturnsValidJson()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var json = await context.RawSqlEvents
            .Where(e => e.Category == "books")
            .ToJsonAsync(context);

        Assert.NotNull(json);
        Assert.Contains("\"data\"", json);
    }

    [Fact]
    public async Task ToJsonLinesAsync_ReturnsJsonLines()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var jsonLines = await context.RawSqlEvents
            .Where(e => e.Category == "electronics")
            .ToJsonLinesAsync(context);

        Assert.NotNull(jsonLines);
        Assert.NotEmpty(jsonLines);
    }

    [Fact]
    public async Task ToFormatStreamAsync_WritesToStream()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        using var ms = new MemoryStream();

        await context.RawSqlEvents
            .Where(e => e.Category == "electronics")
            .ToFormatStreamAsync(context, "CSVWithNames", ms);

        Assert.True(ms.Length > 0);
    }

    #endregion

    #region Setup

    private async Task SetupTable(RawSqlTestContext context)
    {
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""RawSqlEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Category"" String,
                ""Amount"" Decimal64(2),
                ""EventDate"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");
    }

    private async Task SeedData(RawSqlTestContext context)
    {
        var now = DateTime.UtcNow;
        context.RawSqlEvents.AddRange(
            new RawSqlEvent { Id = Guid.NewGuid(), Name = "Laptop Sale", Category = "electronics", Amount = 999.99m, EventDate = now.AddDays(-1) },
            new RawSqlEvent { Id = Guid.NewGuid(), Name = "Phone Sale", Category = "electronics", Amount = 599.99m, EventDate = now.AddDays(-2) },
            new RawSqlEvent { Id = Guid.NewGuid(), Name = "Cable Sale", Category = "electronics", Amount = 9.99m, EventDate = now.AddDays(-3) },
            new RawSqlEvent { Id = Guid.NewGuid(), Name = "Book Sale", Category = "books", Amount = 29.99m, EventDate = now.AddDays(-5) },
            new RawSqlEvent { Id = Guid.NewGuid(), Name = "Magazine Sale", Category = "books", Amount = 5.99m, EventDate = now.AddDays(-10) }
        );
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();
    }

    private RawSqlTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<RawSqlTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new RawSqlTestContext(options);
    }

    #endregion
}

public class RawSqlEvent
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime EventDate { get; set; }
}

public class RawSqlTestContext : DbContext
{
    public RawSqlTestContext(DbContextOptions<RawSqlTestContext> options)
        : base(options) { }

    public DbSet<RawSqlEvent> RawSqlEvents => Set<RawSqlEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<RawSqlEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("RawSqlEvents");
            entity.UseMergeTree(x => x.Id);
        });
    }
}
