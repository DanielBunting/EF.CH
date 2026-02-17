using EF.CH.Extensions;
using EF.CH.TempTable;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class TempTableTests : IAsyncLifetime
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

    [Fact]
    public async Task CreateTempTable_InsertEntities_QueryBack()
    {
        await using var context = CreateContext();
        await SetupPermanentTable(context);

        await using var temp = await context.CreateTempTableAsync<TempEvent>("temp_events");

        var entities = new List<TempEvent>
        {
            new() { Id = Guid.NewGuid(), Name = "Event1", Category = "A", Amount = 100m, EventDate = DateTime.UtcNow },
            new() { Id = Guid.NewGuid(), Name = "Event2", Category = "B", Amount = 200m, EventDate = DateTime.UtcNow },
            new() { Id = Guid.NewGuid(), Name = "Event3", Category = "A", Amount = 300m, EventDate = DateTime.UtcNow }
        };

        await temp.InsertAsync(entities);

        var results = await temp.Query().ToListAsync();
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task CreateTempTableFromQuery_PopulatesFromPermanentTable()
    {
        await using var context = CreateContext();
        await SetupPermanentTable(context);
        await SeedData(context);

        await using var temp = await context.TempEvents
            .Where(e => e.Category == "electronics")
            .ToTempTableAsync(context, "temp_filtered");

        var results = await temp.Query().ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.All(results, e => Assert.Equal("electronics", e.Category));
    }

    [Fact]
    public async Task Query_SupportsLinqComposition()
    {
        await using var context = CreateContext();
        await SetupPermanentTable(context);

        await using var temp = await context.CreateTempTableAsync<TempEvent>("temp_linq");

        var entities = new List<TempEvent>
        {
            new() { Id = Guid.NewGuid(), Name = "Alpha", Category = "A", Amount = 500m, EventDate = DateTime.UtcNow },
            new() { Id = Guid.NewGuid(), Name = "Beta", Category = "B", Amount = 100m, EventDate = DateTime.UtcNow },
            new() { Id = Guid.NewGuid(), Name = "Gamma", Category = "A", Amount = 1500m, EventDate = DateTime.UtcNow }
        };
        await temp.InsertAsync(entities);

        // Where
        var filtered = await temp.Query().Where(e => e.Amount > 200m).ToListAsync();
        Assert.Equal(2, filtered.Count);

        // OrderBy
        var ordered = await temp.Query().OrderBy(e => e.Name).ToListAsync();
        Assert.Equal("Alpha", ordered[0].Name);
        Assert.Equal("Beta", ordered[1].Name);
        Assert.Equal("Gamma", ordered[2].Name);

        // Select projection
        var projected = await temp.Query().Select(e => e.Name).ToListAsync();
        Assert.Equal(3, projected.Count);
    }

    [Fact]
    public async Task InsertFromTwoSources_BothBatchesPresent()
    {
        await using var context = CreateContext();
        await SetupPermanentTable(context);

        await using var temp = await context.CreateTempTableAsync<TempEvent>("temp_multi");

        var batch1 = new List<TempEvent>
        {
            new() { Id = Guid.NewGuid(), Name = "Batch1A", Category = "X", Amount = 10m, EventDate = DateTime.UtcNow }
        };
        var batch2 = new List<TempEvent>
        {
            new() { Id = Guid.NewGuid(), Name = "Batch2A", Category = "Y", Amount = 20m, EventDate = DateTime.UtcNow },
            new() { Id = Guid.NewGuid(), Name = "Batch2B", Category = "Y", Amount = 30m, EventDate = DateTime.UtcNow }
        };

        await temp.InsertAsync(batch1);
        await temp.InsertAsync(batch2);

        var results = await temp.Query().ToListAsync();
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Dispose_DropsTable()
    {
        await using var context = CreateContext();
        await SetupPermanentTable(context);

        string tableName;
        {
            await using var temp = await context.CreateTempTableAsync<TempEvent>("temp_drop_test");
            tableName = temp.TableName;

            await temp.InsertAsync(new[]
            {
                new TempEvent { Id = Guid.NewGuid(), Name = "Test", Category = "A", Amount = 1m, EventDate = DateTime.UtcNow }
            });

            // Verify it exists
            var count = await temp.Query().CountAsync();
            Assert.Equal(1, count);
        }

        // After dispose, querying the table should fail
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await context.Database.ExecuteSqlRawAsync($"SELECT count() FROM \"{tableName}\"");
        });
    }

    [Fact]
    public async Task Scope_CreatesAndDisposesMultipleTables()
    {
        await using var context = CreateContext();
        await SetupPermanentTable(context);

        string name1, name2;

        {
            await using var scope = context.BeginTempTableScope();

            var t1 = await scope.CreateAsync<TempEvent>("temp_scope1");
            var t2 = await scope.CreateAsync<TempEvent>("temp_scope2");
            name1 = t1.TableName;
            name2 = t2.TableName;

            await t1.InsertAsync(new[]
            {
                new TempEvent { Id = Guid.NewGuid(), Name = "S1", Category = "A", Amount = 1m, EventDate = DateTime.UtcNow }
            });
            await t2.InsertAsync(new[]
            {
                new TempEvent { Id = Guid.NewGuid(), Name = "S2", Category = "B", Amount = 2m, EventDate = DateTime.UtcNow }
            });

            Assert.Equal(1, await t1.Query().CountAsync());
            Assert.Equal(1, await t2.Query().CountAsync());
        }

        // Both should be dropped after scope disposal
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await context.Database.ExecuteSqlRawAsync($"SELECT count() FROM \"{name1}\"");
        });
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await context.Database.ExecuteSqlRawAsync($"SELECT count() FROM \"{name2}\"");
        });
    }

    [Fact]
    public async Task AutoGeneratedNames_AreUnique()
    {
        await using var context = CreateContext();
        await SetupPermanentTable(context);

        await using var t1 = await context.CreateTempTableAsync<TempEvent>();
        await using var t2 = await context.CreateTempTableAsync<TempEvent>();

        Assert.NotEqual(t1.TableName, t2.TableName);
        Assert.StartsWith("_tmp_TempEvent_", t1.TableName);
        Assert.StartsWith("_tmp_TempEvent_", t2.TableName);
    }

    [Fact]
    public async Task SessionAffinity_SurvivesInterveningEfQueries()
    {
        await using var context = CreateContext();
        await SetupPermanentTable(context);
        await SeedData(context);

        await using var temp = await context.CreateTempTableAsync<TempEvent>("temp_affinity");

        await temp.InsertAsync(new[]
        {
            new TempEvent { Id = Guid.NewGuid(), Name = "TempRow", Category = "temp", Amount = 999m, EventDate = DateTime.UtcNow }
        });

        // Do a regular EF query on the permanent table (this opens/closes connection via ref counting)
        var permanentCount = await context.TempEvents.CountAsync();
        Assert.True(permanentCount > 0);

        // The temp table should still be accessible
        var tempResults = await temp.Query().ToListAsync();
        Assert.Single(tempResults);
        Assert.Equal("TempRow", tempResults[0].Name);
    }

    [Fact]
    public async Task InsertIntoTempTableAsync_ConvenienceExtension()
    {
        await using var context = CreateContext();
        await SetupPermanentTable(context);
        await SeedData(context);

        await using var temp = await context.CreateTempTableAsync<TempEvent>("temp_convenience");

        await context.TempEvents
            .Where(e => e.Category == "books")
            .InsertIntoTempTableAsync(temp);

        var results = await temp.Query().ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, e => Assert.Equal("books", e.Category));
    }

    private async Task SetupPermanentTable(TempTableTestContext context)
    {
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""TempEvents"" (
                ""Id"" UUID,
                ""Name"" String,
                ""Category"" String,
                ""Amount"" Decimal64(2),
                ""EventDate"" DateTime64(3)
            ) ENGINE = MergeTree()
            ORDER BY ""Id""
        ");
    }

    private async Task SeedData(TempTableTestContext context)
    {
        var now = DateTime.UtcNow;
        context.TempEvents.AddRange(
            new TempEvent { Id = Guid.NewGuid(), Name = "Laptop Sale", Category = "electronics", Amount = 999.99m, EventDate = now.AddDays(-1) },
            new TempEvent { Id = Guid.NewGuid(), Name = "Phone Sale", Category = "electronics", Amount = 599.99m, EventDate = now.AddDays(-2) },
            new TempEvent { Id = Guid.NewGuid(), Name = "Cable Sale", Category = "electronics", Amount = 9.99m, EventDate = now.AddDays(-3) },
            new TempEvent { Id = Guid.NewGuid(), Name = "Book Sale", Category = "books", Amount = 29.99m, EventDate = now.AddDays(-5) },
            new TempEvent { Id = Guid.NewGuid(), Name = "Magazine Sale", Category = "books", Amount = 5.99m, EventDate = now.AddDays(-10) }
        );
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();
    }

    private TempTableTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TempTableTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new TempTableTestContext(options);
    }
}

public class TempEvent
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime EventDate { get; set; }
}

public class TempTableTestContext : DbContext
{
    public TempTableTestContext(DbContextOptions<TempTableTestContext> options)
        : base(options) { }

    public DbSet<TempEvent> TempEvents => Set<TempEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TempEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("TempEvents");
            entity.UseMergeTree(x => x.Id);
        });
    }
}
