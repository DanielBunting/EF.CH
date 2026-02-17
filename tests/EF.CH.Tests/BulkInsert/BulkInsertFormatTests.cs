using EF.CH.BulkInsert;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.BulkInsert;

public class BulkInsertFormatTests : IAsyncLifetime
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
    public async Task BulkInsert_WithJsonEachRowFormat_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var events = Enumerable.Range(0, 100)
            .Select(i => new FormatEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                Name = $"Event_{i}",
                Value = i * 1.5
            })
            .ToList();

        var result = await context.BulkInsertAsync(events, opts => opts
            .WithFormat(ClickHouseBulkInsertFormat.JsonEachRow));

        Assert.Equal(100, result.RowsInserted);

        var count = await context.FormatEvents.LongCountAsync();
        Assert.Equal(100, count);
    }

    [Fact]
    public async Task BulkInsert_WithValuesFormat_Works()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var events = Enumerable.Range(0, 100)
            .Select(i => new FormatEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                Name = $"Event_{i}",
                Value = i * 1.5
            })
            .ToList();

        var result = await context.BulkInsertAsync(events, opts => opts
            .WithFormat(ClickHouseBulkInsertFormat.Values));

        Assert.Equal(100, result.RowsInserted);

        var count = await context.FormatEvents.LongCountAsync();
        Assert.Equal(100, count);
    }

    [Fact]
    public async Task BulkInsert_JsonEachRow_HandlesSpecialCharacters()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var events = new[]
        {
            new FormatEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                Name = "Test with \"quotes\" and 'apostrophes'",
                Value = 42.5
            },
            new FormatEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                Name = "Test with\nnewlines\tand\ttabs",
                Value = 99.9
            }
        };

        var result = await context.BulkInsertAsync(events, opts => opts
            .WithFormat(ClickHouseBulkInsertFormat.JsonEachRow));

        Assert.Equal(2, result.RowsInserted);

        var loaded = await context.FormatEvents.OrderBy(e => e.Value).ToListAsync();
        Assert.Equal("Test with \"quotes\" and 'apostrophes'", loaded[0].Name);
        Assert.Equal("Test with\nnewlines\tand\ttabs", loaded[1].Name);
    }

    [Fact]
    public async Task BulkInsert_Values_HandlesSpecialCharacters()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        var events = new[]
        {
            new FormatEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                Name = "Test with 'apostrophes'",
                Value = 42.5
            },
            new FormatEvent
            {
                Id = Guid.NewGuid(),
                EventTime = DateTime.UtcNow,
                Name = "Test with\\backslash",
                Value = 99.9
            }
        };

        var result = await context.BulkInsertAsync(events, opts => opts
            .WithFormat(ClickHouseBulkInsertFormat.Values));

        Assert.Equal(2, result.RowsInserted);

        var loaded = await context.FormatEvents.OrderBy(e => e.Value).ToListAsync();
        Assert.Equal("Test with 'apostrophes'", loaded[0].Name);
        Assert.Equal("Test with\\backslash", loaded[1].Name);
    }

    private FormatTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<FormatTestDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new FormatTestDbContext(options);
    }
}

public class FormatTestDbContext : DbContext
{
    public FormatTestDbContext(DbContextOptions<FormatTestDbContext> options) : base(options) { }

    public DbSet<FormatEvent> FormatEvents => Set<FormatEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FormatEvent>(entity =>
        {
            entity.ToTable("FormatEvents");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.EventTime, x.Id });
        });
    }
}

public class FormatEvent
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
}
