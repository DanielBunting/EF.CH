using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class ArrayJoinTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();

    private string GetConnectionString() => _container.GetConnectionString();

    [Fact]
    public async Task ArrayJoin_Basic_UnnestsArrayIntoRows()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var result = await context.ArrayJoinEvents
            .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, e.Name, Tag = tag })
            .OrderBy(x => x.Id).ThenBy(x => x.Tag)
            .ToListAsync();

        // Event 1 has 3 tags, Event 2 has 2 tags, Event 3 has 0 tags (skipped by ARRAY JOIN)
        Assert.Equal(5, result.Count);
        Assert.Contains(result, r => r.Id == 1u && r.Tag == "critical");
        Assert.Contains(result, r => r.Id == 1u && r.Tag == "urgent");
        Assert.Contains(result, r => r.Id == 1u && r.Tag == "production");
        Assert.Contains(result, r => r.Id == 2u && r.Tag == "info");
        Assert.Contains(result, r => r.Id == 2u && r.Tag == "debug");
        // Event 3 with empty tags should not appear
        Assert.DoesNotContain(result, r => r.Id == 3u);
    }

    [Fact]
    public async Task LeftArrayJoin_PreservesEmptyArrayRows()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var result = await context.ArrayJoinEvents
            .LeftArrayJoin(e => e.Tags, (e, tag) => new { e.Id, e.Name, Tag = tag })
            .OrderBy(x => x.Id).ThenBy(x => x.Tag)
            .ToListAsync();

        // Event 1: 3 rows, Event 2: 2 rows, Event 3: 1 row (default tag)
        Assert.Equal(6, result.Count);
        Assert.Contains(result, r => r.Id == 3u); // Event 3 preserved with default tag
    }

    [Fact]
    public async Task ArrayJoin_WithWhere_FiltersOnUnnested()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var result = await context.ArrayJoinEvents
            .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, e.Name, Tag = tag })
            .Where(x => x.Tag == "critical")
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(1u, result[0].Id);
        Assert.Equal("critical", result[0].Tag);
    }

    [Fact]
    public async Task ArrayJoin_MultipleArrays_JoinsPositionally()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var result = await context.ArrayJoinEvents
            .ArrayJoin(
                e => e.Tags,
                e => e.Scores,
                (e, tag, score) => new { e.Id, Tag = tag, Score = score })
            .OrderBy(x => x.Id).ThenBy(x => x.Tag)
            .ToListAsync();

        // Event 1 has 3 tags and 3 scores, Event 2 has 2 tags and 2 scores
        Assert.Equal(5, result.Count);
        Assert.Contains(result, r => r.Id == 1u && r.Tag == "critical" && r.Score == 10);
    }

    [Fact]
    public async Task ArrayJoin_WithFinal_CombinesModifiers()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var result = await context.ArrayJoinEvents
            .Final()
            .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, Tag = tag })
            .ToListAsync();

        // Should work without error; FINAL and ARRAY JOIN are independent clauses
        Assert.True(result.Count > 0);
    }

    [Fact]
    public async Task ArrayJoin_WithGroupBy_AggregatesAfterUnnest()
    {
        await using var context = CreateContext();
        await SetupTable(context);
        await SeedData(context);

        var result = await context.ArrayJoinEvents
            .ArrayJoin(e => e.Tags, (e, tag) => new { e.Id, Tag = tag })
            .GroupBy(x => x.Tag)
            .Select(g => new { Tag = g.Key, Count = g.Count() })
            .OrderBy(x => x.Tag)
            .ToListAsync();

        Assert.Contains(result, r => r.Tag == "critical" && r.Count == 1);
        Assert.Contains(result, r => r.Tag == "info" && r.Count == 1);
    }

    private async Task SetupTable(ArrayJoinTestContext context)
    {
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ""ArrayJoinEvents"" (
                ""Id"" UInt32,
                ""Name"" String,
                ""Tags"" Array(String),
                ""Scores"" Array(Int32)
            ) ENGINE = ReplacingMergeTree()
            ORDER BY ""Id""
        ");
    }

    private async Task SeedData(ArrayJoinTestContext context)
    {
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO ""ArrayJoinEvents"" (""Id"", ""Name"", ""Tags"", ""Scores"") VALUES
            (1, 'Alert', ['critical', 'urgent', 'production'], [10, 20, 30]),
            (2, 'Log', ['info', 'debug'], [5, 15]),
            (3, 'Empty', [], [])
        ");
    }

    private ArrayJoinTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<ArrayJoinTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;
        return new ArrayJoinTestContext(options);
    }
}

public class ArrayJoinEvent
{
    public uint Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string[] Tags { get; set; } = [];
    public int[] Scores { get; set; } = [];
}

public class ArrayJoinTestContext : DbContext
{
    public ArrayJoinTestContext(DbContextOptions<ArrayJoinTestContext> options)
        : base(options) { }

    public DbSet<ArrayJoinEvent> ArrayJoinEvents => Set<ArrayJoinEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ArrayJoinEvent>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("ArrayJoinEvents");
            entity.UseMergeTree(x => x.Id);
        });
    }
}
