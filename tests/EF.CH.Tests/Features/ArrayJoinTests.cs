using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Integration tests for ARRAY JOIN feature.
/// ARRAY JOIN "explodes" array columns into separate rows.
///
/// Note: Due to EF Core's query pipeline architecture, ARRAY JOIN cannot be fully
/// integrated as a LINQ extension method because the NavigationExpandingExpressionVisitor
/// runs before our custom visitor and doesn't recognize the custom method.
///
/// For production use, ARRAY JOIN should be executed via:
/// 1. Raw SQL queries using FromSqlRaw/SqlQuery
/// 2. Client-side evaluation (ToList then SelectMany)
/// </summary>
public class ArrayJoinTests : IAsyncLifetime
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

    [Fact(Skip = "ARRAY JOIN LINQ extension requires EF Core pipeline modification - use raw SQL")]
    public async Task ArrayJoin_ExplodesArrayIntoRows()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert events with tags
        context.Events.AddRange(
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Login", Tags = new[] { "auth", "user", "security" } },
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Purchase", Tags = new[] { "commerce", "payment" } },
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Logout", Tags = new[] { "auth", "user" } }
        );
        await context.SaveChangesAsync();

        // Perform ARRAY JOIN
        var results = await context.Events
            .ArrayJoin(
                e => e.Tags,
                (e, tag) => new { e.Name, Tag = tag })
            .OrderBy(x => x.Name)
            .ThenBy(x => x.Tag)
            .ToListAsync();

        // Login: 3 tags, Logout: 2 tags, Purchase: 2 tags = 7 total
        Assert.Equal(7, results.Count);

        // Verify Login has 3 rows
        var loginTags = results.Where(x => x.Name == "Login").Select(x => x.Tag).ToList();
        Assert.Equal(3, loginTags.Count);
        Assert.Contains("auth", loginTags);
        Assert.Contains("user", loginTags);
        Assert.Contains("security", loginTags);

        // Verify Purchase has 2 rows
        var purchaseTags = results.Where(x => x.Name == "Purchase").Select(x => x.Tag).ToList();
        Assert.Equal(2, purchaseTags.Count);
        Assert.Contains("commerce", purchaseTags);
        Assert.Contains("payment", purchaseTags);
    }

    [Fact(Skip = "ARRAY JOIN LINQ extension requires EF Core pipeline modification - use raw SQL")]
    public async Task ArrayJoin_ExcludesRowsWithEmptyArrays()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert events - one with tags, one without
        context.Events.AddRange(
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "WithTags", Tags = new[] { "tag1", "tag2" } },
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "NoTags", Tags = Array.Empty<string>() }
        );
        await context.SaveChangesAsync();

        // Perform ARRAY JOIN - should exclude NoTags event
        var results = await context.Events
            .ArrayJoin(
                e => e.Tags,
                (e, tag) => new { e.Name, Tag = tag })
            .ToListAsync();

        // Only WithTags should appear (2 rows)
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("WithTags", r.Name));
    }

    [Fact(Skip = "ARRAY JOIN LINQ extension requires EF Core pipeline modification - use raw SQL")]
    public async Task LeftArrayJoin_PreservesRowsWithEmptyArrays()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert events - one with tags, one without
        context.Events.AddRange(
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "WithTags", Tags = new[] { "tag1" } },
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "NoTags", Tags = Array.Empty<string>() }
        );
        await context.SaveChangesAsync();

        // Perform LEFT ARRAY JOIN - should include NoTags event
        var results = await context.Events
            .LeftArrayJoin(
                e => e.Tags,
                (e, tag) => new { e.Name, Tag = tag ?? "(empty)" })
            .OrderBy(x => x.Name)
            .ToListAsync();

        // Should have 2 rows: NoTags with null/empty, WithTags with tag1
        Assert.Equal(2, results.Count);

        var noTags = results.First(x => x.Name == "NoTags");
        Assert.Equal("(empty)", noTags.Tag);

        var withTags = results.First(x => x.Name == "WithTags");
        Assert.Equal("tag1", withTags.Tag);
    }

    [Fact(Skip = "ARRAY JOIN LINQ extension requires EF Core pipeline modification - use raw SQL")]
    public async Task ArrayJoin_WithFilter_AppliesFilterAfterJoin()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        context.Events.AddRange(
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Event1", Tags = new[] { "auth", "user" } },
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Event2", Tags = new[] { "commerce", "auth" } }
        );
        await context.SaveChangesAsync();

        // ARRAY JOIN then filter for "auth" tags
        var results = await context.Events
            .ArrayJoin(
                e => e.Tags,
                (e, tag) => new { e.Name, Tag = tag })
            .Where(x => x.Tag == "auth")
            .ToListAsync();

        // Both events have "auth" tag
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("auth", r.Tag));
    }

    [Fact(Skip = "ARRAY JOIN LINQ extension requires EF Core pipeline modification - use raw SQL")]
    public async Task ArrayJoin_WithGroupBy_CountsTagOccurrences()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        context.Events.AddRange(
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Event1", Tags = new[] { "auth", "user" } },
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Event2", Tags = new[] { "auth", "commerce" } },
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Event3", Tags = new[] { "auth", "user", "security" } }
        );
        await context.SaveChangesAsync();

        // ARRAY JOIN then GROUP BY tag to count occurrences
        var results = await context.Events
            .ArrayJoin(
                e => e.Tags,
                (e, tag) => new { e.Id, Tag = tag })
            .GroupBy(x => x.Tag)
            .Select(g => new { Tag = g.Key, Count = g.Count() })
            .OrderByDescending(x => x.Count)
            .ThenBy(x => x.Tag)
            .ToListAsync();

        // auth appears 3 times, user appears 2 times, commerce and security appear 1 time each
        Assert.Equal(4, results.Count);

        var auth = results.First(x => x.Tag == "auth");
        Assert.Equal(3, auth.Count);

        var user = results.First(x => x.Tag == "user");
        Assert.Equal(2, user.Count);
    }

    /// <summary>
    /// Demonstrates ARRAY JOIN using raw SQL - the recommended approach for ClickHouse ARRAY JOIN queries.
    /// </summary>
    [Fact]
    public async Task ArrayJoin_WithRawSql_ExplodesArrayIntoRows()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert events with tags
        context.Events.AddRange(
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Login", Tags = new[] { "auth", "user" } },
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "Purchase", Tags = new[] { "commerce" } }
        );
        await context.SaveChangesAsync();

        // Perform ARRAY JOIN using raw SQL
        var sql = @"
            SELECT e.Name, tag
            FROM ""ArrayJoinEvents"" AS e
            ARRAY JOIN e.Tags AS tag
            ORDER BY e.Name, tag";

        var results = await context.Database.SqlQueryRaw<ArrayJoinResult>(sql).ToListAsync();

        // Login: 2 tags, Purchase: 1 tag = 3 total
        Assert.Equal(3, results.Count);

        // Verify Login has 2 rows
        var loginTags = results.Where(x => x.Name == "Login").Select(x => x.Tag).ToList();
        Assert.Equal(2, loginTags.Count);
        Assert.Contains("auth", loginTags);
        Assert.Contains("user", loginTags);

        // Verify Purchase has 1 row
        var purchaseTags = results.Where(x => x.Name == "Purchase").Select(x => x.Tag).ToList();
        Assert.Single(purchaseTags);
        Assert.Contains("commerce", purchaseTags);
    }

    /// <summary>
    /// Demonstrates LEFT ARRAY JOIN using raw SQL to preserve rows with empty arrays.
    /// </summary>
    [Fact]
    public async Task LeftArrayJoin_WithRawSql_PreservesEmptyArrays()
    {
        await using var context = CreateContext();
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert events - one with tags, one without
        context.Events.AddRange(
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "WithTags", Tags = new[] { "tag1" } },
            new ArrayJoinEvent { Id = Guid.NewGuid(), Name = "NoTags", Tags = Array.Empty<string>() }
        );
        await context.SaveChangesAsync();

        // Perform LEFT ARRAY JOIN using raw SQL
        // Note: For empty arrays, LEFT ARRAY JOIN returns a row with empty string (default for String type)
        var sql = @"
            SELECT e.Name, tag AS Tag
            FROM ""ArrayJoinEvents"" AS e
            LEFT ARRAY JOIN e.Tags AS tag
            ORDER BY e.Name";

        var results = await context.Database.SqlQueryRaw<ArrayJoinResult>(sql).ToListAsync();

        // Should have 2 rows: NoTags with empty string (default), WithTags with tag1
        Assert.Equal(2, results.Count);

        // For empty arrays, ClickHouse's LEFT ARRAY JOIN returns a row with empty/default value
        var noTags = results.First(x => x.Name == "NoTags");
        Assert.Equal("", noTags.Tag); // Empty string is the default for String type

        var withTags = results.First(x => x.Name == "WithTags");
        Assert.Equal("tag1", withTags.Tag);
    }

    [Fact]
    public void ArrayJoin_ThrowsForNullSource()
    {
        IQueryable<ArrayJoinEvent> nullSource = null!;

        Assert.Throws<ArgumentNullException>(() =>
            nullSource.ArrayJoin(
                e => e.Tags,
                (e, tag) => new { e, tag }));
    }

    [Fact]
    public void ArrayJoin_ThrowsForNullArraySelector()
    {
        using var context = CreateContext();

        Assert.Throws<ArgumentNullException>(() =>
            context.Events.ArrayJoin(
                null!,
                (ArrayJoinEvent e, string tag) => new { e, tag }));
    }

    private ArrayJoinDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<ArrayJoinDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new ArrayJoinDbContext(options);
    }
}

public class ArrayJoinDbContext : DbContext
{
    public ArrayJoinDbContext(DbContextOptions<ArrayJoinDbContext> options) : base(options)
    {
    }

    public DbSet<ArrayJoinEvent> Events => Set<ArrayJoinEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ArrayJoinEvent>(entity =>
        {
            entity.ToTable("ArrayJoinEvents");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });
    }
}

public class ArrayJoinEvent
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string[] Tags { get; set; } = Array.Empty<string>();
}

/// <summary>
/// Result type for raw SQL ARRAY JOIN query.
/// </summary>
public class ArrayJoinResult
{
    public string Name { get; set; } = string.Empty;
    public string Tag { get; set; } = string.Empty;
}
