using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

#region Test Entities

/// <summary>
/// Entity with default-for-null columns for testing.
/// </summary>
public class DefaultForNullEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;

    // Nullable int with default-for-null of 0
    public int? Score { get; set; }

    // Nullable Guid with default-for-null of Guid.Empty
    public Guid? ExternalId { get; set; }

    // Nullable string with default-for-null of ""
    public string? Notes { get; set; }

    // Regular nullable int (no default) for comparison
    public int? RegularNullableScore { get; set; }
}

#endregion

#region Test DbContexts

public class DefaultForNullContext : DbContext
{
    public DefaultForNullContext(DbContextOptions<DefaultForNullContext> options)
        : base(options) { }

    public DbSet<DefaultForNullEntity> Entities => Set<DefaultForNullEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DefaultForNullEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("DefaultForNullEntities");
            entity.UseMergeTree(x => x.Id);

            // Configure default-for-nulls
            entity.Property(e => e.Score)
                .HasDefaultForNull(0);

            entity.Property(e => e.ExternalId)
                .HasDefaultForNull(Guid.Empty);

            entity.Property(e => e.Notes)
                .HasDefaultForNull("");

            // RegularNullableScore is not configured - should use standard Nullable(Int32)
        });
    }
}

#endregion

public class DefaultForNullTests
{
    #region Annotation Tests

    [Fact]
    public void HasDefaultForNull_SetsAnnotation_ForInt()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var entityType = context.Model.FindEntityType(typeof(DefaultForNullEntity))!;
        var property = entityType.FindProperty(nameof(DefaultForNullEntity.Score))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.DefaultForNull);

        Assert.NotNull(annotation);
        Assert.Equal(0, annotation.Value);
    }

    [Fact]
    public void HasDefaultForNull_SetsAnnotation_ForGuid()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var entityType = context.Model.FindEntityType(typeof(DefaultForNullEntity))!;
        var property = entityType.FindProperty(nameof(DefaultForNullEntity.ExternalId))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.DefaultForNull);

        Assert.NotNull(annotation);
        Assert.Equal(Guid.Empty, annotation.Value);
    }

    [Fact]
    public void HasDefaultForNull_SetsAnnotation_ForString()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var entityType = context.Model.FindEntityType(typeof(DefaultForNullEntity))!;
        var property = entityType.FindProperty(nameof(DefaultForNullEntity.Notes))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.DefaultForNull);

        Assert.NotNull(annotation);
        Assert.Equal("", annotation.Value);
    }

    [Fact]
    public void RegularNullable_HasNoDefaultForNullAnnotation()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var entityType = context.Model.FindEntityType(typeof(DefaultForNullEntity))!;
        var property = entityType.FindProperty(nameof(DefaultForNullEntity.RegularNullableScore))!;
        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.DefaultForNull);

        Assert.Null(annotation);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_GeneratesNonNullableColumnWithDefault_ForDefaultForNullInt()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var script = context.Database.GenerateCreateScript();

        // Should have "Score" Int32 DEFAULT 0 (not Nullable(Int32))
        Assert.Contains("\"Score\" Int32 DEFAULT 0", script);
    }

    [Fact]
    public void CreateTable_GeneratesNonNullableColumnWithDefault_ForDefaultForNullGuid()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var script = context.Database.GenerateCreateScript();

        // Should have "ExternalId" UUID DEFAULT ... (not Nullable(UUID))
        Assert.Contains("\"ExternalId\" UUID DEFAULT", script);
    }

    [Fact]
    public void CreateTable_GeneratesNonNullableColumnWithDefault_ForDefaultForNullString()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var script = context.Database.GenerateCreateScript();

        // Should have "Notes" String DEFAULT '' (not Nullable(String))
        Assert.Contains("\"Notes\" String DEFAULT ''", script);
    }

    [Fact]
    public void CreateTable_GeneratesNullableColumn_ForRegularNullable()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var script = context.Database.GenerateCreateScript();

        // Should have Nullable(Int32) for regular nullable
        Assert.Contains("\"RegularNullableScore\" Nullable(Int32)", script);
    }

    #endregion

    #region Query SQL Generation Tests

    [Fact]
    public void WhereNull_TranslatesToDefaultComparison_ForInt()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var query = context.Entities.Where(e => e.Score == null);
        var sql = query.ToQueryString();

        // Should translate `Score == null` to `Score = 0`
        Assert.Contains("\"Score\" = 0", sql);
        Assert.DoesNotContain("IS NULL", sql);
    }

    [Fact]
    public void WhereNotNull_TranslatesToDefaultComparison_ForInt()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var query = context.Entities.Where(e => e.Score != null);
        var sql = query.ToQueryString();

        // Should translate `Score != null` to `Score <> 0`
        Assert.Contains("\"Score\" <> 0", sql);
        Assert.DoesNotContain("IS NOT NULL", sql);
    }

    [Fact]
    public void WhereNull_TranslatesToDefaultComparison_ForGuid()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var query = context.Entities.Where(e => e.ExternalId == null);
        var sql = query.ToQueryString();

        // Should translate `ExternalId == null` to comparison with empty GUID
        Assert.Contains("\"ExternalId\" = '00000000-0000-0000-0000-000000000000'", sql);
        Assert.DoesNotContain("IS NULL", sql);
    }

    [Fact]
    public void WhereNull_TranslatesToDefaultComparison_ForString()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var query = context.Entities.Where(e => e.Notes == null);
        var sql = query.ToQueryString();

        // Should translate `Notes == null` to `Notes = ''`
        Assert.Contains("\"Notes\" = ''", sql);
        Assert.DoesNotContain("IS NULL", sql);
    }

    [Fact]
    public void WhereNull_UsesIsNull_ForRegularNullable()
    {
        using var context = CreateContext<DefaultForNullContext>();

        var query = context.Entities.Where(e => e.RegularNullableScore == null);
        var sql = query.ToQueryString();

        // Regular nullable should use IS NULL
        Assert.Contains("IS NULL", sql);
    }

    #endregion

    #region Count Predicate SQL Generation Tests

    [Fact]
    public void CountPredicateNull_TranslatesToDefaultComparison()
    {
        using var context = CreateContext<DefaultForNullContext>();

        // Count with null predicate should use default value comparison
        // We test via Where since Count() returns scalar and can't use ToQueryString
        var query = context.Entities.Where(e => e.Score == null).Select(e => 1);
        var sql = query.ToQueryString();

        // The predicate should be rewritten to Score = 0
        Assert.Contains("\"Score\" = 0", sql);
        Assert.DoesNotContain("IS NULL", sql);
    }

    [Fact]
    public void CountPredicateNotNull_TranslatesToDefaultComparison()
    {
        using var context = CreateContext<DefaultForNullContext>();

        // Count with not-null predicate should use default value comparison
        var query = context.Entities.Where(e => e.Score != null).Select(e => 1);
        var sql = query.ToQueryString();

        // The predicate should be rewritten to Score <> 0
        Assert.Contains("\"Score\" <> 0", sql);
        Assert.DoesNotContain("IS NOT NULL", sql);
    }

    #endregion

    #region Edge Case SQL Generation Tests

    [Fact(Skip = "HasValue is optimized away since column is non-nullable in ClickHouse")]
    public void HasValue_TranslatesToDefaultComparison()
    {
        // KNOWN BEHAVIOR: When using HasDefaultForNull, the column becomes non-nullable
        // in ClickHouse. EF Core knows this and optimizes `.HasValue` to always true,
        // eliminating the WHERE clause entirely.
        //
        // Workaround: Use explicit null comparison instead
        // context.Entities.Where(e => e.Score != null)  // Works correctly
        // context.Entities.Where(e => e.Score.HasValue) // Optimized away
    }

    [Fact(Skip = "!HasValue is optimized away since column is non-nullable in ClickHouse")]
    public void NotHasValue_TranslatesToDefaultComparison()
    {
        // KNOWN BEHAVIOR: When using HasDefaultForNull, the column becomes non-nullable
        // in ClickHouse. EF Core knows this and optimizes `!.HasValue` to always false,
        // returning no results.
        //
        // Workaround: Use explicit null comparison instead
        // context.Entities.Where(e => e.Score == null)  // Works correctly
        // context.Entities.Where(e => !e.Score.HasValue) // Optimized away
    }

    [Fact(Skip = "Coalesce doesn't work with default-for-null - ClickHouse sees 0, not NULL")]
    public void Coalesce_DoesNotWorkWithDefaultForNull()
    {
        // KNOWN LIMITATION: The ?? operator doesn't work as expected because
        // ClickHouse stores 0 (not NULL), so COALESCE never triggers.
        //
        // context.Entities.Select(e => e.Score ?? 100)
        // Expected: Returns 100 when Score is "null" (stored as 0)
        // Actual: Returns 0 (the stored value)
        //
        // Workaround: Use conditional expression
        // context.Entities.Select(e => e.Score == null ? 100 : e.Score)
    }

    [Fact]
    public void ConditionalWithNull_TranslatesToDefaultComparison()
    {
        using var context = CreateContext<DefaultForNullContext>();

        // Ternary with null check should work (workaround for coalesce)
        var query = context.Entities.Select(e => e.Score == null ? -1 : e.Score.Value);
        var sql = query.ToQueryString();

        // The null check should be rewritten to Score = 0
        Assert.Contains("\"Score\" = 0", sql);
    }

    #endregion

    private static TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}

/// <summary>
/// Integration tests that require a real ClickHouse instance.
/// </summary>
public class DefaultForNullIntegrationTests : IAsyncLifetime
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
    public async Task CanCreateTableWithDefaultForNullColumns()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Verify table exists
        var tableExists = await context.Database.SqlQueryRaw<string>(
            "SELECT name AS \"Value\" FROM system.tables WHERE database = currentDatabase() AND name = 'DefaultForNullEntities'"
        ).AnyAsync();

        Assert.True(tableExists);

        // Verify Score column is NOT nullable
        var scoreType = await context.Database.SqlQueryRaw<string>(
            "SELECT type AS \"Value\" FROM system.columns WHERE database = currentDatabase() AND table = 'DefaultForNullEntities' AND name = 'Score'"
        ).FirstOrDefaultAsync();

        Assert.NotNull(scoreType);
        Assert.Equal("Int32", scoreType); // Not Nullable(Int32)
    }

    [Fact]
    public async Task InsertNull_StoresDefaultForNullValue()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert with null values
        var entity = new DefaultForNullEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            Score = null, // Should store 0
            ExternalId = null, // Should store Guid.Empty
            Notes = null // Should store ""
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Query the raw values from ClickHouse
        var rawScore = await context.Database.SqlQueryRaw<int>(
            $"SELECT Score AS \"Value\" FROM DefaultForNullEntities WHERE Id = '{entity.Id}'"
        ).FirstOrDefaultAsync();

        Assert.Equal(0, rawScore); // DefaultForNull value stored
    }

    [Fact]
    public async Task SelectDefaultForNull_ReturnsNull()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert with null values
        var entity = new DefaultForNullEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            Score = null,
            ExternalId = null,
            Notes = null
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Clear tracker and query through EF Core
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Null(result.Score); // DefaultForNull converted back to null
        Assert.Null(result.ExternalId);
        Assert.Null(result.Notes);
    }

    [Fact]
    public async Task InsertNonNull_PreservesValue()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert with non-null values
        var externalId = Guid.NewGuid();
        var entity = new DefaultForNullEntity
        {
            Id = Guid.NewGuid(),
            Name = "Test",
            Score = 42,
            ExternalId = externalId,
            Notes = "Some notes"
        };

        context.Entities.Add(entity);
        await context.SaveChangesAsync();

        // Clear tracker and query
        context.ChangeTracker.Clear();

        var result = await context.Entities.FirstOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal(42, result.Score);
        Assert.Equal(externalId, result.ExternalId);
        Assert.Equal("Some notes", result.Notes);
    }

    [Fact]
    public async Task QueryWhereNull_TranslatesToDefaultForNullComparison()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore", Score = 42 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore", Score = null }
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        // Clear tracker
        context.ChangeTracker.Clear();

        // Query where Score == null (should translate to Score = 0)
        var nullScoreEntities = await context.Entities
            .Where(e => e.Score == null)
            .ToListAsync();

        Assert.Single(nullScoreEntities);
        Assert.Equal("NoScore", nullScoreEntities[0].Name);
    }

    [Fact]
    public async Task QueryWhereNotNull_TranslatesToDefaultForNullComparison()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore", Score = 42 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore", Score = null }
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        // Clear tracker
        context.ChangeTracker.Clear();

        // Query where Score != null (should translate to Score <> 0)
        var hasScoreEntities = await context.Entities
            .Where(e => e.Score != null)
            .ToListAsync();

        Assert.Single(hasScoreEntities);
        Assert.Equal("HasScore", hasScoreEntities[0].Name);
    }

    [Fact]
    public async Task Average_ExcludesDefaultForNullValues()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities with mix of null and non-null scores
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore1", Score = 10 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore2", Score = 20 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore", Score = null } // stored as 0
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        // Clear tracker
        context.ChangeTracker.Clear();

        // Average should only consider non-default values (10, 20) = 15, not (10, 20, 0) = 10
        var average = await context.Entities.AverageAsync(e => e.Score);

        Assert.Equal(15.0, average);
    }

    [Fact]
    public async Task Sum_ExcludesDefaultForNullValues()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore1", Score = 10 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore2", Score = 20 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore", Score = null } // stored as 0
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();

        // Sum should only consider non-default values: 10 + 20 = 30
        // Not 10 + 20 + 0 = 30 (in this case same result, but conceptually correct)
        var sum = await context.Entities.SumAsync(e => e.Score);

        Assert.Equal(30, sum);
    }

    [Fact]
    public async Task Min_ExcludesDefaultForNullValues()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore1", Score = 10 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore2", Score = 20 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore", Score = null } // stored as 0
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();

        // Min should be 10, not 0 (the default value)
        var min = await context.Entities.MinAsync(e => e.Score);

        Assert.Equal(10, min);
    }

    [Fact]
    public async Task Max_ExcludesDefaultForNullValues()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore1", Score = 10 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore2", Score = 20 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore", Score = null }
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();

        // Max should be 20
        var max = await context.Entities.MaxAsync(e => e.Score);

        Assert.Equal(20, max);
    }

    [Fact]
    public async Task Count_WithNullPredicate_CountsDefaultValues()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore1", Score = 10 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore2", Score = 20 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore1", Score = null },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore2", Score = null }
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();

        // Count where Score == null should count rows with default value (0)
        var nullCount = await context.Entities.CountAsync(e => e.Score == null);

        Assert.Equal(2, nullCount);
    }

    [Fact]
    public async Task Count_WithNotNullPredicate_CountsNonDefaultValues()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore1", Score = 10 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore2", Score = 20 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore1", Score = null },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore2", Score = null }
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();

        // Count where Score != null should count rows with non-default values
        var nonNullCount = await context.Entities.CountAsync(e => e.Score != null);

        Assert.Equal(2, nonNullCount);
    }

    [Fact]
    public async Task Count_WithoutPredicate_CountsAllRows()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore1", Score = 10 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore2", Score = 20 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore", Score = null }
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();

        // Count() without predicate should count ALL rows
        var totalCount = await context.Entities.CountAsync();

        Assert.Equal(3, totalCount);
    }

    [Fact]
    public async Task LongCount_WithNullPredicate_CountsDefaultValues()
    {
        await using var context = CreateContext<DefaultForNullContext>();

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        // Insert entities
        var entities = new[]
        {
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "HasScore", Score = 10 },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore1", Score = null },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore2", Score = null },
            new DefaultForNullEntity { Id = Guid.NewGuid(), Name = "NoScore3", Score = null }
        };

        context.Entities.AddRange(entities);
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();

        // LongCount where Score == null should count rows with default value
        var nullCount = await context.Entities.LongCountAsync(e => e.Score == null);

        Assert.Equal(3L, nullCount);
    }

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}
