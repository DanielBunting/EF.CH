using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// <c>.Final()</c> applies FINAL to a table during merge. ClickHouse only supports
/// FINAL on MergeTree-family engines; on Memory / Log / Distributed / etc. the server
/// returns a confusing error like "Storage X doesn't support FINAL". We catch that at
/// translation time so the failure surfaces close to the offending call site.
/// </summary>
public class FinalEngineValidationTests
{
    [Fact]
    public void Final_OnMergeTreeEntity_GeneratesFinalClause()
    {
        using var context = new EngineValidationContext();
        var sql = context.MergeTreeEntities.Final().ToQueryString();
        Assert.Contains("FINAL", sql);
    }

    [Fact]
    public void Final_OnReplacingMergeTreeEntity_GeneratesFinalClause()
    {
        using var context = new EngineValidationContext();
        var sql = context.ReplacingEntities.Final().ToQueryString();
        Assert.Contains("FINAL", sql);
    }

    [Fact]
    public void Final_OnMemoryEntity_ThrowsAtTranslation()
    {
        using var context = new EngineValidationContext();
        var ex = Assert.Throws<InvalidOperationException>(
            () => context.MemoryEntities.Final().ToQueryString());

        Assert.Contains("FINAL", ex.Message);
        Assert.Contains("Memory", ex.Message);
    }

    [Fact]
    public void Final_OnLogEntity_ThrowsAtTranslation()
    {
        using var context = new EngineValidationContext();
        var ex = Assert.Throws<InvalidOperationException>(
            () => context.LogEntities.Final().ToQueryString());

        Assert.Contains("FINAL", ex.Message);
        Assert.Contains("Log", ex.Message);
    }
}

#region Test context

public class EngineValidationContext : DbContext
{
    public DbSet<MergeTreeEntity> MergeTreeEntities => Set<MergeTreeEntity>();
    public DbSet<ReplacingEntity> ReplacingEntities => Set<ReplacingEntity>();
    public DbSet<MemoryEntity> MemoryEntities => Set<MemoryEntity>();
    public DbSet<LogEntity> LogEntities => Set<LogEntity>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseClickHouse("Host=localhost;Database=test");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MergeTreeEntity>(e =>
        {
            e.HasKey(x => x.Id);
            e.UseMergeTree(x => x.Id);
        });
        modelBuilder.Entity<ReplacingEntity>(e =>
        {
            e.HasKey(x => x.Id);
            e.UseReplacingMergeTree(x => x.Id);
        });
        modelBuilder.Entity<MemoryEntity>(e =>
        {
            e.HasKey(x => x.Id);
            e.UseMemoryEngine();
        });
        modelBuilder.Entity<LogEntity>(e =>
        {
            e.HasKey(x => x.Id);
            e.UseLogEngine();
        });
    }
}

public class MergeTreeEntity { public Guid Id { get; set; } }
public class ReplacingEntity { public Guid Id { get; set; } }
public class MemoryEntity { public Guid Id { get; set; } }
public class LogEntity { public Guid Id { get; set; } }

#endregion
