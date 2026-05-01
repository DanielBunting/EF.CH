using EF.CH.Extensions;
using EF.CH.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Xunit;

namespace EF.CH.Tests.Sql;

/// <summary>
/// Hygiene tests for the thread-local <see cref="ClickHouseQueryGenerationContext"/>.
/// Most setters in <c>ClickHouseQueryTranslationPostprocessor.Process</c> are conditional
/// (they only fire when the corresponding option is present), so any field left populated
/// from an earlier query — for instance because translation threw, or because the SQL
/// generator never reached that field's consumption site — would leak into the next query
/// compiled on the same thread. These tests pin that behaviour.
/// </summary>
public class QueryGenerationContextLeakTests
{
    [Fact]
    public void StaleQuerySettings_DoNotLeakIntoNextQuery()
    {
        // Simulate a previous query that left QuerySettings populated on the thread —
        // e.g. the SQL generator never reached GenerateLimitOffset because translation
        // threw between the postprocessor and SQL generation.
        ClickHouseQuerySqlGenerator.SetQuerySettings(new Dictionary<string, object>
        {
            ["max_threads"] = 4,
        });

        using var context = CreateContext();
        // Force EF Core to call GenerateLimitOffset (where SETTINGS would be emitted)
        // by adding a Take().
        var sql = context.LeakTestEntities.Where(e => e.Value > 0).Take(10).ToQueryString();

        Assert.DoesNotContain("SETTINGS", sql);
        Assert.DoesNotContain("max_threads", sql);
    }

    [Fact]
    public void StalePreWhereExpression_DoesNotLeakIntoNextQuery()
    {
        var fakePreWhere = new SqlFragmentExpression("1 = 1");
        ClickHouseQuerySqlGenerator.SetPreWhereExpression(fakePreWhere);

        using var context = CreateContext();
        var sql = context.LeakTestEntities.Where(e => e.Value > 0).ToQueryString();

        Assert.DoesNotContain("PREWHERE", sql);
        Assert.DoesNotContain("1 = 1", sql);
    }

    [Fact]
    public void StaleRawFilter_DoesNotLeakIntoNextQuery()
    {
        ClickHouseQuerySqlGenerator.SetRawFilter("/* leaked */ 1 = 1");

        using var context = CreateContext();
        var sql = context.LeakTestEntities.Where(e => e.Value > 0).ToQueryString();

        Assert.DoesNotContain("/* leaked */", sql);
    }

    [Fact]
    public void StaleGroupByModifier_DoesNotLeakIntoNextQuery()
    {
        ClickHouseQuerySqlGenerator.SetGroupByModifier(GroupByModifier.Rollup);

        using var context = CreateContext();
        var sql = context.LeakTestEntities
            .GroupBy(e => e.Name)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToQueryString();

        Assert.DoesNotContain("ROLLUP", sql);
    }

    [Fact]
    public void StaleLimitBy_DoesNotLeakIntoNextQuery()
    {
        ClickHouseQuerySqlGenerator.SetLimitBy(
            limit: 1,
            offset: null,
            expressions: new List<SqlExpression> { new SqlFragmentExpression("Name") });

        using var context = CreateContext();
        // Force GenerateLimitOffset by adding Take().
        var sql = context.LeakTestEntities.Where(e => e.Value > 0).Take(10).ToQueryString();

        Assert.DoesNotContain("LIMIT 1 BY", sql);
    }

    private static LeakTestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<LeakTestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new LeakTestDbContext(options);
    }
}

public class LeakTestDbContext : DbContext
{
    public LeakTestDbContext(DbContextOptions<LeakTestDbContext> options) : base(options)
    {
    }

    public DbSet<LeakTestEntity> LeakTestEntities => Set<LeakTestEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<LeakTestEntity>(entity =>
        {
            entity.ToTable("leak_test_entities");
            entity.HasKey(e => e.Id);
        });
    }
}

public class LeakTestEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
}
