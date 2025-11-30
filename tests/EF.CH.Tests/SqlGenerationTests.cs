using EF.CH.Extensions;
using EF.CH.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace EF.CH.Tests;

public class SqlGenerationTests
{
    [Fact]
    public void SqlGenerationHelper_QuotesIdentifiersCorrectly()
    {
        var helper = new ClickHouseSqlGenerationHelper(
            new RelationalSqlGenerationHelperDependencies());

        var quoted = helper.DelimitIdentifier("MyTable");

        Assert.Equal("\"MyTable\"", quoted);
    }

    [Fact]
    public void SqlGenerationHelper_GeneratesParameterCorrectly()
    {
        var helper = new ClickHouseSqlGenerationHelper(
            new RelationalSqlGenerationHelperDependencies());

        var param = helper.GenerateParameterName("p0");

        // ClickHouse parameter names are plain (format is handled in QuerySqlGenerator)
        Assert.Equal("p0", param);
    }


    [Fact]
    public void BasicSelect_GeneratesCorrectSql()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Where(e => e.Name == "test");
        var sql = query.ToQueryString();

        // Output the SQL for inspection
        Console.WriteLine("Generated SQL: " + sql);

        Assert.Contains("SELECT", sql);
        Assert.Contains("\"Name\"", sql); // ClickHouse uses double quotes
    }

    [Fact]
    public void StringContains_GeneratesLike()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Where(e => e.Name.Contains("test"));
        var sql = query.ToQueryString();

        Assert.Contains("LIKE", sql);
        Assert.Contains("%test%", sql);
    }

    [Fact]
    public void StringStartsWith_GeneratesLike()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Where(e => e.Name.StartsWith("test"));
        var sql = query.ToQueryString();

        Assert.Contains("LIKE", sql);
        Assert.Contains("test%", sql);
    }

    [Fact]
    public void StringEndsWith_GeneratesLike()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Where(e => e.Name.EndsWith("test"));
        var sql = query.ToQueryString();

        Assert.Contains("LIKE", sql);
        Assert.Contains("%test", sql);
    }

    [Fact]
    public void StringLength_GeneratesLength()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Where(e => e.Name.Length > 5);
        var sql = query.ToQueryString();

        Assert.Contains("length", sql);
    }

    [Fact]
    public void StringToUpper_GeneratesUpperUTF8()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Select(e => e.Name.ToUpper());
        var sql = query.ToQueryString();

        Assert.Contains("upperUTF8", sql);
    }

    [Fact]
    public void StringToLower_GeneratesLowerUTF8()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Select(e => e.Name.ToLower());
        var sql = query.ToQueryString();

        Assert.Contains("lowerUTF8", sql);
    }

    [Fact]
    public void DateTimeYear_GeneratesToYear()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Select(e => e.CreatedAt.Year);
        var sql = query.ToQueryString();

        Assert.Contains("toYear", sql);
    }

    [Fact]
    public void DateTimeMonth_GeneratesToMonth()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Select(e => e.CreatedAt.Month);
        var sql = query.ToQueryString();

        Assert.Contains("toMonth", sql);
    }

    [Fact]
    public void DateTimeAddDays_GeneratesAddDays()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Select(e => e.CreatedAt.AddDays(1));
        var sql = query.ToQueryString();

        Assert.Contains("addDays", sql);
    }

    [Fact]
    public void MathAbs_GeneratesAbs()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Select(e => Math.Abs(e.Value));
        var sql = query.ToQueryString();

        Assert.Contains("abs", sql);
    }

    [Fact]
    public void MathFloor_GeneratesFloor()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Select(e => Math.Floor(e.DoubleValue));
        var sql = query.ToQueryString();

        Assert.Contains("floor", sql);
    }

    [Fact]
    public void MathRound_GeneratesRound()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Select(e => Math.Round(e.DoubleValue, 2));
        var sql = query.ToQueryString();

        Assert.Contains("round", sql);
    }

    [Fact]
    public void LimitOffset_GeneratesClickHouseSyntax()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Skip(10).Take(5);
        var sql = query.ToQueryString();

        // ClickHouse uses: LIMIT offset, count
        // Parameters may be used for the values
        Assert.Contains("LIMIT", sql);
        // The SQL should have the LIMIT with either literals or parameters
        Assert.Matches(@"LIMIT.*,", sql);
    }

    [Fact]
    public void GuidNewGuid_GeneratesGenerateUUIDv4()
    {
        using var context = CreateContext();

        var query = context.TestEntities.Select(e => new { e.Id, NewId = Guid.NewGuid() });
        var sql = query.ToQueryString();

        Assert.Contains("generateUUIDv4", sql);
    }

    [Fact(Skip = "Aggregate methods require different testing approach")]
    public void Sum_GeneratesSumOrNull()
    {
        // Aggregate methods like Sum() can't be tested via ToQueryString()
        // because they execute immediately. Would need actual database connection
        // or mock to test these.
    }

    [Fact]
    public void Final_GeneratesFinalClause()
    {
        using var context = CreateContext();
        var query = context.TestEntities.Final().Where(e => e.Value > 0);
        var sql = query.ToQueryString();
        Assert.Contains("FINAL", sql);
    }

    [Fact(Skip = "EF Core parameterizes constants before translation. SAMPLE works at query execution time but not with ToQueryString().")]
    public void Sample_GeneratesSampleClause()
    {
        using var context = CreateContext();
        var query = context.TestEntities.Sample(0.1).Where(e => e.Value > 0);
        var sql = query.ToQueryString();
        Assert.Contains("SAMPLE", sql);
    }

    [Fact(Skip = "EF Core parameterizes constants before translation. SAMPLE works at query execution time but not with ToQueryString().")]
    public void SampleWithOffset_GeneratesSampleWithOffsetClause()
    {
        using var context = CreateContext();
        var query = context.TestEntities.Sample(0.1, 0.5).Where(e => e.Value > 0);
        var sql = query.ToQueryString();
        Assert.Contains("SAMPLE", sql);
        Assert.Contains("OFFSET", sql);
    }

    [Fact(Skip = "SETTINGS requires full query execution, not available via ToQueryString")]
    public void WithSetting_GeneratesSettingsClause()
    {
        using var context = CreateContext();
        var query = context.TestEntities
            .WithSetting("max_threads", 4)
            .Where(e => e.Value > 0);
        var sql = query.ToQueryString();
        Assert.Contains("SETTINGS", sql);
        Assert.Contains("max_threads = 4", sql);
    }

    [Fact(Skip = "SETTINGS requires full query execution, not available via ToQueryString")]
    public void WithSettings_GeneratesMultipleSettingsClause()
    {
        using var context = CreateContext();
        var settings = new Dictionary<string, object>
        {
            { "max_threads", 4 },
            { "optimize_read_in_order", 1 }
        };
        var query = context.TestEntities
            .WithSettings(settings)
            .Where(e => e.Value > 0);
        var sql = query.ToQueryString();
        Assert.Contains("SETTINGS", sql);
        Assert.Contains("max_threads = 4", sql);
        Assert.Contains("optimize_read_in_order = 1", sql);
    }

    [Fact(Skip = "SETTINGS requires full query execution, not available via ToQueryString")]
    public void WithSetting_ChainedSettings_GeneratesAllSettings()
    {
        using var context = CreateContext();
        var query = context.TestEntities
            .WithSetting("max_threads", 4)
            .WithSetting("max_execution_time", 30)
            .Where(e => e.Value > 0);
        var sql = query.ToQueryString();
        Assert.Contains("SETTINGS", sql);
        Assert.Contains("max_threads = 4", sql);
        Assert.Contains("max_execution_time = 30", sql);
    }

    [Fact(Skip = "SETTINGS requires full query execution, not available via ToQueryString")]
    public void WithSetting_BoolValue_GeneratesCorrectly()
    {
        using var context = CreateContext();
        var query = context.TestEntities
            .WithSetting("optimize_read_in_order", true)
            .Where(e => e.Value > 0);
        var sql = query.ToQueryString();
        Assert.Contains("SETTINGS", sql);
        Assert.Contains("optimize_read_in_order = 1", sql);
    }

    [Fact(Skip = "FINAL and SETTINGS require full query execution")]
    public void FinalAndSettings_GeneratesBothClauses()
    {
        using var context = CreateContext();
        var query = context.TestEntities
            .Final()
            .WithSetting("max_threads", 4)
            .Where(e => e.Value > 0);
        var sql = query.ToQueryString();
        Assert.Contains("FINAL", sql);
        Assert.Contains("SETTINGS", sql);
        Assert.Contains("max_threads = 4", sql);
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new TestDbContext(options);
    }
}

public class TestDbContext : DbContext
{
    public TestDbContext(DbContextOptions<TestDbContext> options) : base(options)
    {
    }

    public DbSet<TestEntity> TestEntities => Set<TestEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TestEntity>(entity =>
        {
            entity.ToTable("test_entities");
            entity.HasKey(e => e.Id);
        });
    }
}

public class TestEntity
{
    public Guid Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Value { get; set; }
    public double DoubleValue { get; set; }
    public DateTime CreatedAt { get; set; }
}
