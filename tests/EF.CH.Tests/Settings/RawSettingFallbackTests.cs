using EF.CH.BulkInsert;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Settings;

/// <summary>
/// Confirms that the raw <c>WithSetting(string, object)</c> escape hatch
/// continues to work for settings outside the typed
/// <c>ClickHouseSettings</c> catalogue (forward compat / undocumented
/// settings).
/// </summary>
public class RawSettingFallbackTests
{
    [Fact]
    public void Raw_UnknownSetting_GeneratesSettingsClause()
    {
        using var context = CreateContext();

        var sql = context.TestEntities
            .WithSetting("rare_undocumented_setting", 42)
            .Where(e => e.Value > 0)
            .ToQueryString();

        Assert.Contains("SETTINGS", sql);
        Assert.Contains("rare_undocumented_setting = 42", sql);
    }

    [Fact]
    public void Raw_StringValue_QuotesProperly()
    {
        using var context = CreateContext();

        var sql = context.TestEntities
            .WithSetting("string_setting", "hello")
            .ToQueryString();

        Assert.Contains("string_setting = 'hello'", sql);
    }

    [Fact]
    public void Raw_BoolValue_RendersAsZeroOrOne()
    {
        using var context = CreateContext();

        var trueSql = context.TestEntities
            .WithSetting("some_flag", true)
            .ToQueryString();
        var falseSql = context.TestEntities
            .WithSetting("some_flag", false)
            .ToQueryString();

        Assert.Contains("some_flag = 1", trueSql);
        Assert.Contains("some_flag = 0", falseSql);
    }

    [Fact]
    public void Raw_NullName_Throws()
    {
        using var context = CreateContext();

        Assert.Throws<ArgumentNullException>(() =>
            context.TestEntities.WithSetting((string)null!, 1).ToQueryString());
    }

    [Fact]
    public void BulkInsert_Raw_UnknownSetting_StoresInDictionary()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithSetting("future_clickhouse_setting", "value");

        Assert.Single(options.Settings);
        Assert.Equal("value", options.Settings["future_clickhouse_setting"]);
    }

    [Fact]
    public void BulkInsert_Raw_LastWriteWins()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithSetting("max_block_size", 1000)
            .WithSetting("max_block_size", 2000);

        Assert.Single(options.Settings);
        Assert.Equal(2000, options.Settings["max_block_size"]);
    }

    [Fact]
    public void Mixed_TypedAndRaw_AllAppearInSql()
    {
        using var context = CreateContext();

        var sql = context.TestEntities
            .WithSetting(EF.CH.Infrastructure.ClickHouseSettings.MaxThreads, 4)
            .WithSetting("escape_hatch_setting", 99)
            .ToQueryString();

        Assert.Contains("max_threads = 4", sql);
        Assert.Contains("escape_hatch_setting = 99", sql);
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new TestDbContext(options);
    }

    public class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
    {
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
    }
}
