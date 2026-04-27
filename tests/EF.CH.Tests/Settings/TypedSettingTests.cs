using EF.CH.BulkInsert;
using EF.CH.Extensions;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Settings;

/// <summary>
/// Verifies that the typed <c>WithSetting&lt;T&gt;(Setting&lt;T&gt;, T)</c>
/// overloads produce SQL byte-identical to the raw
/// <c>WithSetting(string, object)</c> form. The typed form is purely a
/// compile-time check; both routes funnel into the same SETTINGS-clause
/// formatter.
/// </summary>
public class TypedSettingTests
{
    [Fact]
    public void TypedAndRaw_MaxThreads_ProduceIdenticalSql()
    {
        using var context = CreateContext();

        var typed = context.TestEntities
            .WithSetting(ClickHouseSettings.MaxThreads, 4)
            .Where(e => e.Value > 0)
            .ToQueryString();

        var raw = context.TestEntities
            .WithSetting("max_threads", 4)
            .Where(e => e.Value > 0)
            .ToQueryString();

        Assert.Equal(raw, typed);
        Assert.Contains("SETTINGS", typed);
        Assert.Contains("max_threads = 4", typed);
    }

    [Fact]
    public void TypedAndRaw_OptimizeReadInOrder_ProduceIdenticalSql()
    {
        using var context = CreateContext();

        var typed = context.TestEntities
            .WithSetting(ClickHouseSettings.OptimizeReadInOrder, true)
            .Where(e => e.Value > 0)
            .ToQueryString();

        var raw = context.TestEntities
            .WithSetting("optimize_read_in_order", true)
            .Where(e => e.Value > 0)
            .ToQueryString();

        Assert.Equal(raw, typed);
        Assert.Contains("optimize_read_in_order = 1", typed);
    }

    [Fact]
    public void TypedAndRaw_MaxBlockSize_ProduceIdenticalSql()
    {
        using var context = CreateContext();

        var typed = context.TestEntities
            .WithSetting(ClickHouseSettings.MaxBlockSize, 65536L)
            .ToQueryString();

        var raw = context.TestEntities
            .WithSetting("max_block_size", 65536L)
            .ToQueryString();

        Assert.Equal(raw, typed);
        Assert.Contains("max_block_size = 65536", typed);
    }

    [Fact]
    public void TypedAndRaw_MaxExecutionTime_ProduceIdenticalSql()
    {
        using var context = CreateContext();

        var typed = context.TestEntities
            .WithSetting(ClickHouseSettings.MaxExecutionTime, 30)
            .ToQueryString();

        var raw = context.TestEntities
            .WithSetting("max_execution_time", 30)
            .ToQueryString();

        Assert.Equal(raw, typed);
        Assert.Contains("max_execution_time = 30", typed);
    }

    [Fact]
    public void Typed_Chained_GeneratesAllSettings()
    {
        using var context = CreateContext();

        var sql = context.TestEntities
            .WithSetting(ClickHouseSettings.MaxThreads, 4)
            .WithSetting(ClickHouseSettings.OptimizeReadInOrder, true)
            .WithSetting(ClickHouseSettings.MaxExecutionTime, 30)
            .Where(e => e.Value > 0)
            .ToQueryString();

        Assert.Contains("SETTINGS", sql);
        Assert.Contains("max_threads = 4", sql);
        Assert.Contains("optimize_read_in_order = 1", sql);
        Assert.Contains("max_execution_time = 30", sql);
    }

    [Fact]
    public void TypedSetting_ExposesUnderlyingName()
    {
        Assert.Equal("max_insert_threads", ClickHouseSettings.MaxInsertThreads.Name);
        Assert.Equal("max_threads", ClickHouseSettings.MaxThreads.Name);
        Assert.Equal("optimize_read_in_order", ClickHouseSettings.OptimizeReadInOrder.Name);
        Assert.Equal("async_insert", ClickHouseSettings.AsyncInsert.Name);
        Assert.Equal("wait_for_async_insert", ClickHouseSettings.WaitForAsyncInsert.Name);
        Assert.Equal("max_block_size", ClickHouseSettings.MaxBlockSize.Name);
        Assert.Equal("max_rows_to_read", ClickHouseSettings.MaxRowsToRead.Name);
        Assert.Equal("enable_full_text_index", ClickHouseSettings.EnableFullTextIndex.Name);
    }

    [Fact]
    public void SettingFactory_Of_ConstructsTypedHandle()
    {
        var custom = Setting.Of<int>("user_defined_setting");
        Assert.Equal("user_defined_setting", custom.Name);

        using var context = CreateContext();

        var typed = context.TestEntities.WithSetting(custom, 7).ToQueryString();
        var raw = context.TestEntities.WithSetting("user_defined_setting", 7).ToQueryString();

        Assert.Equal(raw, typed);
    }

    [Fact]
    public void Typed_NullSetting_Throws()
    {
        using var context = CreateContext();

        Assert.Throws<ArgumentNullException>(() =>
            context.TestEntities.WithSetting((Setting<int>)null!, 1).ToQueryString());
    }

    [Fact]
    public void BulkInsert_TypedWithSetting_StoresUnderCorrectName()
    {
        var options = new ClickHouseBulkInsertOptions()
            .WithSetting(ClickHouseSettings.MaxBlockSize, 1000L);

        Assert.Single(options.Settings);
        Assert.Equal(1000L, options.Settings["max_block_size"]);
    }

    [Fact]
    public void BulkInsert_TypedAndRaw_ProduceEquivalentDictionary()
    {
        var typed = new ClickHouseBulkInsertOptions()
            .WithSetting(ClickHouseSettings.MaxInsertThreads, 4)
            .WithSetting(ClickHouseSettings.InsertQuorum, 2);

        var raw = new ClickHouseBulkInsertOptions()
            .WithSetting("max_insert_threads", 4)
            .WithSetting("insert_quorum", 2);

        Assert.Equal(raw.Settings["max_insert_threads"], typed.Settings["max_insert_threads"]);
        Assert.Equal(raw.Settings["insert_quorum"], typed.Settings["insert_quorum"]);
    }

    [Fact]
    public void BulkInsert_TypedWithSetting_NullSetting_Throws()
    {
        var options = new ClickHouseBulkInsertOptions();
        Assert.Throws<ArgumentNullException>(() =>
            options.WithSetting<int>(null!, 1));
    }

    // -------------------------------------------------------------------
    // Compile-time guarantees (reference only — these blocks intentionally
    // do NOT compile if uncommented). Kept here as documentation of what
    // the typed overload rejects:
    //
    //   ClickHouseBulkInsertOptions opts = new();
    //   opts.WithSetting(ClickHouseSettings.MaxInsertThreads, "four");
    //   //                                                    ^^^^^^ string vs Setting<int>
    //
    //   IQueryable<TestEntity> q = ...;
    //   q.WithSetting(ClickHouseSettings.OptimizeReadInOrder, 1);
    //   //                                                    ^ int vs Setting<bool>
    // -------------------------------------------------------------------

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
