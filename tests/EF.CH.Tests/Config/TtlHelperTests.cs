using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using EF.CH.Tests.Sql;
using Xunit;

namespace EF.CH.Tests.Config;

public class TtlHelperTests
{
    #region ClickHouseInterval Unit Tests

    [Fact]
    public void ClickHouseInterval_Days_GeneratesCorrectSql()
    {
        var interval = ClickHouseInterval.Days(30);

        Assert.Equal(30, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Day, interval.Unit);
        Assert.Equal("INTERVAL 30 DAY", interval.ToSql());
        Assert.Equal("INTERVAL 30 DAY", interval.ToString());
    }

    [Fact]
    public void ClickHouseInterval_Months_GeneratesCorrectSql()
    {
        var interval = ClickHouseInterval.Months(1);

        Assert.Equal(1, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Month, interval.Unit);
        Assert.Equal("INTERVAL 1 MONTH", interval.ToSql());
    }

    [Fact]
    public void ClickHouseInterval_Years_GeneratesCorrectSql()
    {
        var interval = ClickHouseInterval.Years(2);

        Assert.Equal(2, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Year, interval.Unit);
        Assert.Equal("INTERVAL 2 YEAR", interval.ToSql());
    }

    [Fact]
    public void ClickHouseInterval_Weeks_GeneratesCorrectSql()
    {
        var interval = ClickHouseInterval.Weeks(4);

        Assert.Equal(4, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Week, interval.Unit);
        Assert.Equal("INTERVAL 4 WEEK", interval.ToSql());
    }

    [Fact]
    public void ClickHouseInterval_Quarters_GeneratesCorrectSql()
    {
        var interval = ClickHouseInterval.Quarters(1);

        Assert.Equal(1, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Quarter, interval.Unit);
        Assert.Equal("INTERVAL 1 QUARTER", interval.ToSql());
    }

    [Fact]
    public void ClickHouseInterval_Hours_GeneratesCorrectSql()
    {
        var interval = ClickHouseInterval.Hours(24);

        Assert.Equal(24, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Hour, interval.Unit);
        Assert.Equal("INTERVAL 24 HOUR", interval.ToSql());
    }

    [Fact]
    public void ClickHouseInterval_Minutes_GeneratesCorrectSql()
    {
        var interval = ClickHouseInterval.Minutes(60);

        Assert.Equal(60, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Minute, interval.Unit);
        Assert.Equal("INTERVAL 60 MINUTE", interval.ToSql());
    }

    [Fact]
    public void ClickHouseInterval_Seconds_GeneratesCorrectSql()
    {
        var interval = ClickHouseInterval.Seconds(3600);

        Assert.Equal(3600, interval.Value);
        Assert.Equal(ClickHouseIntervalUnit.Second, interval.Unit);
        Assert.Equal("INTERVAL 3600 SECOND", interval.ToSql());
    }

    [Fact]
    public void ClickHouseInterval_ThrowsOnZero()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ClickHouseInterval.Days(0));
    }

    [Fact]
    public void ClickHouseInterval_ThrowsOnNegative()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ClickHouseInterval.Days(-1));
    }

    [Fact]
    public void ClickHouseInterval_Equality_Works()
    {
        var interval1 = ClickHouseInterval.Days(30);
        var interval2 = ClickHouseInterval.Days(30);
        var interval3 = ClickHouseInterval.Days(31);
        var interval4 = ClickHouseInterval.Months(30);

        Assert.Equal(interval1, interval2);
        Assert.True(interval1 == interval2);
        Assert.False(interval1 != interval2);

        Assert.NotEqual(interval1, interval3);
        Assert.False(interval1 == interval3);
        Assert.True(interval1 != interval3);

        Assert.NotEqual(interval1, interval4); // Same value, different unit
    }

    [Fact]
    public void ClickHouseInterval_GetHashCode_Works()
    {
        var interval1 = ClickHouseInterval.Days(30);
        var interval2 = ClickHouseInterval.Days(30);

        Assert.Equal(interval1.GetHashCode(), interval2.GetHashCode());
    }

    #endregion

    #region TimeSpan Overload Tests

    [Fact]
    public void HasTtl_WithTimeSpan_Days_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestTtlEntity>(entity =>
        {
            entity.ToTable("temp_data");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Id");
            entity.HasTtl(x => x.CreatedAt, TimeSpan.FromDays(30));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestTtlEntity))!;
        var ttl = entityType.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value as string;

        Assert.NotNull(ttl);
        Assert.Equal("\"CreatedAt\" + INTERVAL 30 DAY", ttl);
    }

    [Fact]
    public void HasTtl_WithTimeSpan_Hours_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestTtlEntity>(entity =>
        {
            entity.ToTable("hourly_cache");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Id");
            entity.HasTtl(x => x.CreatedAt, TimeSpan.FromHours(12)); // 12 hours (not a whole day)
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestTtlEntity))!;
        var ttl = entityType.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value as string;

        Assert.NotNull(ttl);
        Assert.Equal("\"CreatedAt\" + INTERVAL 12 HOUR", ttl);
    }

    [Fact]
    public void HasTtl_WithTimeSpan_Minutes_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestTtlEntity>(entity =>
        {
            entity.ToTable("minute_cache");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Id");
            entity.HasTtl(x => x.CreatedAt, TimeSpan.FromMinutes(30));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestTtlEntity))!;
        var ttl = entityType.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value as string;

        Assert.NotNull(ttl);
        Assert.Equal("\"CreatedAt\" + INTERVAL 30 MINUTE", ttl);
    }

    [Fact]
    public void HasTtl_WithTimeSpan_FractionalDays_UsesHours()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestTtlEntity>(entity =>
        {
            entity.ToTable("fractional_days");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Id");
            entity.HasTtl(x => x.CreatedAt, TimeSpan.FromDays(1.5)); // 36 hours
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestTtlEntity))!;
        var ttl = entityType.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value as string;

        Assert.NotNull(ttl);
        Assert.Equal("\"CreatedAt\" + INTERVAL 36 HOUR", ttl);
    }

    [Fact]
    public void HasTtl_WithTimeSpan_Seconds_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestTtlEntity>(entity =>
        {
            entity.ToTable("second_cache");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Id");
            entity.HasTtl(x => x.CreatedAt, TimeSpan.FromSeconds(90));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestTtlEntity))!;
        var ttl = entityType.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value as string;

        Assert.NotNull(ttl);
        Assert.Equal("\"CreatedAt\" + INTERVAL 90 SECOND", ttl);
    }

    [Fact]
    public void HasTtl_WithTimeSpan_ThrowsOnNegative()
    {
        var builder = new ModelBuilder();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            builder.Entity<TestTtlEntity>(entity =>
            {
                entity.ToTable("invalid");
                entity.HasKey(e => e.Id);
                entity.UseMergeTree("Id");
                entity.HasTtl(x => x.CreatedAt, TimeSpan.FromDays(-1));
            });
        });
    }

    #endregion

    #region ClickHouseInterval Overload Tests

    [Fact]
    public void HasTtl_WithInterval_Days_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestTtlEntity>(entity =>
        {
            entity.ToTable("interval_days");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Id");
            entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Days(30));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestTtlEntity))!;
        var ttl = entityType.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value as string;

        Assert.NotNull(ttl);
        Assert.Equal("\"CreatedAt\" + INTERVAL 30 DAY", ttl);
    }

    [Fact]
    public void HasTtl_WithInterval_Months_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestTtlEntity>(entity =>
        {
            entity.ToTable("monthly_archive");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Id");
            entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Months(1));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestTtlEntity))!;
        var ttl = entityType.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value as string;

        Assert.NotNull(ttl);
        Assert.Equal("\"CreatedAt\" + INTERVAL 1 MONTH", ttl);
    }

    [Fact]
    public void HasTtl_WithInterval_Years_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestTtlEntity>(entity =>
        {
            entity.ToTable("yearly_archive");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Id");
            entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Years(1));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestTtlEntity))!;
        var ttl = entityType.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value as string;

        Assert.NotNull(ttl);
        Assert.Equal("\"CreatedAt\" + INTERVAL 1 YEAR", ttl);
    }

    [Fact]
    public void HasTtl_WithInterval_Quarters_SetsAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestTtlEntity>(entity =>
        {
            entity.ToTable("quarterly_data");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree("Id");
            entity.HasTtl(x => x.CreatedAt, ClickHouseInterval.Quarters(1));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestTtlEntity))!;
        var ttl = entityType.FindAnnotation(ClickHouseAnnotationNames.Ttl)?.Value as string;

        Assert.NotNull(ttl);
        Assert.Equal("\"CreatedAt\" + INTERVAL 1 QUARTER", ttl);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_WithTimeSpanTtl_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "temp_data",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "CreatedAt", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "Data", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "CreatedAt" });
        operation.AddAnnotation(ClickHouseAnnotationNames.Ttl, "\"CreatedAt\" + INTERVAL 30 DAY");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = MergeTree()", sql);
        Assert.Contains("TTL \"CreatedAt\" + INTERVAL 30 DAY", sql);
    }

    [Fact]
    public void CreateTable_WithIntervalTtl_GeneratesCorrectDdl()
    {
        using var context = CreateContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "audit_log",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "CreatedAt", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "Action", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "CreatedAt" });
        operation.AddAnnotation(ClickHouseAnnotationNames.Ttl, "\"CreatedAt\" + INTERVAL 1 MONTH");

        var sql = GenerateSql(generator, operation);

        Assert.Contains("ENGINE = MergeTree()", sql);
        Assert.Contains("TTL \"CreatedAt\" + INTERVAL 1 MONTH", sql);
    }

    #endregion

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new TestDbContext(options);
    }

    private static IMigrationsSqlGenerator GetMigrationsSqlGenerator(DbContext context)
    {
        return ((IInfrastructure<IServiceProvider>)context).Instance.GetService<IMigrationsSqlGenerator>()!;
    }

    private static string GenerateSql(IMigrationsSqlGenerator generator, MigrationOperation operation)
    {
        var commands = generator.Generate(new[] { operation });
        return string.Join("\n", commands.Select(c => c.CommandText));
    }
}

public class TestTtlEntity
{
    public Guid Id { get; set; }
    public DateTime CreatedAt { get; set; }
    public string Data { get; set; } = string.Empty;
}
