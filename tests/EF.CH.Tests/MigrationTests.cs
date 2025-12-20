using EF.CH.Extensions;
using EF.CH.Infrastructure;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class MigrationTests : IAsyncLifetime
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
    public void HistoryRepository_GeneratesCorrectCreateScript()
    {
        using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        var createScript = historyRepository.GetCreateScript();

        Assert.Contains("CREATE TABLE", createScript);
        Assert.Contains("\"__EFMigrationsHistory\"", createScript);
        Assert.Contains("\"MigrationId\" String", createScript);
        Assert.Contains("\"ProductVersion\" String", createScript);
        Assert.Contains("ENGINE = MergeTree()", createScript);
        Assert.Contains("ORDER BY", createScript);
    }

    [Fact]
    public void HistoryRepository_GeneratesCorrectInsertScript()
    {
        using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        var row = new HistoryRow("20241129120000_InitialCreate", "9.0.0");
        var insertScript = historyRepository.GetInsertScript(row);

        Assert.Contains("INSERT INTO", insertScript);
        Assert.Contains("\"__EFMigrationsHistory\"", insertScript);
        Assert.Contains("'20241129120000_InitialCreate'", insertScript);
        Assert.Contains("'9.0.0'", insertScript);
    }

    [Fact]
    public void HistoryRepository_GeneratesCorrectDeleteScript()
    {
        using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        var deleteScript = historyRepository.GetDeleteScript("20241129120000_InitialCreate");

        // ClickHouse uses ALTER TABLE DELETE for mutations
        Assert.Contains("ALTER TABLE", deleteScript);
        Assert.Contains("DELETE WHERE", deleteScript);
        Assert.Contains("'20241129120000_InitialCreate'", deleteScript);
    }

    [Fact]
    public async Task HistoryRepository_CanCreateAndCheckHistoryTable()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        // Create the history table
        var createScript = historyRepository.GetCreateIfNotExistsScript();
        await context.Database.ExecuteSqlRawAsync(createScript);

        // Check if table exists
        var exists = await historyRepository.ExistsAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task HistoryRepository_CanInsertAndRetrieveMigrations()
    {
        await using var context = CreateContext();
        var historyRepository = context.GetService<IHistoryRepository>();

        // Create the history table
        var createScript = historyRepository.GetCreateIfNotExistsScript();
        await context.Database.ExecuteSqlRawAsync(createScript);

        // Insert a migration record
        var insertScript = historyRepository.GetInsertScript(
            new HistoryRow("20241129120000_TestMigration", "9.0.0"));
        await context.Database.ExecuteSqlRawAsync(insertScript);

        // Retrieve all applied migrations
        var appliedMigrations = await historyRepository.GetAppliedMigrationsAsync();

        Assert.Contains(appliedMigrations, m => m.MigrationId == "20241129120000_TestMigration");
    }

    [Fact]
    public void MigrationsSqlGenerator_ThrowsForForeignKeyOperations()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var addFkOperation = new AddForeignKeyOperation
        {
            Name = "FK_Test",
            Table = "TestTable",
            Columns = new[] { "RefId" },
            PrincipalTable = "OtherTable",
            PrincipalColumns = new[] { "Id" }
        };

        var ex = Assert.Throws<ClickHouseUnsupportedOperationException>(
            () => generator.Generate(new[] { addFkOperation }));

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.ForeignKey, ex.Category);
        Assert.Contains("foreign key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MigrationsSqlGenerator_ThrowsForPrimaryKeyOperations()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var addPkOperation = new AddPrimaryKeyOperation
        {
            Name = "PK_Test",
            Table = "TestTable",
            Columns = new[] { "Id" }
        };

        var ex = Assert.Throws<ClickHouseUnsupportedOperationException>(
            () => generator.Generate(new[] { addPkOperation }));

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.PrimaryKey, ex.Category);
        Assert.Contains("primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MigrationsSqlGenerator_ThrowsForColumnRename()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var renameColOperation = new RenameColumnOperation
        {
            Name = "OldName",
            NewName = "NewName",
            Table = "TestTable"
        };

        var ex = Assert.Throws<ClickHouseUnsupportedOperationException>(
            () => generator.Generate(new[] { renameColOperation }));

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.ColumnRename, ex.Category);
        Assert.Contains("renaming", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MigrationsSqlGenerator_ThrowsForUniqueIndex()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var createIndexOperation = new CreateIndexOperation
        {
            Name = "IX_Test_Unique",
            Table = "TestTable",
            Columns = new[] { "Col1" },
            IsUnique = true
        };

        var ex = Assert.Throws<ClickHouseUnsupportedOperationException>(
            () => generator.Generate(new[] { createIndexOperation }));

        Assert.Equal(ClickHouseUnsupportedOperationException.OperationCategory.UniqueConstraint, ex.Category);
        Assert.Contains("unique", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesRenameTable()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var renameTableOperation = new RenameTableOperation
        {
            Name = "OldTable",
            NewName = "NewTable"
        };

        var commands = generator.Generate(new[] { renameTableOperation });
        var sql = commands.First().CommandText;

        Assert.Contains("RENAME TABLE", sql);
        Assert.Contains("\"OldTable\"", sql);
        Assert.Contains("\"NewTable\"", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesAlterColumn()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var alterColumnOperation = new AlterColumnOperation
        {
            Name = "Col1",
            Table = "TestTable",
            ClrType = typeof(string),
            ColumnType = "String",
            IsNullable = true
        };

        var commands = generator.Generate(new[] { alterColumnOperation });
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("MODIFY COLUMN", sql);
        Assert.Contains("\"Col1\"", sql);
        Assert.Contains("Nullable(String)", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesCreateIndex()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var createIndexOperation = new CreateIndexOperation
        {
            Name = "IX_Test",
            Table = "TestTable",
            Columns = new[] { "Col1" },
            IsUnique = false
        };

        var commands = generator.Generate(new[] { createIndexOperation });
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("ADD INDEX", sql);
        Assert.Contains("\"IX_Test\"", sql);
        Assert.Contains("TYPE minmax", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesDropIndex()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var dropIndexOperation = new DropIndexOperation
        {
            Name = "IX_Test",
            Table = "TestTable"
        };

        var commands = generator.Generate(new[] { dropIndexOperation });
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("DROP INDEX", sql);
        Assert.Contains("\"IX_Test\"", sql);
    }

    #region Skip Index DDL Generation Tests

    [Fact]
    public void MigrationsSqlGenerator_GeneratesBloomFilterIndex()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var createIndexOperation = new CreateIndexOperation
        {
            Name = "IX_Tags",
            Table = "TestTable",
            Columns = ["Tags"],
            IsUnique = false
        };
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.BloomFilter);
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity, 3);
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexParams, SkipIndexParams.ForBloomFilter(0.025));

        var commands = generator.Generate(new[] { createIndexOperation });
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("ADD INDEX", sql);
        Assert.Contains("\"IX_Tags\"", sql);
        Assert.Contains("TYPE bloom_filter(0.025)", sql);
        Assert.Contains("GRANULARITY 3", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesTokenBFIndex()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var createIndexOperation = new CreateIndexOperation
        {
            Name = "IX_ErrorMessage",
            Table = "TestTable",
            Columns = ["ErrorMessage"],
            IsUnique = false
        };
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.TokenBF);
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity, 4);
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexParams, SkipIndexParams.ForTokenBF(10240, 3, 0));

        var commands = generator.Generate(new[] { createIndexOperation });
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("ADD INDEX", sql);
        Assert.Contains("\"IX_ErrorMessage\"", sql);
        Assert.Contains("TYPE tokenbf_v1(10240, 3, 0)", sql);
        Assert.Contains("GRANULARITY 4", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesNgramBFIndex()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var createIndexOperation = new CreateIndexOperation
        {
            Name = "IX_Description",
            Table = "TestTable",
            Columns = ["Description"],
            IsUnique = false
        };
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.NgramBF);
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity, 5);
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexParams, SkipIndexParams.ForNgramBF(4, 10240, 3, 0));

        var commands = generator.Generate(new[] { createIndexOperation });
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("ADD INDEX", sql);
        Assert.Contains("\"IX_Description\"", sql);
        Assert.Contains("TYPE ngrambf_v1(4, 10240, 3, 0)", sql);
        Assert.Contains("GRANULARITY 5", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesSetIndex()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var createIndexOperation = new CreateIndexOperation
        {
            Name = "IX_Status",
            Table = "TestTable",
            Columns = ["Status"],
            IsUnique = false
        };
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.Set);
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity, 2);
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexParams, SkipIndexParams.ForSet(100));

        var commands = generator.Generate(new[] { createIndexOperation });
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("ADD INDEX", sql);
        Assert.Contains("\"IX_Status\"", sql);
        Assert.Contains("TYPE set(100)", sql);
        Assert.Contains("GRANULARITY 2", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesMinmaxWithCustomGranularity()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var createIndexOperation = new CreateIndexOperation
        {
            Name = "IX_CreatedAt",
            Table = "TestTable",
            Columns = ["CreatedAt"],
            IsUnique = false
        };
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexType, SkipIndexType.Minmax);
        createIndexOperation.AddAnnotation(ClickHouseAnnotationNames.SkipIndexGranularity, 5);

        var commands = generator.Generate(new[] { createIndexOperation });
        var sql = commands.First().CommandText;

        Assert.Contains("ALTER TABLE", sql);
        Assert.Contains("ADD INDEX", sql);
        Assert.Contains("\"IX_CreatedAt\"", sql);
        Assert.Contains("TYPE minmax", sql);
        Assert.Contains("GRANULARITY 5", sql);
    }

    #endregion

    private MigrationDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<MigrationDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new MigrationDbContext(options);
    }
}

public class MigrationDbContext : DbContext
{
    public MigrationDbContext(DbContextOptions<MigrationDbContext> options) : base(options) { }
}
