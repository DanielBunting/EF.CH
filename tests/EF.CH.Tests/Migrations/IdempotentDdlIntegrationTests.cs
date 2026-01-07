using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Integration tests verifying idempotent DDL can be executed multiple times safely.
/// </summary>
public class IdempotentDdlIntegrationTests : IAsyncLifetime
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
    public async Task CreateTableIfNotExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        var createSql = @"CREATE TABLE IF NOT EXISTS ""test_idempotent_table"" (
            ""Id"" UUID,
            ""Name"" String
        ) ENGINE = MergeTree() ORDER BY (""Id"");";

        // First run
        await context.Database.ExecuteSqlRawAsync(createSql);

        // Second run - should not error
        await context.Database.ExecuteSqlRawAsync(createSql);

        // Verify table exists
        var count = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.tables WHERE name = 'test_idempotent_table'")
            .CountAsync();

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task DropTableIfExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        // Create table first
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE ""test_drop_table"" (""Id"" UUID) ENGINE = MergeTree() ORDER BY (""Id"");");

        // First drop
        await context.Database.ExecuteSqlRawAsync(@"DROP TABLE IF EXISTS ""test_drop_table"";");

        // Second drop - should not error
        await context.Database.ExecuteSqlRawAsync(@"DROP TABLE IF EXISTS ""test_drop_table"";");
    }

    [Fact]
    public async Task AddColumnIfNotExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        // Create table
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE ""test_add_column"" (""Id"" UUID) ENGINE = MergeTree() ORDER BY (""Id"");");

        // First add
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_add_column"" ADD COLUMN IF NOT EXISTS ""NewCol"" String;");

        // Second add - should not error
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_add_column"" ADD COLUMN IF NOT EXISTS ""NewCol"" String;");

        // Verify column exists
        var count = await context.Database.SqlQueryRaw<int>(
            @"SELECT 1 AS Value FROM system.columns WHERE table = 'test_add_column' AND name = 'NewCol'")
            .CountAsync();

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task DropColumnIfExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        // Create table with column
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE ""test_drop_column"" (""Id"" UUID, ""ToDelete"" String) ENGINE = MergeTree() ORDER BY (""Id"");");

        // First drop
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_drop_column"" DROP COLUMN IF EXISTS ""ToDelete"";");

        // Second drop - should not error
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_drop_column"" DROP COLUMN IF EXISTS ""ToDelete"";");
    }

    [Fact]
    public async Task AddIndexIfNotExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        // Create table
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE ""test_add_index"" (""Id"" UUID, ""Value"" Int32) ENGINE = MergeTree() ORDER BY (""Id"");");

        // First add
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_add_index"" ADD INDEX IF NOT EXISTS ""IX_Value"" (""Value"") TYPE minmax GRANULARITY 1;");

        // Second add - should not error
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_add_index"" ADD INDEX IF NOT EXISTS ""IX_Value"" (""Value"") TYPE minmax GRANULARITY 1;");
    }

    [Fact]
    public async Task DropIndexIfExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        // Create table with index
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE ""test_drop_index"" (""Id"" UUID, ""Value"" Int32) ENGINE = MergeTree() ORDER BY (""Id"");");
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_drop_index"" ADD INDEX ""IX_ToDelete"" (""Value"") TYPE minmax GRANULARITY 1;");

        // First drop
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_drop_index"" DROP INDEX IF EXISTS ""IX_ToDelete"";");

        // Second drop - should not error
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_drop_index"" DROP INDEX IF EXISTS ""IX_ToDelete"";");
    }

    [Fact]
    public async Task AddProjectionIfNotExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        // Create table
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE ""test_add_projection"" (""Id"" UUID, ""Date"" Date) ENGINE = MergeTree() ORDER BY (""Id"");");

        // First add
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_add_projection"" ADD PROJECTION IF NOT EXISTS ""prj_by_date"" (SELECT * ORDER BY ""Date"");");

        // Second add - should not error
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_add_projection"" ADD PROJECTION IF NOT EXISTS ""prj_by_date"" (SELECT * ORDER BY ""Date"");");
    }

    [Fact]
    public async Task DropProjectionIfExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        // Create table with projection
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE ""test_drop_projection"" (""Id"" UUID, ""Date"" Date) ENGINE = MergeTree() ORDER BY (""Id"");");
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_drop_projection"" ADD PROJECTION ""prj_to_delete"" (SELECT * ORDER BY ""Date"");");

        // First drop
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_drop_projection"" DROP PROJECTION IF EXISTS ""prj_to_delete"";");

        // Second drop - should not error
        await context.Database.ExecuteSqlRawAsync(
            @"ALTER TABLE ""test_drop_projection"" DROP PROJECTION IF EXISTS ""prj_to_delete"";");
    }

    [Fact]
    public async Task CreateMaterializedViewIfNotExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        // Create source table first
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""mv_source"" (""Id"" UUID, ""Value"" Int32) ENGINE = MergeTree() ORDER BY (""Id"");");

        // Create MV
        var mvSql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS ""test_mv""
            ENGINE = SummingMergeTree() ORDER BY ()
            AS SELECT sum(""Value"") AS ""TotalValue"" FROM ""mv_source"";";

        // First run
        await context.Database.ExecuteSqlRawAsync(mvSql);

        // Second run - should not error
        await context.Database.ExecuteSqlRawAsync(mvSql);
    }

    [Fact]
    public async Task CreateDictionaryIfNotExists_RunsTwiceWithoutError()
    {
        await using var context = CreateContext();

        // Create source table first
        await context.Database.ExecuteSqlRawAsync(
            @"CREATE TABLE IF NOT EXISTS ""dict_source"" (""Id"" UInt64, ""Name"" String) ENGINE = MergeTree() ORDER BY (""Id"");");

        // Create Dictionary
        var dictSql = @"CREATE DICTIONARY IF NOT EXISTS ""test_dict""
            (""Id"" UInt64, ""Name"" String)
            PRIMARY KEY ""Id""
            SOURCE(CLICKHOUSE(TABLE 'dict_source'))
            LAYOUT(HASHED())
            LIFETIME(300);";

        // First run
        await context.Database.ExecuteSqlRawAsync(dictSql);

        // Second run - should not error
        await context.Database.ExecuteSqlRawAsync(dictSql);
    }

    [Fact]
    public async Task MigrationsSqlGenerator_GeneratesIdempotentCreateTable()
    {
        await using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new CreateTableOperation
        {
            Name = "IdempotentTest",
            Columns =
            {
                new AddColumnOperation { Name = "Id", Table = "IdempotentTest", ClrType = typeof(Guid), ColumnType = "UUID" }
            }
        };
        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree()");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        var commands = generator.Generate(new[] { operation });
        var sql = commands.First().CommandText;

        // Execute twice - should not error
        await context.Database.ExecuteSqlRawAsync(sql);
        await context.Database.ExecuteSqlRawAsync(sql);
    }

    private IdempotentIntegrationDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<IdempotentIntegrationDbContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new IdempotentIntegrationDbContext(options);
    }
}

public class IdempotentIntegrationDbContext : DbContext
{
    public IdempotentIntegrationDbContext(DbContextOptions<IdempotentIntegrationDbContext> options)
        : base(options)
    {
    }
}
