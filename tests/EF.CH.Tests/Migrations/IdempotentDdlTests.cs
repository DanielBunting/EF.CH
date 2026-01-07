using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Unit tests to verify idempotent DDL generation (IF NOT EXISTS / IF EXISTS).
/// </summary>
public class IdempotentDdlTests
{
    private static IMigrationsSqlGenerator GetGenerator()
    {
        var options = new DbContextOptionsBuilder<IdempotentTestDbContext>()
            .UseClickHouse("Host=localhost;Port=9000;Database=test")
            .Options;

        using var context = new IdempotentTestDbContext(options);
        return context.GetService<IMigrationsSqlGenerator>();
    }

    [Fact]
    public void CreateTable_GeneratesIfNotExists()
    {
        var generator = GetGenerator();

        var operation = new CreateTableOperation
        {
            Name = "Orders",
            Columns =
            {
                new AddColumnOperation { Name = "Id", Table = "Orders", ClrType = typeof(Guid), ColumnType = "UUID" }
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = ["Id"] }
        };

        var commands = generator.Generate(new[] { operation });
        var sql = commands.First().CommandText;

        Assert.Contains("CREATE TABLE IF NOT EXISTS", sql);
    }

    [Fact]
    public void DropTable_GeneratesIfExists()
    {
        var generator = GetGenerator();

        var operation = new DropTableOperation { Name = "Orders" };

        var commands = generator.Generate(new[] { operation });
        var sql = commands.First().CommandText;

        Assert.Contains("DROP TABLE IF EXISTS", sql);
    }

    [Fact]
    public void AddColumn_GeneratesIfNotExists()
    {
        var generator = GetGenerator();

        var operation = new AddColumnOperation
        {
            Table = "Orders",
            Name = "NewColumn",
            ClrType = typeof(string),
            ColumnType = "String"
        };

        var commands = generator.Generate(new[] { operation });
        var sql = commands.First().CommandText;

        Assert.Contains("ADD COLUMN IF NOT EXISTS", sql);
    }

    [Fact]
    public void DropColumn_GeneratesIfExists()
    {
        var generator = GetGenerator();

        var operation = new DropColumnOperation
        {
            Table = "Orders",
            Name = "OldColumn"
        };

        var commands = generator.Generate(new[] { operation });
        var sql = commands.First().CommandText;

        Assert.Contains("DROP COLUMN IF EXISTS", sql);
    }

    [Fact]
    public void CreateIndex_GeneratesIfNotExists()
    {
        var generator = GetGenerator();

        var operation = new CreateIndexOperation
        {
            Name = "IX_Orders_Date",
            Table = "Orders",
            Columns = ["OrderDate"],
            IsUnique = false
        };

        var commands = generator.Generate(new[] { operation });
        var sql = commands.First().CommandText;

        Assert.Contains("ADD INDEX IF NOT EXISTS", sql);
    }

    [Fact]
    public void DropIndex_GeneratesIfExists()
    {
        var generator = GetGenerator();

        var operation = new DropIndexOperation
        {
            Name = "IX_Orders_Date",
            Table = "Orders"
        };

        var commands = generator.Generate(new[] { operation });
        var sql = commands.First().CommandText;

        Assert.Contains("DROP INDEX IF EXISTS", sql);
    }

    [Fact]
    public void AddProjection_GeneratesIfNotExists()
    {
        var generator = GetGenerator();

        var operation = new AddProjectionOperation
        {
            Name = "prj_by_date",
            Table = "Orders",
            SelectSql = "SELECT * ORDER BY \"OrderDate\""
        };

        var commands = generator.Generate(new[] { operation });
        var sql = commands.First().CommandText;

        Assert.Contains("ADD PROJECTION IF NOT EXISTS", sql);
    }

    [Fact]
    public void DropProjection_GeneratesIfExists()
    {
        var generator = GetGenerator();

        var operation = new DropProjectionOperation
        {
            Name = "prj_by_date",
            Table = "Orders",
            IfExists = true
        };

        var commands = generator.Generate(new[] { operation });
        var sql = commands.First().CommandText;

        Assert.Contains("DROP PROJECTION IF EXISTS", sql);
    }
}

public class IdempotentTestDbContext : DbContext
{
    public IdempotentTestDbContext(DbContextOptions<IdempotentTestDbContext> options)
        : base(options)
    {
    }
}
