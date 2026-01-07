using EF.CH.Extensions;
using EF.CH.Infrastructure;
using EF.CH.Migrations.Internal;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Tests for idempotent DDL generation.
/// Verifies that migration SQL uses IF EXISTS/IF NOT EXISTS clauses
/// to enable safe retry after partial failure.
/// </summary>
public class IdempotentDdlTests
{
    [Fact]
    public void CreateTable_GeneratesIfNotExists()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new CreateTableOperation
        {
            Name = "TestTable",
            Columns =
            {
                new AddColumnOperation { Name = "Id", Table = "TestTable", ClrType = typeof(Guid) }
            }
        };

        var commands = generator.Generate([operation], context.Model);
        var sql = string.Join("", commands.Select(c => c.CommandText));

        Assert.Contains("CREATE TABLE IF NOT EXISTS", sql);
    }

    [Fact]
    public void AddColumn_GeneratesIfNotExists()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new AddColumnOperation
        {
            Table = "TestTable",
            Name = "NewColumn",
            ClrType = typeof(string),
            ColumnType = "String"
        };

        var commands = generator.Generate([operation], context.Model);
        var sql = string.Join("", commands.Select(c => c.CommandText));

        Assert.Contains("ADD COLUMN IF NOT EXISTS", sql);
    }

    [Fact]
    public void DropColumn_GeneratesIfExists()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new DropColumnOperation
        {
            Table = "TestTable",
            Name = "OldColumn"
        };

        var commands = generator.Generate([operation], context.Model);
        var sql = string.Join("", commands.Select(c => c.CommandText));

        Assert.Contains("DROP COLUMN IF EXISTS", sql);
    }

    [Fact]
    public void DropTable_GeneratesIfExists()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new DropTableOperation { Name = "TestTable" };

        var commands = generator.Generate([operation], context.Model);
        var sql = string.Join("", commands.Select(c => c.CommandText));

        Assert.Contains("DROP TABLE IF EXISTS", sql);
    }

    [Fact]
    public void CreateIndex_GeneratesIfNotExists()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new CreateIndexOperation
        {
            Table = "TestTable",
            Name = "IX_TestTable_Column",
            Columns = ["Column1"]
        };

        var commands = generator.Generate([operation], context.Model);
        var sql = string.Join("", commands.Select(c => c.CommandText));

        Assert.Contains("ADD INDEX IF NOT EXISTS", sql);
    }

    [Fact]
    public void DropIndex_GeneratesIfExists()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new DropIndexOperation
        {
            Table = "TestTable",
            Name = "IX_TestTable_Column"
        };

        var commands = generator.Generate([operation], context.Model);
        var sql = string.Join("", commands.Select(c => c.CommandText));

        Assert.Contains("DROP INDEX IF EXISTS", sql);
    }

    [Fact]
    public void AddProjection_GeneratesIfNotExists()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new AddProjectionOperation
        {
            Table = "TestTable",
            Name = "prj_test",
            SelectSql = "SELECT * ORDER BY Id"
        };

        var commands = generator.Generate([operation], context.Model);
        var sql = string.Join("", commands.Select(c => c.CommandText));

        Assert.Contains("ADD PROJECTION IF NOT EXISTS", sql);
    }

    [Fact]
    public void DropProjection_GeneratesIfExists()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new DropProjectionOperation
        {
            Table = "TestTable",
            Name = "prj_test"
        };

        var commands = generator.Generate([operation], context.Model);
        var sql = string.Join("", commands.Select(c => c.CommandText));

        Assert.Contains("DROP PROJECTION IF EXISTS", sql);
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return new TestDbContext(options);
    }

    private class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions options) : base(options) { }
    }
}
