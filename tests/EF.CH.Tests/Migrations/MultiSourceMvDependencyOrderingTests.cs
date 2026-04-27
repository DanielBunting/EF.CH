using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Unit tests asserting that the migrations SQL generator's dependency
/// resolver places joined source tables before the materialized view in the
/// emitted operation order, even when the operation list arrives in a
/// dependency-violating sequence.
/// </summary>
public class MultiSourceMvDependencyOrderingTests
{
    [Fact]
    public void Generate_PutsJoinedSourcesBeforeMv_WhenAnnotationListed()
    {
        using var ctx = new MultiSourceMvOrderingContext();
        var generator = ctx.GetService<IMigrationsSqlGenerator>();
        var model = ctx.Model;

        // The MV operation arrives BEFORE its joined source tables on purpose —
        // the generator must reorder so source-table operations land first.
        var mvOp = new CreateTableOperation
        {
            Name = "RegionalRevenue",
            Columns =
            {
                new AddColumnOperation { Name = "Category", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Total", ClrType = typeof(long), ColumnType = "Int64" },
            },
        };
        mvOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "Order");
        mvOp.AddAnnotation(
            ClickHouseAnnotationNames.MaterializedViewJoinedSources,
            new[] { "Customer", "Product" });
        mvOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery,
            "SELECT '' AS Category, 0 AS Total");
        mvOp.AddAnnotation(ClickHouseAnnotationNames.Engine, "SummingMergeTree");
        mvOp.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Category" });

        var orderOp = new CreateTableOperation
        {
            Name = "Order",
            Columns = { new AddColumnOperation { Name = "Id", ClrType = typeof(int), ColumnType = "Int32" } },
        };
        orderOp.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        orderOp.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        var customerOp = new CreateTableOperation
        {
            Name = "Customer",
            Columns = { new AddColumnOperation { Name = "Id", ClrType = typeof(int), ColumnType = "Int32" } },
        };
        customerOp.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        customerOp.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        var productOp = new CreateTableOperation
        {
            Name = "Product",
            Columns = { new AddColumnOperation { Name = "Id", ClrType = typeof(int), ColumnType = "Int32" } },
        };
        productOp.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        productOp.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        // MV first, then sources — generator must reorder.
        var input = new MigrationOperation[] { mvOp, orderOp, customerOp, productOp };
        var commands = generator.Generate(input, model);

        // Find indices in the produced SQL command list.
        int IndexOf(string fragment)
            => commands.ToList().FindIndex(c => c.CommandText.Contains(fragment));

        var orderIdx = IndexOf("CREATE TABLE IF NOT EXISTS \"Order\"");
        var customerIdx = IndexOf("CREATE TABLE IF NOT EXISTS \"Customer\"");
        var productIdx = IndexOf("CREATE TABLE IF NOT EXISTS \"Product\"");
        var mvIdx = IndexOf("CREATE MATERIALIZED VIEW IF NOT EXISTS \"RegionalRevenue\"");

        Assert.True(orderIdx >= 0, "Order CREATE not emitted.");
        Assert.True(customerIdx >= 0, "Customer CREATE not emitted.");
        Assert.True(productIdx >= 0, "Product CREATE not emitted.");
        Assert.True(mvIdx >= 0, "MV CREATE not emitted.");
        Assert.True(orderIdx < mvIdx, "Trigger source must come before MV.");
        Assert.True(customerIdx < mvIdx, "Joined source 'Customer' must come before MV.");
        Assert.True(productIdx < mvIdx, "Joined source 'Product' must come before MV.");
    }

    private sealed class MultiSourceMvOrderingContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseClickHouse("Host=localhost;Port=9000;Database=default;");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // No real entities — the generator test drives via raw operations.
        }
    }
}
