using EF.CH.Migrations.Design;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Tests for the ClickHouseMigrationsSplitter class.
/// Verifies that migration operations are correctly split into individual steps
/// with proper topological ordering.
/// </summary>
public class MigrationSplitterTests
{
    private readonly ClickHouseMigrationsSplitter _splitter = new();

    [Fact]
    public void Split_EmptyOperations_ReturnsEmpty()
    {
        var operations = Array.Empty<MigrationOperation>();

        var steps = _splitter.Split(operations);

        Assert.Empty(steps);
    }

    [Fact]
    public void Split_SingleOperation_ReturnsSingleStep()
    {
        var operations = new MigrationOperation[]
        {
            new CreateTableOperation { Name = "Orders" }
        };

        var steps = _splitter.Split(operations);

        Assert.Single(steps);
        Assert.Equal(1, steps[0].StepNumber);
        Assert.Equal("001", steps[0].StepSuffix);
        Assert.Equal("CreateTable_Orders", steps[0].OperationDescription);
    }

    [Fact]
    public void Split_MultipleOperations_CreatesMultipleSteps()
    {
        var operations = new MigrationOperation[]
        {
            new CreateTableOperation { Name = "Orders" },
            new CreateTableOperation { Name = "Customers" },
            new CreateTableOperation { Name = "Products" }
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        Assert.Equal("001", steps[0].StepSuffix);
        Assert.Equal("002", steps[1].StepSuffix);
        Assert.Equal("003", steps[2].StepSuffix);
    }

    [Fact]
    public void Split_DependentOperations_OrdersTableBeforeIndex()
    {
        // Index depends on table - should be ordered correctly
        var operations = new MigrationOperation[]
        {
            new CreateIndexOperation { Name = "IX_Orders_Date", Table = "Orders" },
            new CreateTableOperation { Name = "Orders" }
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        // CreateTable should come first
        Assert.IsType<CreateTableOperation>(steps[0].Operation);
        Assert.IsType<CreateIndexOperation>(steps[1].Operation);
    }

    [Fact]
    public void Split_ProjectionDependsOnTable_OrdersCorrectly()
    {
        var operations = new MigrationOperation[]
        {
            new AddProjectionOperation
            {
                Table = "Orders",
                Name = "prj_by_date",
                SelectSql = "SELECT * ORDER BY OrderDate"
            },
            new CreateTableOperation { Name = "Orders" }
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);
        Assert.IsType<AddProjectionOperation>(steps[1].Operation);
    }

    [Fact]
    public void Split_AddColumnDependsOnTable_OrdersCorrectly()
    {
        var operations = new MigrationOperation[]
        {
            new AddColumnOperation { Table = "Orders", Name = "Status" },
            new CreateTableOperation { Name = "Orders" }
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(2, steps.Count);
        Assert.IsType<CreateTableOperation>(steps[0].Operation);
        Assert.IsType<AddColumnOperation>(steps[1].Operation);
    }

    [Fact]
    public void Split_ComplexDependencyChain_OrdersCorrectly()
    {
        // Simulate: Table -> Index -> Projection -> MaterializeProjection
        var operations = new MigrationOperation[]
        {
            new MaterializeProjectionOperation { Table = "Orders", Name = "prj_daily" },
            new AddProjectionOperation { Table = "Orders", Name = "prj_daily", SelectSql = "SELECT ..." },
            new CreateIndexOperation { Table = "Orders", Name = "IX_Orders_Date" },
            new CreateTableOperation { Name = "Orders" }
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(4, steps.Count);
        // Table must come first
        Assert.IsType<CreateTableOperation>(steps[0].Operation);
        // Remaining operations depend on table
        var remainingOps = steps.Skip(1).Select(s => s.Operation.GetType()).ToList();
        Assert.Contains(typeof(CreateIndexOperation), remainingOps);
        Assert.Contains(typeof(AddProjectionOperation), remainingOps);
        Assert.Contains(typeof(MaterializeProjectionOperation), remainingOps);
    }

    [Fact]
    public void Split_IndependentOperations_PreservesOriginalOrder()
    {
        // Independent tables should maintain original order
        var operations = new MigrationOperation[]
        {
            new CreateTableOperation { Name = "Alpha" },
            new CreateTableOperation { Name = "Beta" },
            new CreateTableOperation { Name = "Gamma" }
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        Assert.Equal("CreateTable_Alpha", steps[0].OperationDescription);
        Assert.Equal("CreateTable_Beta", steps[1].OperationDescription);
        Assert.Equal("CreateTable_Gamma", steps[2].OperationDescription);
    }

    [Fact]
    public void Split_DropOperations_GeneratesCorrectDescriptions()
    {
        var operations = new MigrationOperation[]
        {
            new DropTableOperation { Name = "OldTable" },
            new DropColumnOperation { Table = "Orders", Name = "ObsoleteColumn" },
            new DropIndexOperation { Table = "Orders", Name = "IX_Old" }
        };

        var steps = _splitter.Split(operations);

        Assert.Equal(3, steps.Count);
        Assert.Equal("DropTable_OldTable", steps[0].OperationDescription);
        Assert.Equal("DropColumn_Orders_ObsoleteColumn", steps[1].OperationDescription);
        Assert.Equal("DropIndex_IX_Old", steps[2].OperationDescription);
    }

    [Fact]
    public void StepMigration_StepSuffix_FormatsCorrectly()
    {
        var step1 = new StepMigration(1, "Test", new CreateTableOperation { Name = "T" }, 0);
        var step10 = new StepMigration(10, "Test", new CreateTableOperation { Name = "T" }, 0);
        var step100 = new StepMigration(100, "Test", new CreateTableOperation { Name = "T" }, 0);

        Assert.Equal("001", step1.StepSuffix);
        Assert.Equal("010", step10.StepSuffix);
        Assert.Equal("100", step100.StepSuffix);
    }
}
