using EF.CH.Metadata;
using EF.CH.Migrations.Design;
using EF.CH.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Unit tests for ClickHouseMigrationsSplitter.
/// </summary>
public class MigrationSplitterTests
{
    private readonly ClickHouseMigrationsSplitter _splitter = new();

    [Fact]
    public void Split_EmptyOperations_ReturnsEmpty()
    {
        var result = _splitter.Split([]);

        Assert.Empty(result);
    }

    [Fact]
    public void Split_SingleOperation_ReturnsSingleStep()
    {
        var operations = new List<MigrationOperation>
        {
            new CreateTableOperation { Name = "Orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Single(result);
        Assert.Equal(1, result[0].StepNumber);
        Assert.Equal("001", result[0].StepSuffix);
        Assert.Equal("CreateTable_Orders", result[0].OperationDescription);
    }

    [Fact]
    public void Split_MultipleOperations_CreatesMultipleSteps()
    {
        var operations = new List<MigrationOperation>
        {
            new CreateTableOperation { Name = "Orders" },
            new CreateTableOperation { Name = "Products" },
            new CreateTableOperation { Name = "Customers" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(3, result.Count);
        Assert.Equal(1, result[0].StepNumber);
        Assert.Equal(2, result[1].StepNumber);
        Assert.Equal(3, result[2].StepNumber);
    }

    [Fact]
    public void Split_DependentOperations_OrdersTableBeforeIndex()
    {
        var operations = new List<MigrationOperation>
        {
            new CreateIndexOperation { Name = "IX_Orders_Date", Table = "Orders" },
            new CreateTableOperation { Name = "Orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        Assert.IsType<CreateTableOperation>(result[0].Operation);
        Assert.IsType<CreateIndexOperation>(result[1].Operation);
    }

    [Fact]
    public void Split_ProjectionDependsOnTable_OrdersCorrectly()
    {
        var operations = new List<MigrationOperation>
        {
            new AddProjectionOperation { Name = "prj_by_date", Table = "Orders" },
            new CreateTableOperation { Name = "Orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        Assert.IsType<CreateTableOperation>(result[0].Operation);
        Assert.IsType<AddProjectionOperation>(result[1].Operation);
    }

    [Fact]
    public void Split_AddColumnDependsOnTable_OrdersCorrectly()
    {
        var operations = new List<MigrationOperation>
        {
            new AddColumnOperation { Name = "NewColumn", Table = "Orders" },
            new CreateTableOperation { Name = "Orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        Assert.IsType<CreateTableOperation>(result[0].Operation);
        Assert.IsType<AddColumnOperation>(result[1].Operation);
    }

    [Fact]
    public void Split_ComplexDependencyChain_OrdersCorrectly()
    {
        // Operations in wrong order: projection, index, table
        var operations = new List<MigrationOperation>
        {
            new AddProjectionOperation { Name = "prj_daily", Table = "Sales" },
            new CreateIndexOperation { Name = "IX_Sales_Region", Table = "Sales" },
            new CreateTableOperation { Name = "Sales" }
        };

        var result = _splitter.Split(operations);

        // Table must come first
        Assert.Equal(3, result.Count);
        Assert.IsType<CreateTableOperation>(result[0].Operation);
        // Index and projection can come after table (order preserved among equals)
        var remainingTypes = result.Skip(1).Select(r => r.Operation.GetType()).ToList();
        Assert.Contains(typeof(CreateIndexOperation), remainingTypes);
        Assert.Contains(typeof(AddProjectionOperation), remainingTypes);
    }

    [Fact]
    public void Split_IndependentOperations_PreservesOriginalOrder()
    {
        // Independent tables - should maintain original order
        var operations = new List<MigrationOperation>
        {
            new CreateTableOperation { Name = "Customers" },
            new CreateTableOperation { Name = "Products" },
            new CreateTableOperation { Name = "Categories" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(3, result.Count);
        Assert.Equal("CreateTable_Customers", result[0].OperationDescription);
        Assert.Equal("CreateTable_Products", result[1].OperationDescription);
        Assert.Equal("CreateTable_Categories", result[2].OperationDescription);
    }

    [Fact]
    public void Split_DropOperations_GeneratesCorrectDescriptions()
    {
        var operations = new List<MigrationOperation>
        {
            new DropTableOperation { Name = "OldTable" },
            new DropColumnOperation { Name = "OldColumn", Table = "TestTable" },
            new DropIndexOperation { Name = "IX_Old", Table = "TestTable" },
            new DropProjectionOperation { Name = "prj_old", Table = "TestTable" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(4, result.Count);
        // Phase 1: projections/indexes dropped first
        Assert.Equal("DropIndex_IX_Old", result[0].OperationDescription);
        Assert.Equal("DropProjection_prj_old", result[1].OperationDescription);
        // Phase 2: tables
        Assert.Equal("DropTable_OldTable", result[2].OperationDescription);
        // Phase 6: other (columns)
        Assert.Equal("DropColumn_TestTable_OldColumn", result[3].OperationDescription);
    }

    [Fact]
    public void StepMigration_StepSuffix_FormatsCorrectly()
    {
        var step1 = new StepMigration(1, "Test", new SqlOperation(), 0);
        var step9 = new StepMigration(9, "Test", new SqlOperation(), 0);
        var step10 = new StepMigration(10, "Test", new SqlOperation(), 0);
        var step100 = new StepMigration(100, "Test", new SqlOperation(), 0);

        Assert.Equal("001", step1.StepSuffix);
        Assert.Equal("009", step9.StepSuffix);
        Assert.Equal("010", step10.StepSuffix);
        Assert.Equal("100", step100.StepSuffix);
    }

    [Fact]
    public void Split_MaterializedViewDependsOnSourceTable_OrdersCorrectly()
    {
        var createMvOp = new CreateTableOperation { Name = "HourlySummary" };
        createMvOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        createMvOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "Orders");

        var operations = new List<MigrationOperation>
        {
            createMvOp,
            new CreateTableOperation { Name = "Orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        Assert.Equal("CreateTable_Orders", result[0].OperationDescription);
        Assert.Equal("CreateTable_HourlySummary", result[1].OperationDescription);
    }

    [Fact]
    public void Split_OriginalIndex_IsPreserved()
    {
        var operations = new List<MigrationOperation>
        {
            new CreateTableOperation { Name = "A" },
            new CreateTableOperation { Name = "B" },
            new CreateTableOperation { Name = "C" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(0, result[0].OriginalIndex);
        Assert.Equal(1, result[1].OriginalIndex);
        Assert.Equal(2, result[2].OriginalIndex);
    }

    #region Phase-Based Ordering Tests

    [Fact]
    public void Split_AllTablesBeforeMaterializedViews()
    {
        // Even without source annotation, MVs should come after all regular tables
        var mv1 = new CreateTableOperation { Name = "MV1" };
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var mv2 = new CreateTableOperation { Name = "MV2" };
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var operations = new List<MigrationOperation>
        {
            mv1,
            new CreateTableOperation { Name = "Table1" },
            mv2,
            new CreateTableOperation { Name = "Table2" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(4, result.Count);
        // Phase 3: Regular tables first
        Assert.Equal("CreateTable_Table1", result[0].OperationDescription);
        Assert.Equal("CreateTable_Table2", result[1].OperationDescription);
        // Phase 4: MVs after
        Assert.Equal("CreateTable_MV1", result[2].OperationDescription);
        Assert.Equal("CreateTable_MV2", result[3].OperationDescription);
    }

    [Fact]
    public void Split_AllTablesBeforeDictionaries()
    {
        var dict = new CreateTableOperation { Name = "CountryDict" };
        dict.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);

        var operations = new List<MigrationOperation>
        {
            dict,
            new CreateTableOperation { Name = "Countries" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        Assert.Equal("CreateTable_Countries", result[0].OperationDescription);
        Assert.Equal("CreateTable_CountryDict", result[1].OperationDescription);
    }

    [Fact]
    public void Split_AllTablesBeforeProjections()
    {
        var operations = new List<MigrationOperation>
        {
            new AddProjectionOperation { Name = "prj1", Table = "Orders" },
            new CreateTableOperation { Name = "Orders" },
            new AddProjectionOperation { Name = "prj2", Table = "Products" },
            new CreateTableOperation { Name = "Products" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(4, result.Count);
        // Phase 3: Tables first
        Assert.Equal("CreateTable_Orders", result[0].OperationDescription);
        Assert.Equal("CreateTable_Products", result[1].OperationDescription);
        // Phase 5: Projections after
        Assert.Equal("AddProjection_prj1", result[2].OperationDescription);
        Assert.Equal("AddProjection_prj2", result[3].OperationDescription);
    }

    [Fact]
    public void Split_DropsBeforeCreates()
    {
        var operations = new List<MigrationOperation>
        {
            new CreateTableOperation { Name = "NewTable" },
            new DropTableOperation { Name = "OldTable" },
            new CreateIndexOperation { Name = "IX_New", Table = "NewTable" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(3, result.Count);
        // Phase 2: Drops first
        Assert.Equal("DropTable_OldTable", result[0].OperationDescription);
        // Phase 3: Creates
        Assert.Equal("CreateTable_NewTable", result[1].OperationDescription);
        // Phase 5: Indexes
        Assert.Equal("CreateIndex_IX_New", result[2].OperationDescription);
    }

    [Fact]
    public void Split_ProjectionDropsBeforeTableDrops()
    {
        var operations = new List<MigrationOperation>
        {
            new DropTableOperation { Name = "Orders" },
            new DropProjectionOperation { Name = "prj_by_date", Table = "Orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 1: Projection drops first
        Assert.Equal("DropProjection_prj_by_date", result[0].OperationDescription);
        // Phase 2: Table drops after
        Assert.Equal("DropTable_Orders", result[1].OperationDescription);
    }

    [Fact]
    public void Split_FullMigrationScenario_OrdersCorrectly()
    {
        // Simulate a migration that drops old stuff and creates new stuff
        var newMv = new CreateTableOperation { Name = "NewSummary" };
        newMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var operations = new List<MigrationOperation>
        {
            // Drops (in wrong order)
            new DropTableOperation { Name = "OldTable" },
            new DropProjectionOperation { Name = "old_prj", Table = "OldTable" },
            new DropIndexOperation { Name = "old_idx", Table = "OldTable" },
            // Creates (in wrong order)
            new AddProjectionOperation { Name = "new_prj", Table = "NewTable" },
            newMv,
            new CreateTableOperation { Name = "NewTable" },
            new CreateIndexOperation { Name = "new_idx", Table = "NewTable" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(7, result.Count);

        // Phase 1: Drop projections/indexes
        Assert.IsType<DropProjectionOperation>(result[0].Operation);
        Assert.IsType<DropIndexOperation>(result[1].Operation);

        // Phase 2: Drop tables
        Assert.IsType<DropTableOperation>(result[2].Operation);

        // Phase 3: Create regular tables
        Assert.Equal("CreateTable_NewTable", result[3].OperationDescription);

        // Phase 4: Create MVs
        Assert.Equal("CreateTable_NewSummary", result[4].OperationDescription);

        // Phase 5: Add projections/indexes
        Assert.IsType<AddProjectionOperation>(result[5].Operation);
        Assert.IsType<CreateIndexOperation>(result[6].Operation);
    }

    #endregion
}
