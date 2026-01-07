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
        // Phase 3: tables
        Assert.Equal("DropTable_OldTable", result[2].OperationDescription);
        // Phase 7: modify columns (drop)
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

    #region Phase-Based Ordering Tests (9-Phase)

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
        // Phase 4: Regular tables first
        Assert.Equal("CreateTable_Table1", result[0].OperationDescription);
        Assert.Equal("CreateTable_Table2", result[1].OperationDescription);
        // Phase 6: MVs after
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
        // Phase 4: Tables first
        Assert.Equal("CreateTable_Orders", result[0].OperationDescription);
        Assert.Equal("CreateTable_Products", result[1].OperationDescription);
        // Phase 9: Projections after
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
        // Phase 3: Drops first
        Assert.Equal("DropTable_OldTable", result[0].OperationDescription);
        // Phase 4: Creates
        Assert.Equal("CreateTable_NewTable", result[1].OperationDescription);
        // Phase 8: Indexes
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

        // Phase 3: Drop tables
        Assert.IsType<DropTableOperation>(result[2].Operation);

        // Phase 4: Create regular tables
        Assert.Equal("CreateTable_NewTable", result[3].OperationDescription);

        // Phase 6: Create MVs
        Assert.Equal("CreateTable_NewSummary", result[4].OperationDescription);

        // Phase 8/9: Add indexes and projections
        Assert.IsType<CreateIndexOperation>(result[5].Operation);
        Assert.IsType<AddProjectionOperation>(result[6].Operation);
    }

    #endregion

    #region 9-Phase Specific Tests

    [Fact]
    public void Split_ColumnThenIndex_ColumnComesFirst()
    {
        // Column additions (Phase 5) must come before indexes (Phase 8)
        var operations = new List<MigrationOperation>
        {
            new CreateIndexOperation { Name = "IX_Test", Table = "orders" },
            new AddColumnOperation { Name = "NewColumn", Table = "orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 5: Add column first
        Assert.Equal("AddColumn_orders_NewColumn", result[0].OperationDescription);
        // Phase 8: Index after
        Assert.Equal("CreateIndex_IX_Test", result[1].OperationDescription);
    }

    [Fact]
    public void Split_DropMvBeforeSourceTable_MvDroppedFirst()
    {
        // V2 fix: MV drops (Phase 2) must come before regular table drops (Phase 3)
        var mvDrop = new DropTableOperation { Name = "daily_summary" };
        mvDrop.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvDrop.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "orders");

        var tableDrop = new DropTableOperation { Name = "orders" };

        // Wrong order in input
        var operations = new List<MigrationOperation> { tableDrop, mvDrop };
        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 2: MV dropped first
        Assert.Equal("DropTable_daily_summary", result[0].OperationDescription);
        // Phase 3: Source table dropped after
        Assert.Equal("DropTable_orders", result[1].OperationDescription);
    }

    [Fact]
    public void Split_DropDictionaryBeforeSourceTable_DictionaryDroppedFirst()
    {
        // V2 fix: Dictionary drops (Phase 2) must come before regular table drops (Phase 3)
        var dictDrop = new DropTableOperation { Name = "country_lookup" };
        dictDrop.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        dictDrop.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "countries");

        var tableDrop = new DropTableOperation { Name = "countries" };

        // Wrong order in input
        var operations = new List<MigrationOperation> { tableDrop, dictDrop };
        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 2: Dictionary dropped first
        Assert.Equal("DropTable_country_lookup", result[0].OperationDescription);
        // Phase 3: Source table dropped after
        Assert.Equal("DropTable_countries", result[1].OperationDescription);
    }

    [Fact]
    public void Split_CascadingMvs_OrderedByDependency()
    {
        // V2 feature: MV-B reads from MV-A → MV-A must be created first
        var mvA = new CreateTableOperation { Name = "mv_hourly" };
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "raw_events");

        var mvB = new CreateTableOperation { Name = "mv_daily" };
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "mv_hourly");

        // Wrong order in input
        var operations = new List<MigrationOperation> { mvB, mvA };
        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // MV-A (hourly) must come before MV-B (daily)
        Assert.Equal("CreateTable_mv_hourly", result[0].OperationDescription);
        Assert.Equal("CreateTable_mv_daily", result[1].OperationDescription);
    }

    [Fact]
    public void Split_CascadingMvDrops_DroppedInReverseOrder()
    {
        // V2 feature: When dropping cascading MVs, dependents must drop before sources
        var mvA = new DropTableOperation { Name = "mv_hourly" };
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        // mv_hourly is the source for mv_daily

        var mvB = new DropTableOperation { Name = "mv_daily" };
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "mv_hourly");

        // Wrong order in input (source before dependent)
        var operations = new List<MigrationOperation> { mvA, mvB };
        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Dependent (daily) must drop before source (hourly)
        Assert.Equal("DropTable_mv_daily", result[0].OperationDescription);
        Assert.Equal("DropTable_mv_hourly", result[1].OperationDescription);
    }

    [Fact]
    public void Split_IndexesAfterColumnsInSameTable()
    {
        // Ensure multiple column+index ops on same table are ordered correctly
        var operations = new List<MigrationOperation>
        {
            new CreateIndexOperation { Name = "IX_Col1", Table = "orders" },
            new CreateIndexOperation { Name = "IX_Col2", Table = "orders" },
            new AddColumnOperation { Name = "Col1", Table = "orders" },
            new AddColumnOperation { Name = "Col2", Table = "orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(4, result.Count);
        // Phase 5: All add columns first
        Assert.IsType<AddColumnOperation>(result[0].Operation);
        Assert.IsType<AddColumnOperation>(result[1].Operation);
        // Phase 8: All indexes after
        Assert.IsType<CreateIndexOperation>(result[2].Operation);
        Assert.IsType<CreateIndexOperation>(result[3].Operation);
    }

    [Fact]
    public void Split_9PhaseFullScenario()
    {
        // Complete 9-phase ordering test
        var mv = new CreateTableOperation { Name = "summary_mv" };
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var oldMv = new DropTableOperation { Name = "old_mv" };
        oldMv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var operations = new List<MigrationOperation>
        {
            // Phase 9: Projections (should be last)
            new AddProjectionOperation { Name = "prj_new", Table = "orders" },
            // Phase 8: Indexes
            new CreateIndexOperation { Name = "IX_new", Table = "orders" },
            // Phase 7: Modify columns (alter/drop/rename)
            new AlterColumnOperation { Name = "old_col", Table = "orders" },
            // Phase 6: Create MVs
            mv,
            // Phase 5: Add columns
            new AddColumnOperation { Name = "new_col", Table = "orders" },
            // Phase 4: Create tables
            new CreateTableOperation { Name = "orders" },
            // Phase 3: Drop tables
            new DropTableOperation { Name = "old_table" },
            // Phase 2: Drop MVs
            oldMv,
            // Phase 1: Drop projections/indexes
            new DropProjectionOperation { Name = "old_prj", Table = "old_table" },
            new DropIndexOperation { Name = "IX_old", Table = "old_table" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(10, result.Count);

        // Phase 1: Drop projections and indexes
        Assert.True(result[0].Operation is DropProjectionOperation or DropIndexOperation);
        Assert.True(result[1].Operation is DropProjectionOperation or DropIndexOperation);

        // Phase 2: Drop MVs
        Assert.IsType<DropTableOperation>(result[2].Operation);
        Assert.Equal("DropTable_old_mv", result[2].OperationDescription);

        // Phase 3: Drop regular tables
        Assert.IsType<DropTableOperation>(result[3].Operation);
        Assert.Equal("DropTable_old_table", result[3].OperationDescription);

        // Phase 4: Create regular tables
        Assert.Equal("CreateTable_orders", result[4].OperationDescription);

        // Phase 5: Add columns
        Assert.IsType<AddColumnOperation>(result[5].Operation);

        // Phase 6: Create MVs
        Assert.Equal("CreateTable_summary_mv", result[6].OperationDescription);

        // Phase 7: Modify columns (alter/drop/rename)
        Assert.IsType<AlterColumnOperation>(result[7].Operation);

        // Phase 8: Create indexes
        Assert.IsType<CreateIndexOperation>(result[8].Operation);

        // Phase 9: Add projections
        Assert.IsType<AddProjectionOperation>(result[9].Operation);
    }

    #endregion

    #region Edge Case Tests - MV + New Column Scenarios

    [Fact]
    public void Split_AddColumnThenMvUsingIt_ColumnBeforeMv()
    {
        // Critical fix: MV that references a new column must be created AFTER the column
        var mv = new CreateTableOperation { Name = "discount_summary" };
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "orders");
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery,
            "SELECT toDate(OrderDate) AS Date, SUM(Discount) AS TotalDiscount FROM orders GROUP BY Date");

        var operations = new List<MigrationOperation>
        {
            // MV first (wrong order) - references Discount column
            mv,
            // Column added second (wrong order)
            new AddColumnOperation { Name = "Discount", Table = "orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 5: Add column MUST come first
        Assert.IsType<AddColumnOperation>(result[0].Operation);
        Assert.Equal("AddColumn_orders_Discount", result[0].OperationDescription);
        // Phase 6: MV creation after column exists
        Assert.Equal("CreateTable_discount_summary", result[1].OperationDescription);
    }

    [Fact]
    public void Split_AddColumnThenDictionaryUsingIt_ColumnBeforeDict()
    {
        // Dictionary that sources from a table with new column
        var dict = new CreateTableOperation { Name = "product_lookup" };
        dict.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        dict.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "products");

        var operations = new List<MigrationOperation>
        {
            dict,
            new AddColumnOperation { Name = "NewAttribute", Table = "products" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 5: Add column first
        Assert.IsType<AddColumnOperation>(result[0].Operation);
        // Phase 6: Dictionary after
        Assert.IsType<CreateTableOperation>(result[1].Operation);
    }

    [Fact]
    public void Split_CreateTableWithMvAndIndex_AllOrderedCorrectly()
    {
        // New table + MV reading from it + index on MV
        var mv = new CreateTableOperation { Name = "events_summary" };
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "events");

        var operations = new List<MigrationOperation>
        {
            new CreateIndexOperation { Name = "IX_events_summary_date", Table = "events_summary" },
            mv,
            new CreateTableOperation { Name = "events" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(3, result.Count);
        // Phase 4: Create table
        Assert.Equal("CreateTable_events", result[0].OperationDescription);
        // Phase 6: Create MV
        Assert.Equal("CreateTable_events_summary", result[1].OperationDescription);
        // Phase 8: Create index
        Assert.Equal("CreateIndex_IX_events_summary_date", result[2].OperationDescription);
    }

    [Fact]
    public void Split_MultipleColumnsAndMultipleMvs_AllColumnsBeforeAllMvs()
    {
        // Multiple columns added, multiple MVs created
        var mv1 = new CreateTableOperation { Name = "mv1" };
        mv1.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var mv2 = new CreateTableOperation { Name = "mv2" };
        mv2.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var operations = new List<MigrationOperation>
        {
            mv1,
            new AddColumnOperation { Name = "Col1", Table = "orders" },
            mv2,
            new AddColumnOperation { Name = "Col2", Table = "orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(4, result.Count);
        // Phase 5: Both columns first
        Assert.IsType<AddColumnOperation>(result[0].Operation);
        Assert.IsType<AddColumnOperation>(result[1].Operation);
        // Phase 6: Both MVs after
        Assert.IsType<CreateTableOperation>(result[2].Operation);
        Assert.IsType<CreateTableOperation>(result[3].Operation);
    }

    #endregion

    #region Edge Case Tests - Rare Scenarios

    [Fact]
    public void Split_DropColumnAndMvThatUsedIt_MvDropsFirst()
    {
        // When dropping a column, any MV using it must drop first
        var mvDrop = new DropTableOperation { Name = "summary_mv" };
        mvDrop.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var operations = new List<MigrationOperation>
        {
            new DropColumnOperation { Name = "OldColumn", Table = "orders" },
            mvDrop
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 2: MV drop first
        Assert.Equal("DropTable_summary_mv", result[0].OperationDescription);
        // Phase 7: Column drop after
        Assert.Equal("DropColumn_orders_OldColumn", result[1].OperationDescription);
    }

    [Fact]
    public void Split_RenameColumnAfterMvCreation()
    {
        // Rename column should happen after MVs are created
        var mv = new CreateTableOperation { Name = "summary" };
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var operations = new List<MigrationOperation>
        {
            new RenameColumnOperation { Name = "OldName", Table = "orders", NewName = "NewName" },
            mv
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 6: MV first
        Assert.IsType<CreateTableOperation>(result[0].Operation);
        // Phase 7: Rename after
        Assert.IsType<RenameColumnOperation>(result[1].Operation);
    }

    [Fact]
    public void Split_AlterColumnAfterMvCreation()
    {
        // Alter column should happen after MVs are created
        var mv = new CreateTableOperation { Name = "summary" };
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

        var operations = new List<MigrationOperation>
        {
            new AlterColumnOperation { Name = "Amount", Table = "orders" },
            mv
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 6: MV first
        Assert.IsType<CreateTableOperation>(result[0].Operation);
        // Phase 7: Alter after
        Assert.IsType<AlterColumnOperation>(result[1].Operation);
    }

    [Fact]
    public void Split_AddColumnWithProjection_ColumnBeforeProjection()
    {
        // Projection referencing new column
        var operations = new List<MigrationOperation>
        {
            new AddProjectionOperation { Name = "prj_by_newcol", Table = "orders" },
            new AddColumnOperation { Name = "NewCol", Table = "orders" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 5: Add column first
        Assert.IsType<AddColumnOperation>(result[0].Operation);
        // Phase 9: Projection after
        Assert.IsType<AddProjectionOperation>(result[1].Operation);
    }

    [Fact]
    public void Split_ComplexScenario_TableMvColumnIndexProjection()
    {
        // Real-world scenario: add table, MV, column on existing table, index, projection
        var mv = new CreateTableOperation { Name = "new_summary" };
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mv.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "new_events");

        var operations = new List<MigrationOperation>
        {
            new AddProjectionOperation { Name = "prj", Table = "existing_table" },
            new CreateIndexOperation { Name = "IX_col", Table = "existing_table" },
            mv,
            new AddColumnOperation { Name = "NewCol", Table = "existing_table" },
            new CreateTableOperation { Name = "new_events" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(5, result.Count);

        // Phase 4: Create table
        Assert.Equal("CreateTable_new_events", result[0].OperationDescription);
        // Phase 5: Add column
        Assert.IsType<AddColumnOperation>(result[1].Operation);
        // Phase 6: Create MV
        Assert.Equal("CreateTable_new_summary", result[2].OperationDescription);
        // Phase 8: Create index
        Assert.IsType<CreateIndexOperation>(result[3].Operation);
        // Phase 9: Add projection
        Assert.IsType<AddProjectionOperation>(result[4].Operation);
    }

    [Fact]
    public void Split_SqlOperation_GoesToModifyColumnsPhase()
    {
        // Raw SQL operations should go to phase 7 (modify columns / other)
        var operations = new List<MigrationOperation>
        {
            new CreateTableOperation { Name = "test" },
            new SqlOperation { Sql = "ALTER TABLE test ADD COLUMN x Int32" }
        };

        var result = _splitter.Split(operations);

        Assert.Equal(2, result.Count);
        // Phase 4: Create table
        Assert.IsType<CreateTableOperation>(result[0].Operation);
        // Phase 7: SQL operation
        Assert.IsType<SqlOperation>(result[1].Operation);
    }

    [Fact]
    public void Split_ThreeLevelCascadingMvs_OrderedCorrectly()
    {
        // MV-C reads from MV-B reads from MV-A reads from raw_events
        var mvA = new CreateTableOperation { Name = "mv_minutely" };
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvA.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "raw_events");

        var mvB = new CreateTableOperation { Name = "mv_hourly" };
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvB.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "mv_minutely");

        var mvC = new CreateTableOperation { Name = "mv_daily" };
        mvC.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvC.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "mv_hourly");

        // Completely wrong order
        var operations = new List<MigrationOperation> { mvC, mvB, mvA };
        var result = _splitter.Split(operations);

        Assert.Equal(3, result.Count);
        // Topologically sorted: A -> B -> C
        Assert.Equal("CreateTable_mv_minutely", result[0].OperationDescription);
        Assert.Equal("CreateTable_mv_hourly", result[1].OperationDescription);
        Assert.Equal("CreateTable_mv_daily", result[2].OperationDescription);
    }

    #endregion
}
