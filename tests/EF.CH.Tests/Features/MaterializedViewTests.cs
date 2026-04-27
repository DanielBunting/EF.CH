using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

public class MaterializedViewTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
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

    #region DDL Generation Tests

    [Fact]
    public void MigrationsSqlGenerator_GeneratesCreateMaterializedView_RawSql()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var entityType = model.FindEntityType(typeof(MvDailySummary));
        Assert.NotNull(entityType);

        // Create a CreateTableOperation that represents the materialized view
        var createTableOp = new CreateTableOperation
        {
            Name = "DailySummary_MV",
            Columns =
            {
                new AddColumnOperation { Name = "Date", ClrType = typeof(DateTime), ColumnType = "Date" },
                new AddColumnOperation { Name = "ProductId", ClrType = typeof(int), ColumnType = "Int32" },
                new AddColumnOperation { Name = "TotalQuantity", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" },
                new AddColumnOperation { Name = "TotalRevenue", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" }
            }
        };

        // Add materialized view annotations
        createTableOp.AddAnnotation("ClickHouse:MaterializedView", true);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewSource", "Orders");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewQuery", @"
            SELECT
                toDate(OrderDate) AS Date,
                ProductId,
                sum(Quantity) AS TotalQuantity,
                sum(Revenue) AS TotalRevenue
            FROM Orders
            GROUP BY Date, ProductId");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewPopulate", false);
        createTableOp.AddAnnotation("ClickHouse:Engine", "SummingMergeTree");
        createTableOp.AddAnnotation("ClickHouse:OrderBy", new[] { "Date", "ProductId" });

        var commands = generator.Generate(new[] { createTableOp }, model);
        var sql = commands.First().CommandText;

        // Verify CREATE MATERIALIZED VIEW syntax
        Assert.Contains("CREATE MATERIALIZED VIEW", sql);
        Assert.Contains("\"DailySummary_MV\"", sql);
        Assert.Contains("ENGINE = SummingMergeTree()", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("AS", sql);
        Assert.Contains("SELECT", sql);
        Assert.Contains("toDate(OrderDate)", sql);
        Assert.Contains("sum(Quantity)", sql);
        Assert.DoesNotContain("POPULATE", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesCreateMaterializedView_WithPopulate()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var createTableOp = new CreateTableOperation
        {
            Name = "DailySummary_MV",
            Columns =
            {
                new AddColumnOperation { Name = "Date", ClrType = typeof(DateTime), ColumnType = "Date" },
                new AddColumnOperation { Name = "ProductId", ClrType = typeof(int), ColumnType = "Int32" },
                new AddColumnOperation { Name = "TotalQuantity", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" }
            }
        };

        createTableOp.AddAnnotation("ClickHouse:MaterializedView", true);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewSource", "Orders");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewQuery", "SELECT toDate(OrderDate) AS Date, ProductId, sum(Quantity) AS TotalQuantity FROM Orders GROUP BY Date, ProductId");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewPopulate", true);
        createTableOp.AddAnnotation("ClickHouse:Engine", "SummingMergeTree");
        createTableOp.AddAnnotation("ClickHouse:OrderBy", new[] { "Date", "ProductId" });

        var commands = generator.Generate(new[] { createTableOp }, model);
        var sql = commands.First().CommandText;

        Assert.Contains("POPULATE", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_ThrowsWhenNoQueryDefined()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var createTableOp = new CreateTableOperation
        {
            Name = "BadView",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(int), ColumnType = "Int32" }
            }
        };

        createTableOp.AddAnnotation("ClickHouse:MaterializedView", true);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewSource", "SomeTable");
        // No query annotation

        var ex = Assert.Throws<InvalidOperationException>(
            () => generator.Generate(new[] { createTableOp }, model));

        Assert.Contains("must have a view query defined", ex.Message);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesSimpleProjectionMV()
    {
        using var context = CreateContext<SimpleProjectionMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var entityType = model.FindEntityType(typeof(MvProcessedEvent));
        Assert.NotNull(entityType);

        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;

        var createTableOp = new CreateTableOperation
        {
            Name = "ProcessedEvents_MV",
            Columns =
            {
                new AddColumnOperation { Name = "EventNameId", ClrType = typeof(ulong), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "EventTime", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "Version", ClrType = typeof(long), ColumnType = "Int64" },
                new AddColumnOperation { Name = "Value", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" },
                new AddColumnOperation { Name = "IsActive", ClrType = typeof(byte), ColumnType = "UInt8" }
            }
        };

        createTableOp.AddAnnotation("ClickHouse:MaterializedView", true);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewSource", "RawEvents");
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewQuery", query);
        createTableOp.AddAnnotation("ClickHouse:MaterializedViewPopulate", false);
        createTableOp.AddAnnotation("ClickHouse:Engine", "ReplacingMergeTree");
        createTableOp.AddAnnotation("ClickHouse:OrderBy", new[] { "EventNameId", "EventTime" });
        createTableOp.AddAnnotation("ClickHouse:VersionColumn", "Version");

        var commands = generator.Generate(new[] { createTableOp }, model);
        var sql = commands.First().CommandText;

        // Verify CREATE MATERIALIZED VIEW syntax
        Assert.Contains("CREATE MATERIALIZED VIEW", sql);
        Assert.Contains("\"ProcessedEvents_MV\"", sql);
        Assert.Contains("ENGINE = ReplacingMergeTree(\"Version\")", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("AS", sql);

        // Verify ClickHouse functions are in the query
        Assert.Contains("cityHash64", sql);
        Assert.Contains("toUnixTimestamp64Milli", sql);

        // Verify no GROUP BY (simple projection)
        Assert.DoesNotContain("GROUP BY", sql);
        Assert.DoesNotContain("POPULATE", sql);
    }

    #endregion

    #region Fluent API Tests

    [Fact]
    public void AsMaterializedViewRaw_SetsCorrectAnnotations()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var entityType = context.Model.FindEntityType(typeof(MvDailySummary));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation("ClickHouse:MaterializedView")?.Value);
        Assert.Equal("Orders", entityType.FindAnnotation("ClickHouse:MaterializedViewSource")?.Value);
        Assert.NotNull(entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value);
        Assert.False((bool?)entityType.FindAnnotation("ClickHouse:MaterializedViewPopulate")?.Value);
    }

    [Fact]
    public void AsMaterializedView_Linq_SetsCorrectAnnotations()
    {
        using var context = CreateContext<LinqMaterializedViewContext>();
        var entityType = context.Model.FindEntityType(typeof(MvHourlySummary));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation("ClickHouse:MaterializedView")?.Value);
        Assert.NotNull(entityType.FindAnnotation("ClickHouse:MaterializedViewSource")?.Value);
        Assert.False((bool?)entityType.FindAnnotation("ClickHouse:MaterializedViewPopulate")?.Value);

        // LINQ expression is translated to SQL at configuration time
        var queryAnnotation = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery");
        Assert.NotNull(queryAnnotation?.Value);

        var query = queryAnnotation.Value as string;
        Assert.NotNull(query);
        Assert.Contains("GROUP BY", query);  // Confirms LINQ GroupBy was translated
    }

    [Fact]
    public void AsMaterializedView_SimpleProjection_SetsCorrectAnnotations()
    {
        using var context = CreateContext<SimpleProjectionMaterializedViewContext>();
        var entityType = context.Model.FindEntityType(typeof(MvProcessedEvent));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation("ClickHouse:MaterializedView")?.Value);
        Assert.Equal("RawEvents", entityType.FindAnnotation("ClickHouse:MaterializedViewSource")?.Value);
        Assert.False((bool?)entityType.FindAnnotation("ClickHouse:MaterializedViewPopulate")?.Value);

        // Verify the SQL query was generated (not stored as expression)
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Simple projection should contain CityHash64 and toUnixTimestamp64Milli
        Assert.Contains("cityHash64", query);
        Assert.Contains("toUnixTimestamp64Milli", query);

        // Simple projection should NOT contain GROUP BY
        Assert.DoesNotContain("GROUP BY", query);
    }

    [Fact]
    public void AsMaterializedView_AllAggregates_TranslatesWithColumnReferences()
    {
        using var context = CreateContext<AllAggregatesContext>();
        var entityType = context.Model.FindEntityType(typeof(MvAllAggregatesSummary));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Verify all aggregates include column references
        Assert.Contains("sum(\"Quantity\")", query);
        Assert.Contains("avg(\"Revenue\")", query);
        Assert.Contains("min(\"Revenue\")", query);
        Assert.Contains("max(\"Revenue\")", query);
        Assert.Contains("count()", query);

        // Verify GROUP BY
        Assert.Contains("GROUP BY", query);
        Assert.Contains("\"ProductId\"", query);
    }

    [Fact]
    public void AsMaterializedView_SingleKey_TranslatesDirectKeyAccess()
    {
        using var context = CreateContext<SingleKeyGroupByContext>();
        var entityType = context.Model.FindEntityType(typeof(MvDailyStats));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Verify g.Key is translated to the GROUP BY expression
        Assert.Contains("toDate(\"OrderDate\") AS \"Date\"", query);

        // Verify GROUP BY contains the expression
        Assert.Contains("GROUP BY toDate(\"OrderDate\")", query);

        // Verify aggregates
        Assert.Contains("count()", query);
        Assert.Contains("sum(\"Revenue\")", query);
    }

    [Fact]
    public void AsMaterializedView_CountIf_TranslatesConditionalCount()
    {
        using var context = CreateContext<CountIfContext>();
        var entityType = context.Model.FindEntityType(typeof(MvCountIfSummary));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Verify simple count
        Assert.Contains("count()", query);

        // Verify countIf with condition
        Assert.Contains("countIf", query);
        Assert.Contains("\"Revenue\"", query);
        Assert.Contains("100", query);
    }

    [Fact]
    public void AsMaterializedView_ClickHouseAggregates_TranslatesCorrectly()
    {
        using var context = CreateContext<ClickHouseAggregatesContext>();
        var entityType = context.Model.FindEntityType(typeof(MvClickHouseAggregatesSummary));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Verify uniq aggregate
        Assert.Contains("uniq(\"UserId\")", query);

        // Verify any aggregate
        Assert.Contains("any(\"UserId\")", query);

        // Verify GROUP BY with single string key
        Assert.Contains("GROUP BY", query);
    }

    [Fact]
    public void AsMaterializedView_ToStartOfHour_TranslatesDateTimeFunction()
    {
        using var context = CreateContext<DateTimeFunctionsContext>();
        var entityType = context.Model.FindEntityType(typeof(MvHourlyByStartOfHour));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Verify toStartOfHour is used
        Assert.Contains("toStartOfHour(\"OrderDate\")", query);

        // Verify GROUP BY uses the same expression
        Assert.Contains("GROUP BY toStartOfHour(\"OrderDate\")", query);
    }

    [Fact]
    public void AsMaterializedView_AnyJoin_EmitsAnyInnerJoin()
    {
        using var context = CreateContext<AnyInnerJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(JoinedRevenue));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("ANY INNER JOIN \"AnyJoinCustomers\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
    }

    [Fact]
    public void AsMaterializedView_AnyLeftJoin_EmitsAnyLeftJoin()
    {
        using var context = CreateContext<AnyLeftJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(JoinedRevenue));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("ANY LEFT JOIN \"AnyLeftJoinCustomers\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
    }

    [Fact]
    public void AsMaterializedView_AnyRightJoin_EmitsAnyRightJoin()
    {
        using var context = CreateContext<AnyRightJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(JoinedRevenue));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("ANY RIGHT JOIN \"AnyRightJoinCustomers\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
    }

    [Fact]
    public void AsMaterializedView_RightJoin_EmitsRightJoin()
    {
        using var context = CreateContext<RightJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(JoinedRevenue));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("RIGHT JOIN \"RightJoinCustomers\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
    }

    [Fact]
    public void AsMaterializedView_FullOuterJoin_EmitsFullOuterJoin()
    {
        using var context = CreateContext<FullOuterJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(JoinedRevenue));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("FULL OUTER JOIN \"FullOuterCustomers\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
    }

    [Fact]
    public void AsMaterializedView_LeftSemiJoin_EmitsLeftSemiJoin()
    {
        using var context = CreateContext<LeftSemiJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(SingleSideOrder));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("LEFT SEMI JOIN \"LeftSemiCustomers\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
        Assert.Contains("t0.\"Id\"", query);
        // Inner side must NOT appear in the projection.
        Assert.DoesNotContain("t1.\"Region\"", query);
    }

    [Fact]
    public void AsMaterializedView_LeftAntiJoin_EmitsLeftAntiJoin()
    {
        using var context = CreateContext<LeftAntiJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(SingleSideOrder));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("LEFT ANTI JOIN \"LeftAntiCustomers\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
    }

    [Fact]
    public void AsMaterializedView_RightSemiJoin_EmitsRightSemiJoin()
    {
        using var context = CreateContext<RightSemiJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(SingleSideCustomer));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("RIGHT SEMI JOIN \"RightSemiCustomers\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
        // Result selector projects t1 columns (the preserved inner side).
        Assert.Contains("t1.\"Region\"", query);
    }

    [Fact]
    public void AsMaterializedView_RightAntiJoin_EmitsRightAntiJoin()
    {
        using var context = CreateContext<RightAntiJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(SingleSideCustomer));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("RIGHT ANTI JOIN \"RightAntiCustomers\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
    }

    [Fact]
    public void AsMaterializedView_ArrayJoin_EmitsArrayJoinClause()
    {
        using var context = CreateContext<ArrayJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(ArrayJoinedRow));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("FROM \"MvArrayJoinEvents\" AS t0", query);
        Assert.Contains("ARRAY JOIN t0.\"Tags\" AS \"tag\"", query);
        // Element ref in projection resolves to the bare alias.
        Assert.Contains("\"tag\" AS \"Tag\"", query);
    }

    [Fact]
    public void AsMaterializedView_LeftArrayJoin_EmitsLeftArrayJoinClause()
    {
        using var context = CreateContext<LeftArrayJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(ArrayJoinedRow));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("LEFT ARRAY JOIN t0.\"Tags\" AS \"tag\"", query);
    }

    [Fact]
    public void AsMaterializedView_CrossJoin_EmitsCrossJoinNoOnClause()
    {
        using var context = CreateContext<CrossJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(CrossJoinedRow));
        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);
        Assert.Contains("CROSS JOIN \"CrossJoinTags\" AS t1", query);
        // Verify there's no ON clause attached to the CROSS JOIN line.
        Assert.DoesNotContain("CROSS JOIN \"CrossJoinTags\" AS t1 ON", query);
    }

    [Fact]
    public void AsMaterializedView_LinqAsofJoin_EmitsAsofInnerJoinWithInequality()
    {
        using var context = CreateContext<LinqAsofInnerJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(AsofTradeWithQuote));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        Assert.Contains("FROM \"AsofTrades\" AS t0", query);
        Assert.Contains("ASOF INNER JOIN \"AsofQuotes\" AS t1 ON t0.\"Symbol\" = t1.\"Symbol\" AND t0.\"T\" >= t1.\"T\"", query);
        // Result projection columns under correct aliases
        Assert.Contains("t0.\"Id\"", query);
        Assert.Contains("t1.\"Price\"", query);
    }

    [Fact]
    public void AsMaterializedView_LinqAsofLeftJoin_EmitsAsofLeftJoinWithInequality()
    {
        using var context = CreateContext<LinqAsofLeftJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(AsofTradeWithQuote));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        Assert.Contains("FROM \"AsofLeftTrades\" AS t0", query);
        Assert.Contains("ASOF LEFT JOIN \"AsofLeftQuotes\" AS t1 ON t0.\"Symbol\" = t1.\"Symbol\" AND t0.\"T\" >= t1.\"T\"", query);
    }

    [Fact]
    public void AsMaterializedView_LinqJoin_EmitsInnerJoinAndAliasedColumns()
    {
        using var context = CreateContext<LinqInnerJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(JoinedRevenue));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Aliased FROM and INNER JOIN with ON predicate
        Assert.Contains("FROM \"JoinOrderSrc\" AS t0", query);
        Assert.Contains("INNER JOIN \"JoinCustomerSrc\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);

        // Aggregate over outer-source column resolves to the outer alias
        Assert.Contains("sum(t0.\"Amount\")", query);

        // GROUP BY references the inner-source alias (came from the join's result selector)
        Assert.Contains("GROUP BY t1.\"Region\"", query);
    }

    [Fact]
    public void AsMaterializedView_LinqJoin_OnDictionaryEntity_EmitsDictionaryTableFunction()
    {
        using var context = CreateContext<LinqJoinDictionaryContext>();
        var entityType = context.Model.FindEntityType(typeof(JoinedRevenue));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        // Dictionary-flagged entity emits dictionary('name') instead of a quoted table.
        Assert.Contains("INNER JOIN dictionary('CustomerLookup') AS t1", query);
        Assert.Contains("ON t0.\"CustomerId\" = t1.\"Id\"", query);
    }

    [Fact]
    public void AsMaterializedView_LinqGroupJoin_EmitsLeftJoinAndCoalesceFirstOrDefault()
    {
        using var context = CreateContext<LinqGroupJoinContext>();
        var entityType = context.Model.FindEntityType(typeof(GroupJoinedOrderRegion));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        Assert.Contains("FROM \"GjOrderSrc\" AS t0", query);
        Assert.Contains("LEFT JOIN \"GjCustomerSrc\" AS t1 ON t0.\"CustomerId\" = t1.\"Id\"", query);
        // FirstOrDefault() ?? "" should collapse to coalesce(alias.col, '').
        Assert.Contains("coalesce(t1.\"Region\", '')", query);
        // Outer-source columns flow through to the projection under the outer alias.
        Assert.Contains("t0.\"Id\"", query);
        Assert.Contains("t0.\"Amount\"", query);
    }

    [Fact]
    public void AsMaterializedView_MergeStateAggregates_TranslatesAllTenCombinators()
    {
        using var context = CreateContext<MergeStateAggregatesContext>();
        var entityType = context.Model.FindEntityType(typeof(MvMergeStateRollup));

        Assert.NotNull(entityType);
        var query = entityType.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(query);

        Assert.Contains("countMergeState(\"C\")", query);
        Assert.Contains("sumMergeState(\"S\")", query);
        Assert.Contains("avgMergeState(\"Av\")", query);
        Assert.Contains("minMergeState(\"Mn\")", query);
        Assert.Contains("maxMergeState(\"Mx\")", query);
        Assert.Contains("uniqMergeState(\"U\")", query);
        Assert.Contains("uniqExactMergeState(\"Ue\")", query);
        Assert.Contains("anyMergeState(\"An\")", query);
        Assert.Contains("anyLastMergeState(\"Al\")", query);
        Assert.Contains("quantileMergeState(0.5)(\"Q\")", query);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CreateMaterializedView_RawSql_ExecutesSuccessfully()
    {
        await using var context = CreateContext<RawSqlMaterializedViewContext>();

        // Create source table first
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS Orders (
                OrderId Int32,
                OrderDate DateTime64(3),
                ProductId Int32,
                Quantity Decimal(18, 4),
                Revenue Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (OrderDate, OrderId)
        ");

        // Create the materialized view
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE MATERIALIZED VIEW IF NOT EXISTS DailySummary_MV
            ENGINE = SummingMergeTree()
            ORDER BY (Date, ProductId)
            AS
            SELECT
                toDate(OrderDate) AS Date,
                ProductId,
                sum(Quantity) AS TotalQuantity,
                sum(Revenue) AS TotalRevenue
            FROM Orders
            GROUP BY Date, ProductId
        ");

        // Insert some data into source table
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO Orders (OrderId, OrderDate, ProductId, Quantity, Revenue) VALUES
            (1, '2024-01-15 10:00:00', 100, 5, 99.99),
            (2, '2024-01-15 11:00:00', 100, 3, 59.97),
            (3, '2024-01-15 12:00:00', 200, 2, 39.98)
        ");

        // Query the materialized view
        var results = await context.Database.SqlQueryRaw<MvDailySummaryResult>(
            "SELECT Date, ProductId, TotalQuantity, TotalRevenue FROM DailySummary_MV FINAL ORDER BY Date, ProductId"
        ).ToListAsync();

        Assert.Equal(2, results.Count);

        var product100 = results.First(r => r.ProductId == 100);
        Assert.Equal(8m, product100.TotalQuantity);
        Assert.Equal(159.96m, product100.TotalRevenue);
    }

    [Fact]
    public async Task CreateMaterializedView_SimpleProjection_ExecutesSuccessfully()
    {
        await using var context = CreateContext<SimpleProjectionMaterializedViewContext>();

        // Create source table first
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS RawEvents (
                EventName String,
                EventTime DateTime64(3),
                UserId String,
                Value Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY (EventName, EventTime)
        ");

        // Get the translated SQL query from the model
        var entityType = context.Model.FindEntityType(typeof(MvProcessedEvent));
        var selectSql = entityType?.FindAnnotation("ClickHouse:MaterializedViewQuery")?.Value as string;
        Assert.NotNull(selectSql);

        // Create the materialized view with simple projection
        await context.Database.ExecuteSqlRawAsync($@"
            CREATE MATERIALIZED VIEW IF NOT EXISTS ProcessedEvents_MV
            ENGINE = ReplacingMergeTree(""Version"")
            ORDER BY (""EventNameId"", ""EventTime"")
            AS
            {selectSql}
        ");

        // Insert some data into source table
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO RawEvents (EventName, EventTime, UserId, Value) VALUES
            ('UserLogin', '2024-01-15 10:00:00', 'user1', 1.0),
            ('UserLogin', '2024-01-15 10:05:00', 'user2', 1.0),
            ('Purchase', '2024-01-15 11:00:00', 'user1', 99.99)
        ");

        // Query the materialized view
        var results = await context.Database.SqlQueryRaw<MvProcessedEventResult>(
            "SELECT EventNameId, EventTime, Version, Value, IsActive FROM ProcessedEvents_MV ORDER BY EventTime"
        ).ToListAsync();

        Assert.Equal(3, results.Count);

        // Verify cityHash64 was applied (all EventNameId values should be non-zero)
        Assert.All(results, r => Assert.NotEqual(0UL, r.EventNameId));

        // Verify toUnixTimestamp64Milli was applied (Version > 0)
        Assert.All(results, r => Assert.True(r.Version > 0));

        // Verify constant IsActive = 1
        Assert.All(results, r => Assert.Equal((byte)1, r.IsActive));
    }

    #endregion

    #region Operation Ordering Tests

    [Fact]
    public void MigrationsSqlGenerator_OrdersSourceTablesBeforeMaterializedViews()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        // Create operations in wrong order: MV first, source table second
        var mvOp = new CreateTableOperation
        {
            Name = "DailySummary_MV",
            Columns =
            {
                new AddColumnOperation { Name = "Date", ClrType = typeof(DateTime), ColumnType = "Date" },
                new AddColumnOperation { Name = "ProductId", ClrType = typeof(int), ColumnType = "Int32" }
            }
        };
        mvOp.AddAnnotation("ClickHouse:MaterializedView", true);
        mvOp.AddAnnotation("ClickHouse:MaterializedViewSource", "Orders");
        mvOp.AddAnnotation("ClickHouse:MaterializedViewQuery", "SELECT toDate(OrderDate) AS Date, ProductId FROM Orders");
        mvOp.AddAnnotation("ClickHouse:Engine", "SummingMergeTree");
        mvOp.AddAnnotation("ClickHouse:OrderBy", new[] { "Date", "ProductId" });

        var tableOp = new CreateTableOperation
        {
            Name = "Orders",
            Columns =
            {
                new AddColumnOperation { Name = "OrderId", ClrType = typeof(int), ColumnType = "Int32" },
                new AddColumnOperation { Name = "OrderDate", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" },
                new AddColumnOperation { Name = "ProductId", ClrType = typeof(int), ColumnType = "Int32" }
            }
        };
        tableOp.AddAnnotation("ClickHouse:Engine", "MergeTree");
        tableOp.AddAnnotation("ClickHouse:OrderBy", new[] { "OrderDate", "OrderId" });

        // Pass operations in wrong order (MV first)
        var commands = generator.Generate(new MigrationOperation[] { mvOp, tableOp }, model);

        var allSql = string.Join("\n---\n", commands.Select(c => c.CommandText));

        // Find positions of CREATE statements
        var ordersIndex = allSql.IndexOf("CREATE TABLE", StringComparison.Ordinal);
        var mvIndex = allSql.IndexOf("CREATE MATERIALIZED VIEW", StringComparison.Ordinal);

        // Source table should be created before materialized view
        Assert.True(ordersIndex >= 0, "Orders table CREATE statement not found");
        Assert.True(mvIndex >= 0, "Materialized view CREATE statement not found");
        Assert.True(ordersIndex < mvIndex,
            $"Source table (index {ordersIndex}) should be created before materialized view (index {mvIndex})");
    }

    [Fact]
    public void MigrationsSqlGenerator_HandlesMultipleMVsFromSameSource()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        // Create source table operation
        var tableOp = new CreateTableOperation
        {
            Name = "Events",
            Columns =
            {
                new AddColumnOperation { Name = "EventId", ClrType = typeof(int), ColumnType = "Int32" },
                new AddColumnOperation { Name = "EventDate", ClrType = typeof(DateTime), ColumnType = "DateTime64(3)" }
            }
        };
        tableOp.AddAnnotation("ClickHouse:Engine", "MergeTree");
        tableOp.AddAnnotation("ClickHouse:OrderBy", new[] { "EventDate" });

        // Create two MVs that depend on the same source
        var mv1Op = CreateMvOperation("DailySummary_MV", "Events");
        var mv2Op = CreateMvOperation("HourlySummary_MV", "Events");

        // Pass in order: MV1, Source, MV2 (mixed)
        var commands = generator.Generate(new MigrationOperation[] { mv1Op, tableOp, mv2Op }, model);
        var allSql = string.Join("\n---\n", commands.Select(c => c.CommandText));

        var tableIndex = allSql.IndexOf("CREATE TABLE", StringComparison.Ordinal);
        var mv1Index = allSql.IndexOf("\"DailySummary_MV\"", StringComparison.Ordinal);
        var mv2Index = allSql.IndexOf("\"HourlySummary_MV\"", StringComparison.Ordinal);

        // Source table should appear before both MVs
        Assert.True(tableIndex < mv1Index, "Source table should be before first MV");
        Assert.True(tableIndex < mv2Index, "Source table should be before second MV");
    }

    [Fact]
    public void MigrationsSqlGenerator_PreservesOrderForIndependentTables()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        // Create three independent tables (no MV dependencies)
        var table1 = CreateTableOp("TableA");
        var table2 = CreateTableOp("TableB");
        var table3 = CreateTableOp("TableC");

        // Pass in specific order
        var commands = generator.Generate(new MigrationOperation[] { table1, table2, table3 }, model);
        var allSql = string.Join("\n---\n", commands.Select(c => c.CommandText));

        var indexA = allSql.IndexOf("\"TableA\"", StringComparison.Ordinal);
        var indexB = allSql.IndexOf("\"TableB\"", StringComparison.Ordinal);
        var indexC = allSql.IndexOf("\"TableC\"", StringComparison.Ordinal);

        // Should preserve original order for independent tables
        Assert.True(indexA < indexB, "TableA should come before TableB");
        Assert.True(indexB < indexC, "TableB should come before TableC");
    }

    [Fact]
    public void MigrationsSqlGenerator_HandlesMVDependingOnExternalTable()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        // Create MV that depends on a table NOT in the migration (already exists)
        var mvOp = CreateMvOperation("Summary_MV", "ExistingExternalTable");

        // Should not throw - MV is created even though source isn't in migration
        var commands = generator.Generate(new MigrationOperation[] { mvOp }, model);

        Assert.Single(commands);
        Assert.Contains("CREATE MATERIALIZED VIEW", commands.First().CommandText);
    }

    [Fact]
    public void MigrationsSqlGenerator_PreservesIndexPositionRelativeToTable()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        // Simulate a migration with: Table1, Index on Table1, MV depending on Table1, Table2
        var table1 = CreateTableOp("Table1");
        var index1 = new CreateIndexOperation
        {
            Name = "IX_Table1_Col",
            Table = "Table1",
            Columns = new[] { "Col" }
        };
        index1.AddAnnotation("ClickHouse:SkipIndexType", "minmax");
        index1.AddAnnotation("ClickHouse:SkipIndexGranularity", 1);

        var mvOp = CreateMvOperation("MV_Summary", "Table1");
        var table2 = CreateTableOp("Table2");

        // Pass in order: MV, Table1, Index1, Table2
        // Expected: Table1 moves before MV (dependency), Index stays after Table1, Table2 stays at end
        var commands = generator.Generate(new MigrationOperation[] { mvOp, table1, index1, table2 }, model);
        var allSql = string.Join("\n---\n", commands.Select(c => c.CommandText));

        // Debug: output the SQL to understand ordering
        // throw new Exception($"SQL:\n{allSql}");

        var table1Index = allSql.IndexOf("CREATE TABLE", StringComparison.Ordinal);
        var indexIndex = allSql.IndexOf("IX_Table1_Col", StringComparison.Ordinal);
        var mvIndex = allSql.IndexOf("CREATE MATERIALIZED VIEW", StringComparison.Ordinal);
        var table2Index = allSql.IndexOf("\"Table2\"", StringComparison.Ordinal);

        // Table1 should come before MV (dependency sorting)
        Assert.True(table1Index >= 0, $"Table1 CREATE not found. SQL:\n{allSql}");
        Assert.True(mvIndex >= 0, $"MV CREATE not found. SQL:\n{allSql}");
        Assert.True(table1Index < mvIndex, $"Table1 should come before MV. Table1={table1Index}, MV={mvIndex}. SQL:\n{allSql}");

        // Index should come after Table1
        Assert.True(indexIndex >= 0, $"Index not found. SQL:\n{allSql}");
        Assert.True(table1Index < indexIndex, $"Index should come after Table1. Table1={table1Index}, Index={indexIndex}");

        // Table2 should exist
        Assert.True(table2Index >= 0, $"Table2 not found. SQL:\n{allSql}");
    }

    [Fact]
    public void MigrationsSqlGenerator_PreservesDropTablePosition()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        // Simulate: DropTable, CreateTable MV, CreateTable Source
        var dropOp = new DropTableOperation { Name = "OldTable" };
        var mvOp = CreateMvOperation("MV_Summary", "SourceTable");
        var sourceOp = CreateTableOp("SourceTable");

        var commands = generator.Generate(new MigrationOperation[] { dropOp, mvOp, sourceOp }, model);
        var allSql = string.Join("\n---\n", commands.Select(c => c.CommandText));

        var dropIndex = allSql.IndexOf("DROP TABLE", StringComparison.Ordinal);
        var sourceIndex = allSql.IndexOf("\"SourceTable\"", StringComparison.Ordinal);
        var mvIndex = allSql.IndexOf("\"MV_Summary\"", StringComparison.Ordinal);

        // Drop should stay first (original position)
        Assert.True(dropIndex < sourceIndex, "Drop should stay before creates");
        Assert.True(dropIndex < mvIndex, "Drop should stay before MV");
        // Source should come before MV (dependency sorting)
        Assert.True(sourceIndex < mvIndex, "Source should come before MV");
    }

    [Fact]
    public void MigrationsSqlGenerator_OrdersAddProjectionAfterCreateTable()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        // Simulate: AddProjection before CreateTable (wrong order)
        var addProjOp = new EF.CH.Migrations.Operations.AddProjectionOperation
        {
            Table = "Orders",
            Name = "prj_by_date",
            SelectSql = "SELECT * ORDER BY OrderDate",
            Materialize = false
        };
        var tableOp = CreateTableOp("Orders");

        // Pass in wrong order: projection first, then table
        var commands = generator.Generate(new MigrationOperation[] { addProjOp, tableOp }, model);
        var allSql = string.Join("\n---\n", commands.Select(c => c.CommandText));

        var createIndex = allSql.IndexOf("CREATE TABLE", StringComparison.Ordinal);
        var projIndex = allSql.IndexOf("ADD PROJECTION", StringComparison.Ordinal);

        // Table should come before projection (dependency sorted)
        Assert.True(createIndex >= 0, $"CREATE TABLE not found. SQL:\n{allSql}");
        Assert.True(projIndex >= 0, $"ADD PROJECTION not found. SQL:\n{allSql}");
        Assert.True(createIndex < projIndex, $"CREATE TABLE should come before ADD PROJECTION. Create={createIndex}, Proj={projIndex}");
    }

    [Fact]
    public void MigrationsSqlGenerator_OrdersAddColumnAfterCreateTable()
    {
        using var context = CreateContext<RawSqlMaterializedViewContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        // Simulate: AddColumn before CreateTable (wrong order in manual migration)
        var addColOp = new AddColumnOperation
        {
            Table = "Orders",
            Name = "NewColumn",
            ClrType = typeof(string),
            ColumnType = "String"
        };
        var tableOp = CreateTableOp("Orders");

        // Pass in wrong order
        var commands = generator.Generate(new MigrationOperation[] { addColOp, tableOp }, model);
        var allSql = string.Join("\n---\n", commands.Select(c => c.CommandText));

        var createIndex = allSql.IndexOf("CREATE TABLE", StringComparison.Ordinal);
        var alterIndex = allSql.IndexOf("ALTER TABLE", StringComparison.Ordinal);

        // Table should come before alter (dependency sorted)
        Assert.True(createIndex >= 0, "CREATE TABLE not found");
        Assert.True(alterIndex >= 0, "ALTER TABLE not found");
        Assert.True(createIndex < alterIndex, "CREATE TABLE should come before ALTER TABLE");
    }

    [Fact]
    public async Task EnsureCreated_CreatesMaterializedViewsAfterSourceTables()
    {
        // This test verifies that EnsureCreated works correctly with materialized views
        // by relying on the operation ordering fix in the migration generator.
        await using var context = CreateContext<RawSqlMaterializedViewContext>();

        // Drop any existing tables/views first
        try
        {
            await context.Database.ExecuteSqlRawAsync("DROP VIEW IF EXISTS DailySummary_MV");
            await context.Database.ExecuteSqlRawAsync("DROP TABLE IF EXISTS Orders");
        }
        catch
        {
            // Ignore if they don't exist
        }

        // EnsureCreated should create both the source table and MV in correct order
        // Without the ordering fix, this would fail because MV is created before source table
        await context.Database.EnsureCreatedAsync();

        // Verify both exist by inserting data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO Orders (OrderId, OrderDate, ProductId, Quantity, Revenue) VALUES
            (1, '2024-01-15 10:00:00', 100, 5, 99.99)
        ");

        // Query the materialized view to verify it works
        var results = await context.Database.SqlQueryRaw<MvDailySummaryResult>(
            "SELECT Date, ProductId, TotalQuantity, TotalRevenue FROM DailySummary_MV FINAL"
        ).ToListAsync();

        // If the MV was created correctly after the source table, we should have data
        Assert.Single(results);
        Assert.Equal(100, results.First().ProductId);
    }

    private static CreateTableOperation CreateMvOperation(string name, string sourceTable)
    {
        var op = new CreateTableOperation
        {
            Name = name,
            Columns =
            {
                new AddColumnOperation { Name = "Date", ClrType = typeof(DateTime), ColumnType = "Date" }
            }
        };
        op.AddAnnotation("ClickHouse:MaterializedView", true);
        op.AddAnnotation("ClickHouse:MaterializedViewSource", sourceTable);
        op.AddAnnotation("ClickHouse:MaterializedViewQuery", $"SELECT toDate(EventDate) AS Date FROM {sourceTable}");
        op.AddAnnotation("ClickHouse:Engine", "SummingMergeTree");
        op.AddAnnotation("ClickHouse:OrderBy", new[] { "Date" });
        return op;
    }

    private static CreateTableOperation CreateTableOp(string name)
    {
        var op = new CreateTableOperation
        {
            Name = name,
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(int), ColumnType = "Int32" }
            }
        };
        op.AddAnnotation("ClickHouse:Engine", "MergeTree");
        op.AddAnnotation("ClickHouse:OrderBy", new[] { "Id" });
        return op;
    }

    #endregion

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}

#region Test Entities

public class MvOrder
{
    public int OrderId { get; set; }
    public DateTime OrderDate { get; set; }
    public int ProductId { get; set; }
    public decimal Quantity { get; set; }
    public decimal Revenue { get; set; }
}

// Entities for simple projection MV tests (without GroupBy)
public class MvRawEvent
{
    public string EventName { get; set; } = string.Empty;
    public DateTime EventTime { get; set; }
    public string UserId { get; set; } = string.Empty;
    public decimal Value { get; set; }
}

public class MvProcessedEvent
{
    public ulong EventNameId { get; set; }          // cityHash64(EventName)
    public DateTime EventTime { get; set; }
    public long Version { get; set; }               // toUnixTimestamp64Milli(EventTime)
    public decimal Value { get; set; }
    public byte IsActive { get; set; }              // Constant = 1
}

public class MvDailySummary
{
    public DateTime Date { get; set; }
    public int ProductId { get; set; }
    public decimal TotalQuantity { get; set; }
    public decimal TotalRevenue { get; set; }
}

public class MvHourlySummary
{
    public DateTime Hour { get; set; }
    public int ProductId { get; set; }
    public int OrderCount { get; set; }
    public decimal TotalRevenue { get; set; }
}

public class MvDailySummaryResult
{
    public DateTime Date { get; set; }
    public int ProductId { get; set; }
    public decimal TotalQuantity { get; set; }
    public decimal TotalRevenue { get; set; }
}

public class MvProcessedEventResult
{
    public ulong EventNameId { get; set; }
    public DateTime EventTime { get; set; }
    public long Version { get; set; }
    public decimal Value { get; set; }
    public byte IsActive { get; set; }
}

#endregion

#region Test Contexts

public class RawSqlMaterializedViewContext : DbContext
{
    public RawSqlMaterializedViewContext(DbContextOptions<RawSqlMaterializedViewContext> options)
        : base(options) { }

    public DbSet<MvOrder> Orders => Set<MvOrder>();
    public DbSet<MvDailySummary> DailySummaries => Set<MvDailySummary>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvOrder>(entity =>
        {
            entity.HasKey(e => e.OrderId);
            entity.ToTable("Orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        modelBuilder.Entity<MvDailySummary>(entity =>
        {
            entity.ToTable("DailySummary_MV");
            entity.UseSummingMergeTree(x => new { x.Date, x.ProductId });
            entity.AsMaterializedViewRaw(
                sourceTable: "Orders",
                selectSql: @"
                    SELECT
                        toDate(OrderDate) AS Date,
                        ProductId,
                        sum(Quantity) AS TotalQuantity,
                        sum(Revenue) AS TotalRevenue
                    FROM Orders
                    GROUP BY Date, ProductId
                ",
                populate: false
            );
        });
    }
}

public class LinqMaterializedViewContext : DbContext
{
    public LinqMaterializedViewContext(DbContextOptions<LinqMaterializedViewContext> options)
        : base(options) { }

    public DbSet<MvOrder> Orders => Set<MvOrder>();
    public DbSet<MvHourlySummary> HourlySummaries => Set<MvHourlySummary>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvOrder>(entity =>
        {
            entity.HasKey(e => e.OrderId);
            entity.ToTable("Orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        modelBuilder.Entity<MvHourlySummary>(entity =>
        {
            entity.ToTable("HourlySummary_MV");
            entity.UseSummingMergeTree(x => new { x.Hour, x.ProductId });
            entity.AsMaterializedView<MvHourlySummary, MvOrder>(
                query: orders => orders
                    .GroupBy(o => new { Hour = o.OrderDate.Date, o.ProductId })
                    .Select(g => new MvHourlySummary
                    {
                        Hour = g.Key.Hour,
                        ProductId = g.Key.ProductId,
                        OrderCount = g.Count(),
                        TotalRevenue = g.Sum(o => o.Revenue)
                    }),
                populate: false
            );
        });
    }
}

/// <summary>
/// Context for testing simple projection MV (Select without GroupBy).
/// Demonstrates data transformation with CityHash64, ToUnixTimestamp64Milli, and constants.
/// </summary>
public class SimpleProjectionMaterializedViewContext : DbContext
{
    public SimpleProjectionMaterializedViewContext(DbContextOptions<SimpleProjectionMaterializedViewContext> options)
        : base(options) { }

    public DbSet<MvRawEvent> RawEvents => Set<MvRawEvent>();
    public DbSet<MvProcessedEvent> ProcessedEvents => Set<MvProcessedEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Source table
        modelBuilder.Entity<MvRawEvent>(entity =>
        {
            entity.ToTable("RawEvents");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.EventName, x.EventTime });
        });

        // MV with simple projection (no GroupBy)
        modelBuilder.Entity<MvProcessedEvent>(entity =>
        {
            entity.ToTable("ProcessedEvents_MV");
            entity.UseReplacingMergeTree(x => new { x.EventNameId, x.EventTime }).WithVersion(x => x.Version);

            entity.AsMaterializedView<MvProcessedEvent, MvRawEvent>(
                query: raw => raw.Select(r => new MvProcessedEvent
                {
                    EventNameId = r.EventName.CityHash64(),
                    EventTime = r.EventTime,
                    Version = r.EventTime.ToUnixTimestamp64Milli(),
                    Value = r.Value,
                    IsActive = 1
                }),
                populate: false);
        });
    }
}

/// <summary>
/// Context for testing all standard LINQ aggregates with selectors.
/// Verifies Sum, Average, Min, Max translate with column references.
/// </summary>
public class AllAggregatesContext : DbContext
{
    public AllAggregatesContext(DbContextOptions<AllAggregatesContext> options)
        : base(options) { }

    public DbSet<MvOrder> Orders => Set<MvOrder>();
    public DbSet<MvAllAggregatesSummary> AllAggregatesSummaries => Set<MvAllAggregatesSummary>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvOrder>(entity =>
        {
            entity.HasKey(e => e.OrderId);
            entity.ToTable("Orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        modelBuilder.Entity<MvAllAggregatesSummary>(entity =>
        {
            entity.ToTable("AllAggregates_MV");
            entity.UseSummingMergeTree(x => x.ProductId);
            entity.AsMaterializedView<MvAllAggregatesSummary, MvOrder>(
                query: orders => orders
                    .GroupBy(o => o.ProductId)
                    .Select(g => new MvAllAggregatesSummary
                    {
                        ProductId = g.Key,
                        TotalQuantity = g.Sum(o => o.Quantity),
                        AverageRevenue = g.Average(o => o.Revenue),
                        MinRevenue = g.Min(o => o.Revenue),
                        MaxRevenue = g.Max(o => o.Revenue),
                        OrderCount = g.Count()
                    }),
                populate: false);
        });
    }
}

/// <summary>
/// Result entity for all aggregates test.
/// </summary>
public class MvAllAggregatesSummary
{
    public int ProductId { get; set; }
    public decimal TotalQuantity { get; set; }
    public decimal AverageRevenue { get; set; }
    public decimal MinRevenue { get; set; }
    public decimal MaxRevenue { get; set; }
    public int OrderCount { get; set; }
}

/// <summary>
/// Context for testing single-value group key (g.Key direct access).
/// </summary>
public class SingleKeyGroupByContext : DbContext
{
    public SingleKeyGroupByContext(DbContextOptions<SingleKeyGroupByContext> options)
        : base(options) { }

    public DbSet<MvOrder> Orders => Set<MvOrder>();
    public DbSet<MvDailyStats> DailyStats => Set<MvDailyStats>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvOrder>(entity =>
        {
            entity.HasKey(e => e.OrderId);
            entity.ToTable("Orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        modelBuilder.Entity<MvDailyStats>(entity =>
        {
            entity.ToTable("DailyStats_MV");
            entity.UseSummingMergeTree(x => x.Date);
            // Single-value key: g.Key is directly the DateTime
            entity.AsMaterializedView<MvDailyStats, MvOrder>(
                query: orders => orders
                    .GroupBy(o => o.OrderDate.Date)
                    .Select(g => new MvDailyStats
                    {
                        Date = g.Key,  // Direct g.Key access
                        TotalOrders = g.Count(),
                        TotalRevenue = g.Sum(o => o.Revenue)
                    }),
                populate: false);
        });
    }
}

/// <summary>
/// Result entity for single-key group by test.
/// </summary>
public class MvDailyStats
{
    public DateTime Date { get; set; }
    public int TotalOrders { get; set; }
    public decimal TotalRevenue { get; set; }
}

/// <summary>
/// Context for testing Count with predicate (countIf).
/// </summary>
public class CountIfContext : DbContext
{
    public CountIfContext(DbContextOptions<CountIfContext> options)
        : base(options) { }

    public DbSet<MvOrder> Orders => Set<MvOrder>();
    public DbSet<MvCountIfSummary> CountIfSummaries => Set<MvCountIfSummary>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvOrder>(entity =>
        {
            entity.HasKey(e => e.OrderId);
            entity.ToTable("Orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        modelBuilder.Entity<MvCountIfSummary>(entity =>
        {
            entity.ToTable("CountIf_MV");
            entity.UseSummingMergeTree(x => x.ProductId);
            entity.AsMaterializedView<MvCountIfSummary, MvOrder>(
                query: orders => orders
                    .GroupBy(o => o.ProductId)
                    .Select(g => new MvCountIfSummary
                    {
                        ProductId = g.Key,
                        TotalOrders = g.Count(),
                        HighValueOrders = g.Count(o => o.Revenue > 100)
                    }),
                populate: false);
        });
    }
}

/// <summary>
/// Result entity for countIf test.
/// </summary>
public class MvCountIfSummary
{
    public int ProductId { get; set; }
    public int TotalOrders { get; set; }
    public int HighValueOrders { get; set; }
}

/// <summary>
/// Context for testing ClickHouse-specific aggregates.
/// </summary>
public class ClickHouseAggregatesContext : DbContext
{
    public ClickHouseAggregatesContext(DbContextOptions<ClickHouseAggregatesContext> options)
        : base(options) { }

    public DbSet<MvRawEvent> RawEvents => Set<MvRawEvent>();
    public DbSet<MvClickHouseAggregatesSummary> AggregatesSummaries => Set<MvClickHouseAggregatesSummary>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvRawEvent>(entity =>
        {
            entity.ToTable("RawEvents");
            entity.HasNoKey();
            entity.UseMergeTree(x => new { x.EventName, x.EventTime });
        });

        modelBuilder.Entity<MvClickHouseAggregatesSummary>(entity =>
        {
            entity.ToTable("ClickHouseAggregates_MV");
            entity.UseSummingMergeTree(x => x.EventName);
            entity.AsMaterializedView<MvClickHouseAggregatesSummary, MvRawEvent>(
                query: events => events
                    .GroupBy(e => e.EventName)
                    .Select(g => new MvClickHouseAggregatesSummary
                    {
                        EventName = g.Key,
                        UniqueUsers = g.Uniq(e => e.UserId),
                        AnyUserId = g.AnyValue(e => e.UserId)
                    }),
                populate: false);
        });
    }
}

/// <summary>
/// Result entity for ClickHouse aggregates test.
/// </summary>
public class MvClickHouseAggregatesSummary
{
    public string EventName { get; set; } = string.Empty;
    public ulong UniqueUsers { get; set; }
    public string AnyUserId { get; set; } = string.Empty;
}

/// <summary>
/// Context for testing DateTime function translations.
/// </summary>
public class DateTimeFunctionsContext : DbContext
{
    public DateTimeFunctionsContext(DbContextOptions<DateTimeFunctionsContext> options)
        : base(options) { }

    public DbSet<MvOrder> Orders => Set<MvOrder>();
    public DbSet<MvHourlyByStartOfHour> HourlySummaries => Set<MvHourlyByStartOfHour>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvOrder>(entity =>
        {
            entity.HasKey(e => e.OrderId);
            entity.ToTable("Orders");
            entity.UseMergeTree(x => new { x.OrderDate, x.OrderId });
        });

        modelBuilder.Entity<MvHourlyByStartOfHour>(entity =>
        {
            entity.ToTable("HourlyByStartOfHour_MV");
            entity.UseSummingMergeTree(x => x.Hour);
            entity.AsMaterializedView<MvHourlyByStartOfHour, MvOrder>(
                query: orders => orders
                    .GroupBy(o => o.OrderDate.ToStartOfHour())
                    .Select(g => new MvHourlyByStartOfHour
                    {
                        Hour = g.Key,
                        TotalOrders = g.Count(),
                        TotalRevenue = g.Sum(o => o.Revenue)
                    }),
                populate: false);
        });
    }
}

/// <summary>
/// Result entity for ToStartOfHour test.
/// </summary>
public class MvHourlyByStartOfHour
{
    public DateTime Hour { get; set; }
    public int TotalOrders { get; set; }
    public decimal TotalRevenue { get; set; }
}

// ============================================================================
//   Phase F/G/H/I/J join-coverage contexts
// ============================================================================
//
// All re-use the JoinOrder/JoinCustomer/JoinedRevenue entities defined below for
// strictness/RIGHT/FULL OUTER coverage; SEMI/ANTI use single-side row types
// because their result selectors are single-arg.

public class SingleSideOrder
{
    public long OrderId { get; set; }
    public long Amount { get; set; }
}

public class SingleSideCustomer
{
    public long CustomerId { get; set; }
    public string Region { get; set; } = string.Empty;
}

public class CrossJoinTag
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class CrossJoinedRow
{
    public long OrderId { get; set; }
    public string Tag { get; set; } = string.Empty;
}

public class AnyInnerJoinContext : DbContext
{
    public AnyInnerJoinContext(DbContextOptions<AnyInnerJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<JoinedRevenue> Revenue => Set<JoinedRevenue>();
    private static readonly IQueryable<JoinCustomer> _stub = Enumerable.Empty<JoinCustomer>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("AnyJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinCustomer>(e => { e.ToTable("AnyJoinCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinedRevenue>(e =>
        {
            e.ToTable("AnyInnerRevenue_MV"); e.HasNoKey();
            e.UseSummingMergeTree(x => x.Region);
            e.AsMaterializedView<JoinedRevenue, JoinOrder>(orders => orders
                .AnyJoin(_stub, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                .GroupBy(x => x.Region)
                .Select(g => new JoinedRevenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        });
    }
}

public class AnyLeftJoinContext : DbContext
{
    public AnyLeftJoinContext(DbContextOptions<AnyLeftJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<JoinedRevenue> Revenue => Set<JoinedRevenue>();
    private static readonly IQueryable<JoinCustomer> _stub = Enumerable.Empty<JoinCustomer>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("AnyLeftJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinCustomer>(e => { e.ToTable("AnyLeftJoinCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinedRevenue>(e =>
        {
            e.ToTable("AnyLeftRevenue_MV"); e.HasNoKey();
            e.UseSummingMergeTree(x => x.Region);
            e.AsMaterializedView<JoinedRevenue, JoinOrder>(orders => orders
                .AnyLeftJoin(_stub, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                .GroupBy(x => x.Region)
                .Select(g => new JoinedRevenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        });
    }
}

public class AnyRightJoinContext : DbContext
{
    public AnyRightJoinContext(DbContextOptions<AnyRightJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<JoinedRevenue> Revenue => Set<JoinedRevenue>();
    private static readonly IQueryable<JoinCustomer> _stub = Enumerable.Empty<JoinCustomer>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("AnyRightJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinCustomer>(e => { e.ToTable("AnyRightJoinCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinedRevenue>(e =>
        {
            e.ToTable("AnyRightRevenue_MV"); e.HasNoKey();
            e.UseSummingMergeTree(x => x.Region);
            e.AsMaterializedView<JoinedRevenue, JoinOrder>(orders => orders
                .AnyRightJoin(_stub, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                .GroupBy(x => x.Region)
                .Select(g => new JoinedRevenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        });
    }
}

public class RightJoinContext : DbContext
{
    public RightJoinContext(DbContextOptions<RightJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<JoinedRevenue> Revenue => Set<JoinedRevenue>();
    private static readonly IQueryable<JoinCustomer> _stub = Enumerable.Empty<JoinCustomer>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("RightJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinCustomer>(e => { e.ToTable("RightJoinCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinedRevenue>(e =>
        {
            e.ToTable("RightJoinRevenue_MV"); e.HasNoKey();
            e.UseSummingMergeTree(x => x.Region);
            e.AsMaterializedView<JoinedRevenue, JoinOrder>(orders => orders
                .RightJoin(_stub, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                .GroupBy(x => x.Region)
                .Select(g => new JoinedRevenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        });
    }
}

public class FullOuterJoinContext : DbContext
{
    public FullOuterJoinContext(DbContextOptions<FullOuterJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<JoinedRevenue> Revenue => Set<JoinedRevenue>();
    private static readonly IQueryable<JoinCustomer> _stub = Enumerable.Empty<JoinCustomer>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("FullOuterOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinCustomer>(e => { e.ToTable("FullOuterCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinedRevenue>(e =>
        {
            e.ToTable("FullOuterRevenue_MV"); e.HasNoKey();
            e.UseSummingMergeTree(x => x.Region);
            e.AsMaterializedView<JoinedRevenue, JoinOrder>(orders => orders
                .FullOuterJoin(_stub, o => o.CustomerId, c => c.Id, (o, c) => new { o.Amount, c.Region })
                .GroupBy(x => x.Region)
                .Select(g => new JoinedRevenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        });
    }
}

public class LeftSemiJoinContext : DbContext
{
    public LeftSemiJoinContext(DbContextOptions<LeftSemiJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<SingleSideOrder> Result => Set<SingleSideOrder>();
    private static readonly IQueryable<JoinCustomer> _stub = Enumerable.Empty<JoinCustomer>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("LeftSemiOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinCustomer>(e => { e.ToTable("LeftSemiCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<SingleSideOrder>(e =>
        {
            e.ToTable("LeftSemi_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.OrderId);
            e.AsMaterializedView<SingleSideOrder, JoinOrder>(orders => orders
                .LeftSemiJoin(_stub, o => o.CustomerId, c => c.Id,
                    o => new SingleSideOrder { OrderId = o.Id, Amount = o.Amount }));
        });
    }
}

public class LeftAntiJoinContext : DbContext
{
    public LeftAntiJoinContext(DbContextOptions<LeftAntiJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<SingleSideOrder> Result => Set<SingleSideOrder>();
    private static readonly IQueryable<JoinCustomer> _stub = Enumerable.Empty<JoinCustomer>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("LeftAntiOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinCustomer>(e => { e.ToTable("LeftAntiCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<SingleSideOrder>(e =>
        {
            e.ToTable("LeftAnti_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.OrderId);
            e.AsMaterializedView<SingleSideOrder, JoinOrder>(orders => orders
                .LeftAntiJoin(_stub, o => o.CustomerId, c => c.Id,
                    o => new SingleSideOrder { OrderId = o.Id, Amount = o.Amount }));
        });
    }
}

public class RightSemiJoinContext : DbContext
{
    public RightSemiJoinContext(DbContextOptions<RightSemiJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<SingleSideCustomer> Result => Set<SingleSideCustomer>();
    private static readonly IQueryable<JoinCustomer> _stub = Enumerable.Empty<JoinCustomer>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("RightSemiOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinCustomer>(e => { e.ToTable("RightSemiCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<SingleSideCustomer>(e =>
        {
            e.ToTable("RightSemi_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.CustomerId);
            e.AsMaterializedView<SingleSideCustomer, JoinOrder>(orders => orders
                .RightSemiJoin(_stub, o => o.CustomerId, c => c.Id,
                    c => new SingleSideCustomer { CustomerId = c.Id, Region = c.Region }));
        });
    }
}

public class RightAntiJoinContext : DbContext
{
    public RightAntiJoinContext(DbContextOptions<RightAntiJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<SingleSideCustomer> Result => Set<SingleSideCustomer>();
    private static readonly IQueryable<JoinCustomer> _stub = Enumerable.Empty<JoinCustomer>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("RightAntiOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<JoinCustomer>(e => { e.ToTable("RightAntiCustomers"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<SingleSideCustomer>(e =>
        {
            e.ToTable("RightAnti_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.CustomerId);
            e.AsMaterializedView<SingleSideCustomer, JoinOrder>(orders => orders
                .RightAntiJoin(_stub, o => o.CustomerId, c => c.Id,
                    c => new SingleSideCustomer { CustomerId = c.Id, Region = c.Region }));
        });
    }
}

public class CrossJoinContext : DbContext
{
    public CrossJoinContext(DbContextOptions<CrossJoinContext> options) : base(options) { }
    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<CrossJoinTag> Tags => Set<CrossJoinTag>();
    public DbSet<CrossJoinedRow> Result => Set<CrossJoinedRow>();
    private static readonly IQueryable<CrossJoinTag> _stub = Enumerable.Empty<CrossJoinTag>().AsQueryable();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e => { e.ToTable("CrossJoinOrders"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<CrossJoinTag>(e => { e.ToTable("CrossJoinTags"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<CrossJoinedRow>(e =>
        {
            e.ToTable("CrossJoin_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.OrderId);
            e.AsMaterializedView<CrossJoinedRow, JoinOrder>(orders => orders
                .CrossJoin(_stub, (o, t) => new CrossJoinedRow { OrderId = o.Id, Tag = t.Name }));
        });
    }
}

// ============================================================================
//   Phase K — ARRAY JOIN / LEFT ARRAY JOIN MV translation tests
// ============================================================================

public class MvArrayJoinEvent
{
    public long Id { get; set; }
    public List<string> Tags { get; set; } = new();
}

public class ArrayJoinedRow
{
    public long EventId { get; set; }
    public string Tag { get; set; } = string.Empty;
}

public class ArrayJoinContext : DbContext
{
    public ArrayJoinContext(DbContextOptions<ArrayJoinContext> options) : base(options) { }
    public DbSet<MvArrayJoinEvent> Events => Set<MvArrayJoinEvent>();
    public DbSet<ArrayJoinedRow> Result => Set<ArrayJoinedRow>();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<MvArrayJoinEvent>(e => { e.ToTable("MvArrayJoinEvents"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<ArrayJoinedRow>(e =>
        {
            e.ToTable("ArrayJoin_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.EventId);
            e.AsMaterializedView<ArrayJoinedRow, MvArrayJoinEvent>(events => events
                .ArrayJoin(x => x.Tags, (x, tag) => new ArrayJoinedRow { EventId = x.Id, Tag = tag }));
        });
    }
}

public class LeftArrayJoinContext : DbContext
{
    public LeftArrayJoinContext(DbContextOptions<LeftArrayJoinContext> options) : base(options) { }
    public DbSet<MvArrayJoinEvent> Events => Set<MvArrayJoinEvent>();
    public DbSet<ArrayJoinedRow> Result => Set<ArrayJoinedRow>();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<MvArrayJoinEvent>(e => { e.ToTable("LeftMvArrayJoinEvents"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
        mb.Entity<ArrayJoinedRow>(e =>
        {
            e.ToTable("LeftArrayJoin_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.EventId);
            e.AsMaterializedView<ArrayJoinedRow, MvArrayJoinEvent>(events => events
                .LeftArrayJoin(x => x.Tags, (x, tag) => new ArrayJoinedRow { EventId = x.Id, Tag = tag }));
        });
    }
}

/// <summary>
/// Source entities for ASOF JOIN MV translation tests. Trades stream is the
/// outer (trigger) source; quotes is the lookup right-side. Equi-key on Symbol,
/// inequality on T (timestamp) — typical time-series ASOF pattern.
/// </summary>
public class AsofTrade
{
    public uint Id { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public DateTime T { get; set; }
}

public class AsofQuote
{
    public uint Id { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public DateTime T { get; set; }
    public decimal Price { get; set; }
}

public class AsofTradeWithQuote
{
    public uint Id { get; set; }
    public DateTime T { get; set; }
    public decimal Price { get; set; }
}

public class LinqAsofInnerJoinContext : DbContext
{
    public LinqAsofInnerJoinContext(DbContextOptions<LinqAsofInnerJoinContext> options) : base(options) { }
    public DbSet<AsofTrade> Trades => Set<AsofTrade>();
    public DbSet<AsofQuote> Quotes => Set<AsofQuote>();
    public DbSet<AsofTradeWithQuote> Result => Set<AsofTradeWithQuote>();

    private static readonly IQueryable<AsofQuote> _quotesStub = Enumerable.Empty<AsofQuote>().AsQueryable();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<AsofTrade>(e => { e.ToTable("AsofTrades"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Symbol, x.T }); });
        mb.Entity<AsofQuote>(e => { e.ToTable("AsofQuotes"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Symbol, x.T }); });
        mb.Entity<AsofTradeWithQuote>(e =>
        {
            e.ToTable("AsofTradeWithQuote_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.Id);
            e.AsMaterializedView<AsofTradeWithQuote, AsofTrade>(trades => trades
                .AsofJoin(_quotesStub,
                    t => t.Symbol,
                    q => q.Symbol,
                    (t, q) => t.T >= q.T,
                    (t, q) => new AsofTradeWithQuote { Id = t.Id, T = t.T, Price = q.Price }));
        });
    }
}

public class LinqAsofLeftJoinContext : DbContext
{
    public LinqAsofLeftJoinContext(DbContextOptions<LinqAsofLeftJoinContext> options) : base(options) { }
    public DbSet<AsofTrade> Trades => Set<AsofTrade>();
    public DbSet<AsofQuote> Quotes => Set<AsofQuote>();
    public DbSet<AsofTradeWithQuote> Result => Set<AsofTradeWithQuote>();

    private static readonly IQueryable<AsofQuote> _quotesStub = Enumerable.Empty<AsofQuote>().AsQueryable();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<AsofTrade>(e => { e.ToTable("AsofLeftTrades"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Symbol, x.T }); });
        mb.Entity<AsofQuote>(e => { e.ToTable("AsofLeftQuotes"); e.HasKey(x => x.Id); e.UseMergeTree(x => new { x.Symbol, x.T }); });
        mb.Entity<AsofTradeWithQuote>(e =>
        {
            e.ToTable("AsofLeftTradeWithQuote_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.Id);
            e.AsMaterializedView<AsofTradeWithQuote, AsofTrade>(trades => trades
                .AsofLeftJoin(_quotesStub,
                    t => t.Symbol,
                    q => q.Symbol,
                    (t, q) => t.T >= q.T,
                    (t, q) => new AsofTradeWithQuote { Id = t.Id, T = t.T, Price = q.Price }));
        });
    }
}

/// <summary>
/// Source entities for the LINQ Join MV translation tests.
/// </summary>
public class JoinCustomer
{
    public long Id { get; set; }
    public string Region { get; set; } = string.Empty;
}

public class JoinOrder
{
    public long Id { get; set; }
    public long CustomerId { get; set; }
    public long Amount { get; set; }
}

public class JoinedRevenue
{
    public string Region { get; set; } = string.Empty;
    public long Total { get; set; }
}

public class GroupJoinedOrderRegion
{
    public long OrderId { get; set; }
    public string Region { get; set; } = string.Empty;
    public long Amount { get; set; }
}

/// <summary>
/// Pins translation of <c>orders.Join(customers, ...)</c> in an MV definition.
/// The customers right-hand-side is a closure-captured static IQueryable&lt;T&gt; —
/// the translator must resolve its element type and the entity's table name.
/// </summary>
public class LinqInnerJoinContext : DbContext
{
    public LinqInnerJoinContext(DbContextOptions<LinqInnerJoinContext> options)
        : base(options) { }

    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<JoinedRevenue> Revenue => Set<JoinedRevenue>();

    private static readonly IQueryable<JoinCustomer> _customersStub =
        Enumerable.Empty<JoinCustomer>().AsQueryable();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e =>
        {
            e.ToTable("JoinOrderSrc"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
        });
        mb.Entity<JoinCustomer>(e =>
        {
            e.ToTable("JoinCustomerSrc"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
        });
        mb.Entity<JoinedRevenue>(e =>
        {
            e.ToTable("JoinRevenue_MV"); e.HasNoKey();
            e.UseSummingMergeTree(x => x.Region);
            e.AsMaterializedView<JoinedRevenue, JoinOrder>(orders => orders
                .Join(_customersStub,
                    o => o.CustomerId,
                    c => c.Id,
                    (o, c) => new { o.Amount, c.Region })
                .GroupBy(x => x.Region)
                .Select(g => new JoinedRevenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        });
    }
}

/// <summary>
/// Same shape as LinqInnerJoinContext but the customer entity is flagged as a
/// ClickHouse dictionary. The translator must emit <c>dictionary('name')</c>
/// instead of a quoted table name in the join clause.
/// </summary>
public class LinqJoinDictionaryContext : DbContext
{
    public LinqJoinDictionaryContext(DbContextOptions<LinqJoinDictionaryContext> options)
        : base(options) { }

    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<JoinedRevenue> Revenue => Set<JoinedRevenue>();

    private static readonly IQueryable<JoinCustomer> _customersStub =
        Enumerable.Empty<JoinCustomer>().AsQueryable();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e =>
        {
            e.ToTable("DictJoinOrderSrc"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
        });
        mb.Entity<JoinCustomer>(e =>
        {
            e.ToTable("CustomerLookup"); e.HasNoKey();
            e.HasAnnotation("ClickHouse:Dictionary", true);
        });
        mb.Entity<JoinedRevenue>(e =>
        {
            e.ToTable("DictJoinRevenue_MV"); e.HasNoKey();
            e.UseSummingMergeTree(x => x.Region);
            e.AsMaterializedView<JoinedRevenue, JoinOrder>(orders => orders
                .Join(_customersStub,
                    o => o.CustomerId,
                    c => c.Id,
                    (o, c) => new { o.Amount, c.Region })
                .GroupBy(x => x.Region)
                .Select(g => new JoinedRevenue { Region = g.Key, Total = g.Sum(x => x.Amount) }));
        });
    }
}

/// <summary>
/// Pins translation of <c>orders.GroupJoin(customers, ...)</c>: emits LEFT JOIN
/// and rewrites <c>cs.Select(c => c.X).FirstOrDefault() ?? d</c> into
/// <c>coalesce(t1."X", d)</c>.
/// </summary>
public class LinqGroupJoinContext : DbContext
{
    public LinqGroupJoinContext(DbContextOptions<LinqGroupJoinContext> options)
        : base(options) { }

    public DbSet<JoinOrder> Orders => Set<JoinOrder>();
    public DbSet<JoinCustomer> Customers => Set<JoinCustomer>();
    public DbSet<GroupJoinedOrderRegion> GroupJoined => Set<GroupJoinedOrderRegion>();

    private static readonly IQueryable<JoinCustomer> _customersStub =
        Enumerable.Empty<JoinCustomer>().AsQueryable();

    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<JoinOrder>(e =>
        {
            e.ToTable("GjOrderSrc"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
        });
        mb.Entity<JoinCustomer>(e =>
        {
            e.ToTable("GjCustomerSrc"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
        });
        mb.Entity<GroupJoinedOrderRegion>(e =>
        {
            e.ToTable("GjOrderRegion_MV"); e.HasNoKey();
            e.UseMergeTree(x => x.OrderId);
            e.AsMaterializedView<GroupJoinedOrderRegion, JoinOrder>(orders => orders
                .GroupJoin(_customersStub,
                    o => o.CustomerId,
                    c => c.Id,
                    (o, cs) => new GroupJoinedOrderRegion
                    {
                        OrderId = o.Id,
                        Region = cs.Select(c => c.Region).FirstOrDefault() ?? "",
                        Amount = o.Amount,
                    }));
        });
    }
}

/// <summary>
/// Source entity carrying AggregateFunction state columns for the MergeState rollup.
/// Each byte[] column holds the binary state produced by the corresponding -State
/// combinator on an upstream AggregatingMergeTree.
/// </summary>
public class MvMergeStateSource
{
    public string Bucket { get; set; } = string.Empty;
    public byte[] C { get; set; } = Array.Empty<byte>();
    public byte[] S { get; set; } = Array.Empty<byte>();
    public byte[] Av { get; set; } = Array.Empty<byte>();
    public byte[] Mn { get; set; } = Array.Empty<byte>();
    public byte[] Mx { get; set; } = Array.Empty<byte>();
    public byte[] U { get; set; } = Array.Empty<byte>();
    public byte[] Ue { get; set; } = Array.Empty<byte>();
    public byte[] An { get; set; } = Array.Empty<byte>();
    public byte[] Al { get; set; } = Array.Empty<byte>();
    public byte[] Q { get; set; } = Array.Empty<byte>();
}

/// <summary>
/// Rollup entity that re-states each merged aggregate for downstream AMTs.
/// </summary>
public class MvMergeStateRollup
{
    public string Bucket { get; set; } = string.Empty;
    public byte[] C { get; set; } = Array.Empty<byte>();
    public byte[] S { get; set; } = Array.Empty<byte>();
    public byte[] Av { get; set; } = Array.Empty<byte>();
    public byte[] Mn { get; set; } = Array.Empty<byte>();
    public byte[] Mx { get; set; } = Array.Empty<byte>();
    public byte[] U { get; set; } = Array.Empty<byte>();
    public byte[] Ue { get; set; } = Array.Empty<byte>();
    public byte[] An { get; set; } = Array.Empty<byte>();
    public byte[] Al { get; set; } = Array.Empty<byte>();
    public byte[] Q { get; set; } = Array.Empty<byte>();
}

/// <summary>
/// Pins translation of all 10 -MergeState combinators in one MV definition.
/// Each Property is bound to its upstream AggregateFunction column type so the
/// translator's column lookup resolves correctly.
/// </summary>
public class MergeStateAggregatesContext : DbContext
{
    public MergeStateAggregatesContext(DbContextOptions<MergeStateAggregatesContext> options)
        : base(options) { }

    public DbSet<MvMergeStateSource> Source => Set<MvMergeStateSource>();
    public DbSet<MvMergeStateRollup> Rollup => Set<MvMergeStateRollup>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MvMergeStateSource>(entity =>
        {
            entity.ToTable("MergeStateSource");
            entity.HasNoKey();
            entity.UseAggregatingMergeTree(x => x.Bucket);
            entity.Property(x => x.C).HasAggregateFunction("count", typeof(ulong));
            entity.Property(x => x.S).HasAggregateFunction("sum", typeof(long));
            entity.Property(x => x.Av).HasAggregateFunction("avg", typeof(double));
            entity.Property(x => x.Mn).HasAggregateFunction("min", typeof(long));
            entity.Property(x => x.Mx).HasAggregateFunction("max", typeof(long));
            entity.Property(x => x.U).HasAggregateFunction("uniq", typeof(long));
            entity.Property(x => x.Ue).HasAggregateFunction("uniqExact", typeof(long));
            entity.Property(x => x.An).HasAggregateFunction("any", typeof(long));
            entity.Property(x => x.Al).HasAggregateFunction("anyLast", typeof(long));
            entity.Property(x => x.Q).HasAggregateFunction("quantile", typeof(double));
        });

        modelBuilder.Entity<MvMergeStateRollup>(entity =>
        {
            entity.ToTable("MergeStateRollup_MV");
            entity.HasNoKey();
            entity.UseAggregatingMergeTree(x => x.Bucket);
            entity.Property(x => x.C).HasAggregateFunction("count", typeof(ulong));
            entity.Property(x => x.S).HasAggregateFunction("sum", typeof(long));
            entity.Property(x => x.Av).HasAggregateFunction("avg", typeof(double));
            entity.Property(x => x.Mn).HasAggregateFunction("min", typeof(long));
            entity.Property(x => x.Mx).HasAggregateFunction("max", typeof(long));
            entity.Property(x => x.U).HasAggregateFunction("uniq", typeof(long));
            entity.Property(x => x.Ue).HasAggregateFunction("uniqExact", typeof(long));
            entity.Property(x => x.An).HasAggregateFunction("any", typeof(long));
            entity.Property(x => x.Al).HasAggregateFunction("anyLast", typeof(long));
            entity.Property(x => x.Q).HasAggregateFunction("quantile", typeof(double));
            entity.AsMaterializedView<MvMergeStateRollup, MvMergeStateSource>(
                query: rows => rows
                    .GroupBy(r => r.Bucket)
                    .Select(g => new MvMergeStateRollup
                    {
                        Bucket = g.Key,
                        C = g.CountMergeState(r => r.C),
                        S = g.SumMergeState(r => r.S),
                        Av = g.AvgMergeState(r => r.Av),
                        Mn = g.MinMergeState(r => r.Mn),
                        Mx = g.MaxMergeState(r => r.Mx),
                        U = g.UniqMergeState(r => r.U),
                        Ue = g.UniqExactMergeState(r => r.Ue),
                        An = g.AnyMergeState(r => r.An),
                        Al = g.AnyLastMergeState(r => r.Al),
                        Q = g.QuantileMergeState(0.5, r => r.Q),
                    }),
                populate: false);
        });
    }
}

#endregion
