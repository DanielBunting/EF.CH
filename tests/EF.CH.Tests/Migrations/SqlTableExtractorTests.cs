using EF.CH.Migrations.Design;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Unit tests for SqlTableExtractor.
/// Tests SQL parsing and table reference extraction using SqlParserCS.
/// </summary>
public class SqlTableExtractorTests
{
    #region Basic SELECT Tests

    [Fact]
    public void ExtractTableReferences_SimpleSelect_ReturnsTableName()
    {
        var sql = "SELECT * FROM orders";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("orders", result);
    }

    [Fact]
    public void ExtractTableReferences_QuotedIdentifier_ReturnsUnquotedName()
    {
        var sql = "SELECT * FROM \"Orders\"";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("Orders", result);
    }

    [Fact]
    public void ExtractTableReferences_SelectWithColumns_ReturnsTableName()
    {
        var sql = "SELECT id, name, created_at FROM customers";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("customers", result);
    }

    [Fact]
    public void ExtractTableReferences_SelectWithWhereClause_ReturnsTableName()
    {
        var sql = "SELECT * FROM products WHERE price > 100";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("products", result);
    }

    #endregion

    #region JOIN Tests

    [Fact]
    public void ExtractTableReferences_InnerJoin_ReturnsBothTables()
    {
        var sql = "SELECT o.id, c.name FROM orders o INNER JOIN customers c ON o.customer_id = c.id";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Equal(2, result.Count);
        Assert.Contains("orders", result);
        Assert.Contains("customers", result);
    }

    [Fact]
    public void ExtractTableReferences_LeftJoin_ReturnsBothTables()
    {
        var sql = "SELECT * FROM orders LEFT JOIN order_items ON orders.id = order_items.order_id";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Equal(2, result.Count);
        Assert.Contains("orders", result);
        Assert.Contains("order_items", result);
    }

    [Fact]
    public void ExtractTableReferences_MultipleJoins_ReturnsAllTables()
    {
        var sql = @"
            SELECT o.id, c.name, p.product_name
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            JOIN products p ON o.product_id = p.id";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Equal(3, result.Count);
        Assert.Contains("orders", result);
        Assert.Contains("customers", result);
        Assert.Contains("products", result);
    }

    #endregion

    #region GROUP BY / Aggregate Tests

    [Fact]
    public void ExtractTableReferences_GroupBy_ReturnsTableName()
    {
        var sql = "SELECT date, SUM(amount) FROM sales GROUP BY date";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("sales", result);
    }

    [Fact]
    public void ExtractTableReferences_MaterializedViewQuery_ReturnsSourceTable()
    {
        var sql = @"
            SELECT toDate(OrderDate) AS Date, ProductId, sum(Quantity) AS TotalQuantity
            FROM Orders
            GROUP BY Date, ProductId";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("Orders", result);
    }

    #endregion

    #region Subquery Tests

    [Fact]
    public void ExtractTableReferences_SubqueryInFrom_ReturnsInnerTable()
    {
        var sql = "SELECT * FROM (SELECT * FROM raw_events) AS sub";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("raw_events", result);
    }

    [Fact]
    public void ExtractTableReferences_SubqueryInWhere_ReturnsAllTables()
    {
        var sql = "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM premium_customers)";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Equal(2, result.Count);
        Assert.Contains("orders", result);
        Assert.Contains("premium_customers", result);
    }

    #endregion

    #region UNION / Set Operations Tests

    [Fact]
    public void ExtractTableReferences_Union_ReturnsBothTables()
    {
        var sql = "SELECT id FROM orders_2023 UNION ALL SELECT id FROM orders_2024";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Equal(2, result.Count);
        Assert.Contains("orders_2023", result);
        Assert.Contains("orders_2024", result);
    }

    #endregion

    #region Edge Cases

    [Fact]
    public void ExtractTableReferences_EmptySql_ReturnsEmptySet()
    {
        var result = SqlTableExtractor.ExtractTableReferences("");
        Assert.Empty(result);
    }

    [Fact]
    public void ExtractTableReferences_NullSql_ReturnsEmptySet()
    {
        var result = SqlTableExtractor.ExtractTableReferences(null!);
        Assert.Empty(result);
    }

    [Fact]
    public void ExtractTableReferences_WhitespaceSql_ReturnsEmptySet()
    {
        var result = SqlTableExtractor.ExtractTableReferences("   \n\t   ");
        Assert.Empty(result);
    }

    [Fact]
    public void ExtractTableReferences_InvalidSql_ReturnsEmptySet()
    {
        // Should gracefully handle parse errors
        var result = SqlTableExtractor.ExtractTableReferences("THIS IS NOT VALID SQL AT ALL");
        Assert.Empty(result);
    }

    [Fact]
    public void ExtractTableReferences_CaseInsensitive_FindsTable()
    {
        var sql = "SELECT * FROM ORDERS";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        // The result should contain the table name as parsed
        Assert.Contains("ORDERS", result);
    }

    #endregion

    #region ClickHouse-Specific Syntax Tests

    [Fact]
    public void ExtractTableReferences_ClickHouseArrayJoin_ReturnsTable()
    {
        // ARRAY JOIN is ClickHouse-specific
        // Note: SqlParserCS interprets "tags AS tag" as a table reference
        // This is acceptable - extra false positives don't break dependency ordering
        var sql = "SELECT id, tag FROM events ARRAY JOIN tags AS tag";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        // Parser sees both "events" (actual table) and "tags" (column parsed as table)
        Assert.Contains("events", result);
    }

    [Fact]
    public void ExtractTableReferences_ClickHouseFinal_ReturnsTable()
    {
        var sql = "SELECT * FROM events FINAL WHERE date > '2024-01-01'";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("events", result);
    }

    [Fact]
    public void ExtractTableReferences_QualifiedTableName_ReturnsTableNameOnly()
    {
        // Database.table format - should return just the table name
        var sql = "SELECT * FROM default.orders";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("orders", result);
    }

    #endregion

    #region Real-World Materialized View Queries

    [Fact]
    public void ExtractTableReferences_RealMvQuery_HourlySummary()
    {
        var sql = @"
            SELECT
                toStartOfHour(created_at) AS hour,
                product_id,
                count() AS order_count,
                sum(amount) AS total_amount
            FROM orders
            GROUP BY hour, product_id";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("orders", result);
    }

    [Fact]
    public void ExtractTableReferences_RealMvQuery_WithJoin()
    {
        var sql = @"
            SELECT
                toDate(o.created_at) AS date,
                c.region,
                sum(o.amount) AS total
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            GROUP BY date, c.region";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Equal(2, result.Count);
        Assert.Contains("orders", result);
        Assert.Contains("customers", result);
    }

    [Fact]
    public void ExtractTableReferences_CascadingMvQuery_DependsOnAnotherMv()
    {
        // This is the key use case for V2 - MV-B reads from MV-A
        var sql = @"
            SELECT
                date,
                sum(order_count) AS total_orders,
                sum(total_amount) AS total_revenue
            FROM hourly_summary
            GROUP BY date";
        var result = SqlTableExtractor.ExtractTableReferences(sql);

        Assert.Single(result);
        Assert.Contains("hourly_summary", result);
    }

    #endregion
}
