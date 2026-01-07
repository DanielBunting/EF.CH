using SqlParser;
using SqlParser.Ast;
using SqlParser.Dialects;

using AstQuery = SqlParser.Ast.Query;

namespace EF.CH.Migrations.Design;

/// <summary>
/// Utility class for extracting table references from SQL statements.
/// Uses SqlParserCS with ClickHouse dialect for accurate parsing.
/// </summary>
public static class SqlTableExtractor
{
    /// <summary>
    /// Extracts all table names referenced in a SQL SELECT statement.
    /// Handles FROM clauses, JOINs, subqueries, and ClickHouse-specific syntax.
    /// </summary>
    /// <param name="sql">The SQL statement to parse.</param>
    /// <returns>A set of table names found in the statement. Returns empty set on parse failure.</returns>
    public static HashSet<string> ExtractTableReferences(string sql)
    {
        var tables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(sql))
            return tables;

        try
        {
            var parser = new Parser();
            var statements = parser.ParseSql(sql, new ClickHouseDialect());

            foreach (var statement in statements)
            {
                ExtractFromStatement(statement, tables);
            }
        }
        catch
        {
            // On parse failure, return empty set for graceful degradation
            return [];
        }

        return tables;
    }

    private static void ExtractFromStatement(Statement statement, HashSet<string> tables)
    {
        switch (statement)
        {
            case Statement.Select select:
                ExtractFromQuery(select.Query, tables);
                break;
        }
    }

    private static void ExtractFromQuery(AstQuery? query, HashSet<string> tables)
    {
        if (query == null)
            return;

        if (query.Body != null)
            ExtractFromSetExpression(query.Body, tables);

        // Handle CTEs (WITH clauses)
        if (query.With?.CteTables != null)
        {
            foreach (var cte in query.With.CteTables)
            {
                ExtractFromQuery(cte.Query, tables);
            }
        }
    }

    private static void ExtractFromSetExpression(SetExpression? setExpr, HashSet<string> tables)
    {
        if (setExpr == null)
            return;

        switch (setExpr)
        {
            case SetExpression.SelectExpression selectExpr:
                ExtractFromSelect(selectExpr.Select, tables);
                break;

            case SetExpression.SetOperation setOp:
                // UNION, INTERSECT, EXCEPT
                ExtractFromSetExpression(setOp.Left, tables);
                ExtractFromSetExpression(setOp.Right, tables);
                break;

            case SetExpression.QueryExpression queryExpr:
                ExtractFromQuery(queryExpr.Query, tables);
                break;
        }
    }

    private static void ExtractFromSelect(Select? select, HashSet<string> tables)
    {
        if (select == null)
            return;

        // Extract from FROM clause
        if (select.From != null)
        {
            foreach (var tableWithJoins in select.From)
            {
                ExtractFromTableWithJoins(tableWithJoins, tables);
            }
        }

        // Check for subqueries in WHERE
        ExtractFromExpression(select.Selection, tables);
    }

    private static void ExtractFromTableWithJoins(TableWithJoins? tableWithJoins, HashSet<string> tables)
    {
        if (tableWithJoins == null)
            return;

        // Extract from the main table
        ExtractFromTableFactor(tableWithJoins.Relation, tables);

        // Extract from joined tables
        if (tableWithJoins.Joins != null)
        {
            foreach (var join in tableWithJoins.Joins)
            {
                ExtractFromTableFactor(join.Relation, tables);
            }
        }
    }

    private static void ExtractFromTableFactor(TableFactor? tableFactor, HashSet<string> tables)
    {
        if (tableFactor == null)
            return;

        switch (tableFactor)
        {
            case TableFactor.Table table:
                // This is a regular table reference
                var tableName = GetTableName(table.Name);
                if (!string.IsNullOrEmpty(tableName))
                    tables.Add(tableName);
                break;

            case TableFactor.Derived derived:
                // Subquery in FROM clause
                ExtractFromQuery(derived.SubQuery, tables);
                break;

            case TableFactor.NestedJoin nestedJoin:
                // Nested join (parenthesized join)
                ExtractFromTableWithJoins(nestedJoin.TableWithJoins, tables);
                break;
        }
    }

    private static void ExtractFromExpression(Expression? expr, HashSet<string> tables)
    {
        if (expr == null)
            return;

        switch (expr)
        {
            case Expression.Subquery subquery:
                ExtractFromQuery(subquery.Query, tables);
                break;

            case Expression.InSubquery inSubquery:
                ExtractFromQuery(inSubquery.SubQuery, tables);
                break;

            case Expression.Exists exists:
                ExtractFromQuery(exists.SubQuery, tables);
                break;

            case Expression.BinaryOp binaryOp:
                ExtractFromExpression(binaryOp.Left, tables);
                ExtractFromExpression(binaryOp.Right, tables);
                break;

            case Expression.UnaryOp unaryOp:
                ExtractFromExpression(unaryOp.Expression, tables);
                break;

            case Expression.Nested nested:
                ExtractFromExpression(nested.Expression, tables);
                break;
        }
    }

    /// <summary>
    /// Extracts the table name from an ObjectName, handling quoted identifiers.
    /// </summary>
    private static string GetTableName(ObjectName? objectName)
    {
        if (objectName == null || objectName.Values.Count == 0)
            return string.Empty;

        // Get the last part (table name, ignoring database/schema prefix)
        var lastIdent = objectName.Values[^1];
        return GetIdentifierValue(lastIdent);
    }

    /// <summary>
    /// Extracts the value from an identifier, removing quotes if present.
    /// </summary>
    private static string GetIdentifierValue(Ident ident)
    {
        // The Value property contains the unquoted identifier
        return ident.Value;
    }
}
