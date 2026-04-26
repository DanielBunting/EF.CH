using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.Views;

/// <summary>
/// Generates CREATE / DROP VIEW SQL for plain (non-parameterized, non-materialized)
/// ClickHouse views from <see cref="ViewMetadataBase"/>.
/// </summary>
public static class ViewSqlGenerator
{
    /// <summary>
    /// Generates a CREATE VIEW statement for the supplied metadata.
    /// </summary>
    public static string GenerateCreateViewSql(IModel model, ViewMetadataBase metadata)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(metadata);

        if (metadata.IfNotExists && metadata.OrReplace)
        {
            throw new InvalidOperationException(
                "ClickHouse CREATE VIEW does not allow combining IF NOT EXISTS with OR REPLACE.");
        }

        var sb = new StringBuilder();
        sb.Append("CREATE ");
        if (metadata.OrReplace)
        {
            sb.Append("OR REPLACE ");
        }
        sb.Append("VIEW ");
        if (metadata.IfNotExists)
        {
            sb.Append("IF NOT EXISTS ");
        }

        AppendQualifiedName(sb, metadata.Schema, metadata.ViewName);

        if (!string.IsNullOrEmpty(metadata.OnCluster))
        {
            sb.Append(" ON CLUSTER ");
            sb.Append(metadata.OnCluster);
        }

        sb.Append(" AS\n");

        if (!string.IsNullOrEmpty(metadata.RawSelectSql))
        {
            sb.Append(metadata.RawSelectSql.Trim());
            return sb.ToString();
        }

        sb.Append(GenerateSelectClause(model, metadata));

        var tableName = ResolveTableName(model, metadata);
        sb.Append("\nFROM \"");
        sb.Append(EscapeIdentifier(tableName));
        sb.Append('"');

        if (metadata.WhereExpressions?.Count > 0)
        {
            sb.Append("\nWHERE ");
            var whereClauses = new List<string>();
            foreach (var whereExpr in metadata.WhereExpressions)
            {
                whereClauses.Add(GenerateWhereClause(model, metadata, whereExpr));
            }
            sb.Append(string.Join("\n  AND ", whereClauses));
        }

        return sb.ToString();
    }

    /// <summary>
    /// Generates a DROP VIEW statement.
    /// </summary>
    public static string GenerateDropViewSql(
        string viewName,
        string? schema = null,
        bool ifExists = true,
        string? onCluster = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(viewName);

        var sb = new StringBuilder();
        sb.Append("DROP VIEW ");
        if (ifExists)
        {
            sb.Append("IF EXISTS ");
        }

        AppendQualifiedName(sb, schema, viewName);

        if (!string.IsNullOrEmpty(onCluster))
        {
            sb.Append(" ON CLUSTER ");
            sb.Append(onCluster);
        }

        return sb.ToString();
    }

    private static void AppendQualifiedName(StringBuilder sb, string? schema, string name)
    {
        if (!string.IsNullOrEmpty(schema))
        {
            sb.Append('"');
            sb.Append(EscapeIdentifier(schema));
            sb.Append("\".\"");
            sb.Append(EscapeIdentifier(name));
            sb.Append('"');
        }
        else
        {
            sb.Append('"');
            sb.Append(EscapeIdentifier(name));
            sb.Append('"');
        }
    }

    private static string GenerateSelectClause(IModel model, ViewMetadataBase metadata)
    {
        if (metadata.ProjectionExpression == null)
        {
            throw new InvalidOperationException("Projection expression is required.");
        }

        if (metadata.ProjectionExpression.Body is not MemberInitExpression memberInit)
        {
            throw new InvalidOperationException(
                "Projection expression must be a member initialization expression (e.g., e => new View { Prop = e.Prop }).");
        }

        var sourceEntityType = metadata.SourceType != null ? model.FindEntityType(metadata.SourceType) : null;
        var selectColumns = new List<string>();

        foreach (var binding in memberInit.Bindings)
        {
            if (binding is not MemberAssignment assignment)
            {
                throw new InvalidOperationException(
                    $"Only simple property assignments are supported in projection. Found: {binding.BindingType}");
            }

            var targetPropertyName = assignment.Member.Name;
            var sourceColumnName = ExtractSourceColumnName(assignment.Expression, sourceEntityType);
            selectColumns.Add($"\"{EscapeIdentifier(sourceColumnName)}\" AS \"{EscapeIdentifier(targetPropertyName)}\"");
        }

        return "SELECT " + string.Join(",\n       ", selectColumns);
    }

    private static string ExtractSourceColumnName(Expression expression, IEntityType? sourceEntityType)
    {
        if (expression is MemberExpression memberExpr && memberExpr.Member is PropertyInfo propInfo)
        {
            if (sourceEntityType != null)
            {
                var property = sourceEntityType.FindProperty(propInfo.Name);
                var columnName = property?.GetColumnName();
                if (columnName != null)
                {
                    return columnName;
                }
            }
            return ToSnakeCase(propInfo.Name);
        }

        throw new InvalidOperationException(
            $"Unsupported projection expression: {expression}. Only simple property access is supported.");
    }

    private static string GenerateWhereClause(IModel model, ViewMetadataBase metadata, LambdaExpression whereExpr)
    {
        var sourceEntityType = metadata.SourceType != null ? model.FindEntityType(metadata.SourceType) : null;
        var visitor = new WhereClauseVisitor(sourceEntityType);
        visitor.Visit(whereExpr.Body);
        return visitor.GetSql();
    }

    private static string ResolveTableName(IModel model, ViewMetadataBase metadata)
    {
        if (!string.IsNullOrEmpty(metadata.SourceTable))
        {
            return metadata.SourceTable;
        }

        if (metadata.SourceType == null)
        {
            throw new InvalidOperationException("Source type or explicit table name is required.");
        }

        var entityType = model.FindEntityType(metadata.SourceType)
            ?? throw new InvalidOperationException(
                $"Source entity type '{metadata.SourceType.Name}' is not configured in the model.");

        return entityType.GetTableName()
            ?? throw new InvalidOperationException(
                $"Source entity type '{metadata.SourceType.Name}' does not have a table name.");
    }

    internal static string ToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        if (name.Contains('_'))
            return name.ToLowerInvariant();

        var sb = new StringBuilder();
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                    sb.Append('_');
                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }

    private static string EscapeIdentifier(string identifier)
        => identifier.Replace("\"", "\"\"");

    private sealed class WhereClauseVisitor : ExpressionVisitor
    {
        private readonly IEntityType? _sourceEntityType;
        private readonly StringBuilder _sql = new();

        public WhereClauseVisitor(IEntityType? sourceEntityType)
        {
            _sourceEntityType = sourceEntityType;
        }

        public string GetSql() => _sql.ToString();

        protected override Expression VisitBinary(BinaryExpression node)
        {
            _sql.Append('(');
            Visit(node.Left);

            var op = node.NodeType switch
            {
                ExpressionType.Equal => " = ",
                ExpressionType.NotEqual => " != ",
                ExpressionType.GreaterThan => " > ",
                ExpressionType.GreaterThanOrEqual => " >= ",
                ExpressionType.LessThan => " < ",
                ExpressionType.LessThanOrEqual => " <= ",
                ExpressionType.AndAlso => " AND ",
                ExpressionType.OrElse => " OR ",
                _ => throw new NotSupportedException($"Binary operator {node.NodeType} is not supported.")
            };

            _sql.Append(op);
            Visit(node.Right);
            _sql.Append(')');
            return node;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member is PropertyInfo propInfo && IsSourceEntityParameter(node))
            {
                var columnName = GetColumnName(propInfo);
                _sql.Append('"');
                _sql.Append(columnName.Replace("\"", "\"\""));
                _sql.Append('"');
                return node;
            }

            // Captured/closure constant: evaluate and emit the literal.
            var value = ExpressionEvaluator.Evaluate(node);
            _sql.Append(FormatConstant(value));
            return node;
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            _sql.Append(FormatConstant(node.Value));
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Not)
            {
                _sql.Append("NOT ");
                Visit(node.Operand);
                return node;
            }

            if (node.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked)
            {
                Visit(node.Operand);
                return node;
            }

            throw new NotSupportedException($"Unary operator {node.NodeType} is not supported.");
        }

        private static bool IsSourceEntityParameter(MemberExpression node)
            => node.Expression is ParameterExpression;

        private string GetColumnName(PropertyInfo propInfo)
        {
            if (_sourceEntityType != null)
            {
                var property = _sourceEntityType.FindProperty(propInfo.Name);
                var columnName = property?.GetColumnName();
                if (columnName != null)
                {
                    return columnName;
                }
            }
            return ToSnakeCase(propInfo.Name);
        }

        private static string FormatConstant(object? value)
        {
            return value switch
            {
                null => "NULL",
                string s => $"'{EscapeString(s)}'",
                bool b => b ? "1" : "0",
                DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
                DateTimeOffset dto => $"'{dto.UtcDateTime:yyyy-MM-dd HH:mm:ss}'",
                DateOnly d => $"'{d:yyyy-MM-dd}'",
                TimeOnly t => $"'{t:HH:mm:ss}'",
                decimal dec => dec.ToString(CultureInfo.InvariantCulture),
                double dbl => dbl.ToString(CultureInfo.InvariantCulture),
                float flt => flt.ToString(CultureInfo.InvariantCulture),
                Guid g => $"'{g}'",
                Enum e => Convert.ToInt64(e).ToString(CultureInfo.InvariantCulture),
                _ when value.GetType().IsValueType => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "NULL",
                _ => $"'{EscapeString(value.ToString() ?? string.Empty)}'"
            };
        }

        private static string EscapeString(string value)
            => value.Replace("\\", "\\\\").Replace("'", "\\'");
    }

    private static class ExpressionEvaluator
    {
        public static object? Evaluate(Expression expression)
        {
            if (expression is ConstantExpression ce)
            {
                return ce.Value;
            }

            // Compile-and-invoke as a generic fallback for closures / property chains.
            var lambda = Expression.Lambda(expression);
            return lambda.Compile().DynamicInvoke();
        }
    }
}
