using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EF.CH.ParameterizedViews;

/// <summary>
/// Generates CREATE VIEW SQL statements for parameterized views from fluent configuration.
/// </summary>
public static class ParameterizedViewSqlGenerator
{
    /// <summary>
    /// Generates CREATE VIEW SQL for a parameterized view configuration.
    /// </summary>
    /// <param name="model">The EF Core model for table name resolution.</param>
    /// <param name="metadata">The view configuration metadata.</param>
    /// <param name="ifNotExists">Whether to include IF NOT EXISTS clause.</param>
    /// <returns>The CREATE VIEW SQL statement.</returns>
    public static string GenerateCreateViewSql(IModel model, ParameterizedViewMetadataBase metadata, bool ifNotExists = false)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(metadata);

        var sb = new StringBuilder();

        // CREATE VIEW [IF NOT EXISTS] "view_name" AS
        sb.Append("CREATE VIEW ");
        if (ifNotExists)
        {
            sb.Append("IF NOT EXISTS ");
        }
        sb.Append('"');
        sb.Append(EscapeIdentifier(metadata.ViewName));
        sb.Append("\" AS\n");

        // SELECT clause from projection
        var selectClause = GenerateSelectClause(model, metadata);
        sb.Append(selectClause);

        // FROM clause
        var tableName = ResolveTableName(model, metadata);
        sb.Append("\nFROM \"");
        sb.Append(EscapeIdentifier(tableName));
        sb.Append('"');

        // WHERE clauses
        if (metadata.WhereExpressions?.Count > 0 && metadata.Parameters != null)
        {
            sb.Append("\nWHERE ");
            var whereClauses = new List<string>();
            foreach (var whereExpr in metadata.WhereExpressions)
            {
                var whereClause = GenerateWhereClause(model, metadata, whereExpr);
                whereClauses.Add(whereClause);
            }
            sb.Append(string.Join("\n  AND ", whereClauses));
        }

        return sb.ToString();
    }

    private static string GenerateSelectClause(IModel model, ParameterizedViewMetadataBase metadata)
    {
        if (metadata.ProjectionExpression == null)
        {
            throw new InvalidOperationException("Projection expression is required.");
        }

        // The projection expression should be a lambda: e => new TView { Prop1 = e.Prop1, ... }
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

            // Generate: "source_column" AS "TargetProperty"
            selectColumns.Add($"\"{EscapeIdentifier(sourceColumnName)}\" AS \"{EscapeIdentifier(targetPropertyName)}\"");
        }

        return "SELECT " + string.Join(",\n       ", selectColumns);
    }

    private static string ExtractSourceColumnName(Expression expression, IEntityType? sourceEntityType)
    {
        // Handle simple member access: e.PropertyName
        if (expression is MemberExpression memberExpr && memberExpr.Member is PropertyInfo propInfo)
        {
            // Try to get the column name from EF Core model
            if (sourceEntityType != null)
            {
                var property = sourceEntityType.FindProperty(propInfo.Name);
                if (property != null)
                {
                    var columnName = property.GetColumnName();
                    if (columnName != null)
                    {
                        return columnName;
                    }
                }
            }

            // Fall back to snake_case conversion
            return ToSnakeCase(propInfo.Name);
        }

        throw new InvalidOperationException(
            $"Unsupported projection expression: {expression}. Only simple property access is supported.");
    }

    private static string GenerateWhereClause(
        IModel model,
        ParameterizedViewMetadataBase metadata,
        LambdaExpression whereExpr)
    {
        var sourceEntityType = metadata.SourceType != null ? model.FindEntityType(metadata.SourceType) : null;
        var visitor = new WhereClauseVisitor(sourceEntityType, metadata.Parameters!);
        visitor.Visit(whereExpr.Body);
        return visitor.GetSql();
    }

    private static string ResolveTableName(IModel model, ParameterizedViewMetadataBase metadata)
    {
        if (!string.IsNullOrEmpty(metadata.SourceTable))
        {
            return metadata.SourceTable;
        }

        if (metadata.SourceType == null)
        {
            throw new InvalidOperationException("Source type or explicit table name is required.");
        }

        var entityType = model.FindEntityType(metadata.SourceType);
        if (entityType == null)
        {
            throw new InvalidOperationException(
                $"Source entity type '{metadata.SourceType.Name}' is not configured in the model.");
        }

        return entityType.GetTableName()
            ?? throw new InvalidOperationException(
                $"Source entity type '{metadata.SourceType.Name}' does not have a table name.");
    }

    /// <summary>
    /// Converts a PascalCase property name to snake_case.
    /// </summary>
    internal static string ToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        // Already snake_case
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
    {
        return identifier.Replace("\"", "\"\"");
    }

    /// <summary>
    /// Maps a CLR type to a ClickHouse type name.
    /// </summary>
    internal static string GetClickHouseType(Type clrType)
    {
        // Handle nullable types
        var underlyingType = Nullable.GetUnderlyingType(clrType) ?? clrType;

        return underlyingType switch
        {
            _ when underlyingType == typeof(bool) => "UInt8",
            _ when underlyingType == typeof(byte) => "UInt8",
            _ when underlyingType == typeof(sbyte) => "Int8",
            _ when underlyingType == typeof(short) => "Int16",
            _ when underlyingType == typeof(ushort) => "UInt16",
            _ when underlyingType == typeof(int) => "Int32",
            _ when underlyingType == typeof(uint) => "UInt32",
            _ when underlyingType == typeof(long) => "Int64",
            _ when underlyingType == typeof(ulong) => "UInt64",
            _ when underlyingType == typeof(float) => "Float32",
            _ when underlyingType == typeof(double) => "Float64",
            _ when underlyingType == typeof(decimal) => "Decimal(18, 4)",
            _ when underlyingType == typeof(string) => "String",
            _ when underlyingType == typeof(DateTime) => "DateTime",
            _ when underlyingType == typeof(DateTimeOffset) => "DateTime",
            _ when underlyingType == typeof(DateOnly) => "Date",
            _ when underlyingType == typeof(TimeOnly) => "String",
            _ when underlyingType == typeof(Guid) => "UUID",
            _ when underlyingType == typeof(byte[]) => "String",
            _ when underlyingType.IsEnum => "Int64",
            _ => throw new NotSupportedException(
                $"CLR type '{clrType.Name}' is not supported for parameterized view parameters. " +
                "Use the Parameter<T>(name, clickHouseType) overload to specify an explicit ClickHouse type.")
        };
    }
}

/// <summary>
/// Expression visitor that generates WHERE clause SQL from LINQ expressions.
/// </summary>
internal sealed class WhereClauseVisitor : ExpressionVisitor
{
    private readonly IEntityType? _sourceEntityType;
    private readonly IReadOnlyDictionary<string, ParameterDefinition> _parameters;
    private readonly StringBuilder _sql = new();

    public WhereClauseVisitor(IEntityType? sourceEntityType, IReadOnlyDictionary<string, ParameterDefinition> parameters)
    {
        _sourceEntityType = sourceEntityType;
        _parameters = parameters;
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
        // Handle property access on source entity: e.PropertyName
        if (node.Member is PropertyInfo propInfo && IsSourceEntityParameter(node))
        {
            var columnName = GetColumnName(propInfo);
            _sql.Append('"');
            _sql.Append(columnName.Replace("\"", "\"\""));
            _sql.Append('"');
            return node;
        }

        throw new NotSupportedException(
            $"Member access '{node}' is not supported. Only source entity properties are allowed.");
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Handle IParameterAccessor.Get<T>("paramName")
        if (node.Method.Name == nameof(IParameterAccessor.Get) &&
            node.Method.DeclaringType == typeof(IParameterAccessor) &&
            node.Arguments.Count == 1 &&
            node.Arguments[0] is ConstantExpression constantExpr &&
            constantExpr.Value is string paramName)
        {
            if (!_parameters.TryGetValue(paramName, out var paramDef))
            {
                throw new InvalidOperationException(
                    $"Parameter '{paramName}' not found. Make sure to call Parameter<T>(\"{paramName}\") first.");
            }

            var clickHouseType = paramDef.ClickHouseType ?? ParameterizedViewSqlGenerator.GetClickHouseType(paramDef.ClrType);
            _sql.Append('{');
            _sql.Append(paramName);
            _sql.Append(':');
            _sql.Append(clickHouseType);
            _sql.Append('}');
            return node;
        }

        throw new NotSupportedException($"Method call '{node.Method.Name}' is not supported in WHERE clause.");
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        var value = node.Value;
        var formatted = FormatConstant(value, node.Type);
        _sql.Append(formatted);
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

        if (node.NodeType == ExpressionType.Convert || node.NodeType == ExpressionType.ConvertChecked)
        {
            // Handle implicit conversions - just visit the operand
            Visit(node.Operand);
            return node;
        }

        throw new NotSupportedException($"Unary operator {node.NodeType} is not supported.");
    }

    private bool IsSourceEntityParameter(MemberExpression node)
    {
        // Check if this is accessing a property on the lambda parameter (source entity)
        // e.g., e.UserId where e is the first parameter
        return node.Expression is ParameterExpression;
    }

    private string GetColumnName(PropertyInfo propInfo)
    {
        if (_sourceEntityType != null)
        {
            var property = _sourceEntityType.FindProperty(propInfo.Name);
            if (property != null)
            {
                var columnName = property.GetColumnName();
                if (columnName != null)
                {
                    return columnName;
                }
            }
        }

        return ParameterizedViewSqlGenerator.ToSnakeCase(propInfo.Name);
    }

    private static string FormatConstant(object? value, Type type)
    {
        return value switch
        {
            null => "NULL",
            string s => $"'{s.Replace("\\", "\\\\").Replace("'", "\\'")}'",
            bool b => b ? "1" : "0",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss}'",
            DateTimeOffset dto => $"'{dto.UtcDateTime:yyyy-MM-dd HH:mm:ss}'",
            DateOnly d => $"'{d:yyyy-MM-dd}'",
            decimal dec => dec.ToString(System.Globalization.CultureInfo.InvariantCulture),
            double dbl => dbl.ToString(System.Globalization.CultureInfo.InvariantCulture),
            float flt => flt.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Guid g => $"'{g}'",
            _ when type.IsValueType => value?.ToString() ?? "NULL",
            _ => $"'{value?.ToString()?.Replace("\\", "\\\\").Replace("'", "\\'") ?? string.Empty}'"
        };
    }
}
