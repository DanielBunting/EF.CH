using System.Collections;
using System.Text;
using EF.CH.Diagnostics;
using EF.CH.Infrastructure;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.Extensions.Logging;

namespace EF.CH.Update.Internal;

/// <summary>
/// Factory for creating ClickHouse modification command batches.
/// </summary>
public class ClickHouseModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ClickHouseDeleteStrategy _deleteStrategy;
    private readonly ILogger<ClickHouseModificationCommandBatchFactory>? _logger;

    public ClickHouseModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource,
        IDbContextOptions options,
        ILogger<ClickHouseModificationCommandBatchFactory>? logger = null)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
        _logger = logger;

        var clickHouseOptions = options.FindExtension<ClickHouseOptionsExtension>();
        _deleteStrategy = clickHouseOptions?.DeleteStrategy ?? ClickHouseDeleteStrategy.Lightweight;
    }

    public ModificationCommandBatch Create()
    {
        return new ClickHouseModificationCommandBatch(_dependencies, _typeMappingSource, _deleteStrategy, _logger);
    }
}

/// <summary>
/// A modification command batch for ClickHouse INSERT and DELETE operations.
/// ClickHouse doesn't support parameterized VALUES in INSERT statements via HTTP,
/// so we generate inline values instead.
/// </summary>
public class ClickHouseModificationCommandBatch : ModificationCommandBatch
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ClickHouseDeleteStrategy _deleteStrategy;
    private readonly ILogger? _logger;
    private readonly List<IReadOnlyModificationCommand> _commands = [];
    private readonly List<string> _statements = [];
    private readonly StringBuilder _sqlBuilder = new();
    private bool _requiresTransaction;
    private bool _areMoreBatchesExpected;

    public ClickHouseModificationCommandBatch(
        ModificationCommandBatchFactoryDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource,
        ClickHouseDeleteStrategy deleteStrategy,
        ILogger? logger = null)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
        _deleteStrategy = deleteStrategy;
        _logger = logger;
    }

    public override IReadOnlyList<IReadOnlyModificationCommand> ModificationCommands => _commands;

    public override bool RequiresTransaction => _requiresTransaction;

    public override bool AreMoreBatchesExpected => _areMoreBatchesExpected;

    public override bool TryAddCommand(IReadOnlyModificationCommand modificationCommand)
    {
        // Support INSERT and DELETE operations
        switch (modificationCommand.EntityState)
        {
            case EntityState.Added:
            case EntityState.Deleted:
                _commands.Add(modificationCommand);
                return true;

            case EntityState.Modified:
                throw ClickHouseUnsupportedOperationException.Update(modificationCommand.TableName);

            default:
                // Unchanged or Detached - shouldn't happen in modification batch
                return false;
        }
    }

    public override void Complete(bool moreBatchesExpected)
    {
        _areMoreBatchesExpected = moreBatchesExpected;
        _statements.Clear();

        // Group inserts by table to use single INSERT with multiple VALUES
        var insertCommands = _commands
            .Where(c => c.EntityState == EntityState.Added)
            .GroupBy(c => (c.TableName, c.Schema))
            .ToList();

        foreach (var tableGroup in insertCommands)
        {
            var commands = tableGroup.ToList();

            // Warn about single-row inserts when this is the final batch
            if (!moreBatchesExpected && commands.Count == 1 && _logger is not null)
            {
                _logger.LogWarning(
                    ClickHouseEventId.SingleRowInsertWarning,
                    "Single-row INSERT to table '{TableName}'. ClickHouse is optimized for batch inserts of 1000+ rows. Consider using AddRange() or batching operations for better performance.",
                    tableGroup.Key.TableName);
            }

            var statement = BuildBulkInsertCommand(commands);
            if (!string.IsNullOrEmpty(statement))
            {
                _statements.Add(statement);
            }
        }

        // Handle DELETE commands (one statement per entity - ClickHouse doesn't support multi-statement)
        var deleteCommands = _commands
            .Where(c => c.EntityState == EntityState.Deleted)
            .ToList();

        foreach (var deleteCommand in deleteCommands)
        {
            var statement = BuildDeleteCommand(deleteCommand);
            _statements.Add(statement);
        }
    }

    private string BuildBulkInsertCommand(List<IReadOnlyModificationCommand> commands)
    {
        if (commands.Count == 0) return string.Empty;

        _sqlBuilder.Clear();
        var sqlHelper = _dependencies.SqlGenerationHelper;
        var firstCommand = commands[0];

        _sqlBuilder.Append("INSERT INTO ");
        _sqlBuilder.Append(sqlHelper.DelimitIdentifier(firstCommand.TableName, firstCommand.Schema));
        _sqlBuilder.Append(" (");

        var columns = firstCommand.ColumnModifications
            .Where(c => c.IsWrite)
            .ToList();

        // Build expanded column list (handles Nested columns)
        var expandedColumns = ExpandColumnsForNested(columns, sqlHelper);

        for (var i = 0; i < expandedColumns.Count; i++)
        {
            if (i > 0) _sqlBuilder.Append(", ");
            _sqlBuilder.Append(expandedColumns[i].QuotedName);
        }

        _sqlBuilder.Append(") VALUES ");

        // Add VALUES for each row
        for (var rowIndex = 0; rowIndex < commands.Count; rowIndex++)
        {
            if (rowIndex > 0) _sqlBuilder.Append(", ");

            _sqlBuilder.Append("(");

            var rowColumns = commands[rowIndex].ColumnModifications
                .Where(c => c.IsWrite)
                .ToList();

            var valueIndex = 0;
            for (var i = 0; i < rowColumns.Count; i++)
            {
                var column = rowColumns[i];

                if (column.TypeMapping is ClickHouseNestedTypeMapping nestedMapping)
                {
                    // Expand Nested column into parallel arrays
                    AppendNestedValues(column, nestedMapping, ref valueIndex);
                }
                else
                {
                    if (valueIndex > 0) _sqlBuilder.Append(", ");
                    AppendValue(column);
                    valueIndex++;
                }
            }

            _sqlBuilder.Append(")");
        }

        return _sqlBuilder.ToString();
    }

    /// <summary>
    /// Expands columns for Nested types into their sub-columns.
    /// For a Nested column "Goals" with fields ID and EventTime, this returns:
    /// "Goals.ID", "Goals.EventTime" instead of just "Goals".
    /// </summary>
    private List<(string QuotedName, IColumnModification? Column, ClickHouseNestedTypeMapping? NestedMapping, int FieldIndex)> ExpandColumnsForNested(
        List<IColumnModification> columns, ISqlGenerationHelper sqlHelper)
    {
        var result = new List<(string, IColumnModification?, ClickHouseNestedTypeMapping?, int)>();

        foreach (var column in columns)
        {
            if (column.TypeMapping is ClickHouseNestedTypeMapping nestedMapping)
            {
                // Expand into sub-columns: "ColumnName.FieldName"
                for (var fieldIndex = 0; fieldIndex < nestedMapping.FieldMappings.Count; fieldIndex++)
                {
                    var field = nestedMapping.FieldMappings[fieldIndex];
                    var subColumnName = $"{column.ColumnName}.{field.Name}";
                    result.Add((sqlHelper.DelimitIdentifier(subColumnName), column, nestedMapping, fieldIndex));
                }
            }
            else
            {
                result.Add((sqlHelper.DelimitIdentifier(column.ColumnName), column, null, -1));
            }
        }

        return result;
    }

    /// <summary>
    /// Appends values for a Nested column as parallel arrays.
    /// For a Nested(ID UInt32, EventTime DateTime) column with value [{ID:1, EventTime:t1}, {ID:2, EventTime:t2}],
    /// this outputs: [1, 2], ['t1', 't2']
    /// </summary>
    private void AppendNestedValues(IColumnModification column, ClickHouseNestedTypeMapping nestedMapping, ref int valueIndex)
    {
        var value = column.Value;

        // Collect all items from the collection
        var items = new List<object>();
        if (value is not null)
        {
            foreach (var item in (IEnumerable)value)
            {
                if (item is not null)
                {
                    items.Add(item);
                }
            }
        }

        // Generate an array for each field in the Nested type
        for (var fieldIndex = 0; fieldIndex < nestedMapping.FieldMappings.Count; fieldIndex++)
        {
            if (valueIndex > 0) _sqlBuilder.Append(", ");

            var (_, property, fieldMapping) = nestedMapping.FieldMappings[fieldIndex];

            _sqlBuilder.Append('[');

            for (var itemIndex = 0; itemIndex < items.Count; itemIndex++)
            {
                if (itemIndex > 0) _sqlBuilder.Append(", ");

                var fieldValue = property.GetValue(items[itemIndex]);
                if (fieldValue is null)
                {
                    _sqlBuilder.Append("NULL");
                }
                else
                {
                    _sqlBuilder.Append(fieldMapping.GenerateSqlLiteral(fieldValue));
                }
            }

            _sqlBuilder.Append(']');
            valueIndex++;
        }
    }

    private string BuildDeleteCommand(IReadOnlyModificationCommand command)
    {
        _sqlBuilder.Clear();
        var sqlHelper = _dependencies.SqlGenerationHelper;

        if (_deleteStrategy == ClickHouseDeleteStrategy.Mutation)
        {
            // Mutation-based delete: ALTER TABLE "Table" DELETE WHERE ...
            _sqlBuilder.Append("ALTER TABLE ");
            _sqlBuilder.Append(sqlHelper.DelimitIdentifier(command.TableName, command.Schema));
            _sqlBuilder.Append(" DELETE WHERE ");
        }
        else
        {
            // Lightweight delete (default): DELETE FROM "Table" WHERE ...
            _sqlBuilder.Append("DELETE FROM ");
            _sqlBuilder.Append(sqlHelper.DelimitIdentifier(command.TableName, command.Schema));
            _sqlBuilder.Append(" WHERE ");
        }

        AppendWhereClause(command);
        return _sqlBuilder.ToString();
    }

    private void AppendWhereClause(IReadOnlyModificationCommand command)
    {
        var sqlHelper = _dependencies.SqlGenerationHelper;
        var keyColumns = command.ColumnModifications
            .Where(c => c.IsKey)
            .ToList();

        if (keyColumns.Count == 0)
        {
            throw new InvalidOperationException(
                $"Cannot generate DELETE for entity '{command.TableName}' without key columns. " +
                $"Ensure the entity has a primary key defined.");
        }

        for (var i = 0; i < keyColumns.Count; i++)
        {
            if (i > 0) _sqlBuilder.Append(" AND ");

            _sqlBuilder.Append(sqlHelper.DelimitIdentifier(keyColumns[i].ColumnName));

            var value = keyColumns[i].OriginalValue;
            if (value is null)
            {
                _sqlBuilder.Append(" IS NULL");
            }
            else
            {
                _sqlBuilder.Append(" = ");
                AppendKeyValue(keyColumns[i]);
            }
        }
    }

    private void AppendKeyValue(IColumnModification column)
    {
        // Use OriginalValue for DELETE (the value before modification)
        var value = column.OriginalValue;

        if (value is null)
        {
            _sqlBuilder.Append("NULL");
            return;
        }

        var typeMapping = column.TypeMapping
            ?? _typeMappingSource.FindMapping(value.GetType());

        if (typeMapping is not null)
        {
            _sqlBuilder.Append(typeMapping.GenerateSqlLiteral(value));
        }
        else
        {
            AppendFallbackValue(value);
        }
    }

    private void AppendValue(IColumnModification column)
    {
        var value = column.Value;

        if (value is null)
        {
            _sqlBuilder.Append("NULL");
            return;
        }

        // Get the type mapping to generate proper SQL literal
        var typeMapping = column.TypeMapping
            ?? _typeMappingSource.FindMapping(value.GetType());

        if (typeMapping is not null)
        {
            _sqlBuilder.Append(typeMapping.GenerateSqlLiteral(value));
        }
        else
        {
            // Fallback for unknown types
            AppendFallbackValue(value);
        }
    }

    private void AppendFallbackValue(object value)
    {
        switch (value)
        {
            case string s:
                _sqlBuilder.Append('\'');
                _sqlBuilder.Append(s.Replace("\\", "\\\\").Replace("'", "\\'"));
                _sqlBuilder.Append('\'');
                break;
            case bool b:
                _sqlBuilder.Append(b ? "true" : "false");
                break;
            case DateTime dt:
                _sqlBuilder.Append('\'');
                _sqlBuilder.Append(dt.ToString("yyyy-MM-dd HH:mm:ss.fff"));
                _sqlBuilder.Append('\'');
                break;
            case Guid g:
                _sqlBuilder.Append('\'');
                _sqlBuilder.Append(g.ToString());
                _sqlBuilder.Append('\'');
                break;
            default:
                _sqlBuilder.Append(value);
                break;
        }
    }

    public override void Execute(IRelationalConnection connection)
    {
        if (_statements.Count == 0) return;

        connection.Open();
        try
        {
            foreach (var sql in _statements)
            {
                using var command = connection.DbConnection.CreateCommand();
                command.CommandText = sql;
                command.ExecuteNonQuery();
            }
        }
        finally
        {
            connection.Close();
        }
    }

    public override async Task ExecuteAsync(
        IRelationalConnection connection,
        CancellationToken cancellationToken = default)
    {
        if (_statements.Count == 0) return;

        await connection.OpenAsync(cancellationToken);
        try
        {
            foreach (var sql in _statements)
            {
                await using var command = connection.DbConnection.CreateCommand();
                command.CommandText = sql;
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        finally
        {
            await connection.CloseAsync();
        }
    }
}
