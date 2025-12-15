using System.Collections;
using System.Text;
using EF.CH.External;
using EF.CH.Infrastructure;
using EF.CH.Metadata;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace EF.CH.Update.Internal;

/// <summary>
/// Factory for creating ClickHouse modification command batches.
/// </summary>
public class ClickHouseModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ClickHouseDeleteStrategy _deleteStrategy;
    private readonly IExternalConfigResolver _externalConfigResolver;
    private readonly ICurrentDbContext _currentDbContext;

    public ClickHouseModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource,
        IDbContextOptions options,
        IExternalConfigResolver externalConfigResolver,
        ICurrentDbContext currentDbContext)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
        _externalConfigResolver = externalConfigResolver;
        _currentDbContext = currentDbContext;

        var clickHouseOptions = options.FindExtension<ClickHouseOptionsExtension>();
        _deleteStrategy = clickHouseOptions?.DeleteStrategy ?? ClickHouseDeleteStrategy.Lightweight;
    }

    public ModificationCommandBatch Create()
    {
        return new ClickHouseModificationCommandBatch(
            _dependencies,
            _typeMappingSource,
            _deleteStrategy,
            _externalConfigResolver,
            _currentDbContext.Context.Model);
    }
}

/// <summary>
/// A modification command batch for ClickHouse INSERT and DELETE operations.
/// ClickHouse doesn't support parameterized VALUES in INSERT statements via HTTP,
/// so we generate inline values instead.
/// Supports INSERT INTO FUNCTION for external entities.
/// </summary>
public class ClickHouseModificationCommandBatch : ModificationCommandBatch
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ClickHouseDeleteStrategy _deleteStrategy;
    private readonly IExternalConfigResolver _externalConfigResolver;
    private readonly IModel _model;
    private readonly List<IReadOnlyModificationCommand> _commands = [];
    private readonly List<string> _statements = [];
    private readonly StringBuilder _sqlBuilder = new();
    private bool _requiresTransaction;
    private bool _areMoreBatchesExpected;

    public ClickHouseModificationCommandBatch(
        ModificationCommandBatchFactoryDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource,
        ClickHouseDeleteStrategy deleteStrategy,
        IExternalConfigResolver externalConfigResolver,
        IModel model)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
        _deleteStrategy = deleteStrategy;
        _externalConfigResolver = externalConfigResolver;
        _model = model;
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

        // Check if this is an external entity
        var entityType = FindEntityTypeByTableName(firstCommand.TableName, firstCommand.Schema);
        if (entityType != null && _externalConfigResolver.IsExternalTableFunction(entityType))
        {
            // External entity - use INSERT INTO FUNCTION
            return BuildExternalInsertCommand(commands, entityType);
        }

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
    /// Builds an INSERT INTO FUNCTION statement for external entities.
    /// </summary>
    private string BuildExternalInsertCommand(List<IReadOnlyModificationCommand> commands, IEntityType entityType)
    {
        // Check if inserts are enabled for this external entity
        if (!_externalConfigResolver.AreInsertsEnabled(entityType))
        {
            throw new InvalidOperationException(
                $"External entity '{entityType.ClrType.Name}' is read-only. " +
                "Call AllowInserts() in configuration to enable INSERT operations.");
        }

        _sqlBuilder.Clear();
        var sqlHelper = _dependencies.SqlGenerationHelper;
        var firstCommand = commands[0];

        // Get the table function call
        var functionCall = _externalConfigResolver.ResolvePostgresTableFunction(entityType);

        _sqlBuilder.Append("INSERT INTO FUNCTION ");
        _sqlBuilder.Append(functionCall);
        _sqlBuilder.Append(" (");

        var columns = firstCommand.ColumnModifications
            .Where(c => c.IsWrite)
            .ToList();

        // Build column list (external entities don't support Nested types)
        for (var i = 0; i < columns.Count; i++)
        {
            if (i > 0) _sqlBuilder.Append(", ");
            _sqlBuilder.Append(sqlHelper.DelimitIdentifier(columns[i].ColumnName));
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

            for (var i = 0; i < rowColumns.Count; i++)
            {
                if (i > 0) _sqlBuilder.Append(", ");
                AppendValue(rowColumns[i]);
            }

            _sqlBuilder.Append(")");
        }

        return _sqlBuilder.ToString();
    }

    /// <summary>
    /// Finds an entity type by its table name and schema.
    /// </summary>
    private IEntityType? FindEntityTypeByTableName(string tableName, string? schema)
    {
        foreach (var entityType in _model.GetEntityTypes())
        {
            var entityTableName = entityType.GetTableName();
            var entitySchema = entityType.GetSchema() ?? _model.GetDefaultSchema();

            if (entityTableName == tableName && entitySchema == schema)
            {
                return entityType;
            }
        }

        return null;
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
