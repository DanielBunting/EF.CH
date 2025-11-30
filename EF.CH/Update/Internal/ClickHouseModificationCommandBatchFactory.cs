using System.Text;
using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
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

    public ClickHouseModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource,
        IDbContextOptions options)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;

        var clickHouseOptions = options.FindExtension<ClickHouseOptionsExtension>();
        _deleteStrategy = clickHouseOptions?.DeleteStrategy ?? ClickHouseDeleteStrategy.Lightweight;
    }

    public ModificationCommandBatch Create()
    {
        return new ClickHouseModificationCommandBatch(_dependencies, _typeMappingSource, _deleteStrategy);
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
    private readonly List<IReadOnlyModificationCommand> _commands = [];
    private readonly List<string> _statements = [];
    private readonly StringBuilder _sqlBuilder = new();
    private bool _requiresTransaction;
    private bool _areMoreBatchesExpected;

    public ClickHouseModificationCommandBatch(
        ModificationCommandBatchFactoryDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource,
        ClickHouseDeleteStrategy deleteStrategy)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
        _deleteStrategy = deleteStrategy;
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
                throw new NotSupportedException(
                    $"ClickHouse does not support efficient row-level UPDATE operations. " +
                    $"Consider using INSERT with ReplacingMergeTree engine for updates, " +
                    $"or delete and re-insert the entity. Table: '{modificationCommand.TableName}'.");

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
            .GroupBy(c => (c.TableName, c.Schema));

        foreach (var tableGroup in insertCommands)
        {
            var statement = BuildBulkInsertCommand(tableGroup.ToList());
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
