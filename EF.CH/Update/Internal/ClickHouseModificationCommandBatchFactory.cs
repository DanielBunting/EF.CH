using System.Text;
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

    public ClickHouseModificationCommandBatchFactory(
        ModificationCommandBatchFactoryDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
    }

    public ModificationCommandBatch Create()
    {
        return new ClickHouseModificationCommandBatch(_dependencies, _typeMappingSource);
    }
}

/// <summary>
/// A modification command batch for ClickHouse INSERT operations.
/// ClickHouse doesn't support parameterized VALUES in INSERT statements via HTTP,
/// so we generate inline values instead.
/// </summary>
public class ClickHouseModificationCommandBatch : ModificationCommandBatch
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly List<IReadOnlyModificationCommand> _commands = [];
    private readonly StringBuilder _sqlBuilder = new();
    private bool _requiresTransaction;
    private bool _areMoreBatchesExpected;

    public ClickHouseModificationCommandBatch(
        ModificationCommandBatchFactoryDependencies dependencies,
        IRelationalTypeMappingSource typeMappingSource)
    {
        _dependencies = dependencies;
        _typeMappingSource = typeMappingSource;
    }

    public override IReadOnlyList<IReadOnlyModificationCommand> ModificationCommands => _commands;

    public override bool RequiresTransaction => _requiresTransaction;

    public override bool AreMoreBatchesExpected => _areMoreBatchesExpected;

    public override bool TryAddCommand(IReadOnlyModificationCommand modificationCommand)
    {
        // Only support INSERT operations
        if (modificationCommand.EntityState != Microsoft.EntityFrameworkCore.EntityState.Added)
        {
            // ClickHouse doesn't support traditional UPDATE/DELETE
            // For now, we'll allow the command but it may fail at execution
        }

        _commands.Add(modificationCommand);
        return true;
    }

    public override void Complete(bool moreBatchesExpected)
    {
        _areMoreBatchesExpected = moreBatchesExpected;

        // Build the SQL with inline values
        _sqlBuilder.Clear();

        // Group inserts by table to use single INSERT with multiple VALUES
        var insertCommands = _commands
            .Where(c => c.EntityState == Microsoft.EntityFrameworkCore.EntityState.Added)
            .GroupBy(c => (c.TableName, c.Schema));

        foreach (var tableGroup in insertCommands)
        {
            AppendBulkInsertCommand(tableGroup.ToList());
        }
    }

    private void AppendBulkInsertCommand(List<IReadOnlyModificationCommand> commands)
    {
        if (commands.Count == 0) return;

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

        _sqlBuilder.AppendLine(";");
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
        if (_commands.Count == 0) return;

        var sql = _sqlBuilder.ToString();
        if (string.IsNullOrEmpty(sql)) return;

        using var command = connection.DbConnection.CreateCommand();
        command.CommandText = sql;

        connection.Open();
        try
        {
            command.ExecuteNonQuery();
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
        if (_commands.Count == 0) return;

        var sql = _sqlBuilder.ToString();
        if (string.IsNullOrEmpty(sql)) return;

        await using var command = connection.DbConnection.CreateCommand();
        command.CommandText = sql;

        await connection.OpenAsync(cancellationToken);
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            await connection.CloseAsync();
        }
    }
}
