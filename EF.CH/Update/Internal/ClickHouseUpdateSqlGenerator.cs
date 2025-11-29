using System.Text;
using Microsoft.EntityFrameworkCore.Update;

namespace EF.CH.Update.Internal;

/// <summary>
/// Generates SQL for INSERT operations in ClickHouse.
/// Note: ClickHouse doesn't support traditional UPDATE/DELETE - only INSERT.
/// </summary>
public class ClickHouseUpdateSqlGenerator : UpdateSqlGenerator
{
    public ClickHouseUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    // ClickHouse-specific SQL generation can be customized here
}
