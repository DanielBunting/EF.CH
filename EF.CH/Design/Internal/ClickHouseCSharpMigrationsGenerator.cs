using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Design.Internal;

/// <summary>
/// Custom C# migrations generator that includes ClickHouse extension namespaces.
/// </summary>
public class ClickHouseCSharpMigrationsGenerator : CSharpMigrationsGenerator
{
    public ClickHouseCSharpMigrationsGenerator(
        MigrationsCodeGeneratorDependencies dependencies,
        CSharpMigrationsGeneratorDependencies csharpDependencies)
        : base(dependencies, csharpDependencies)
    {
    }

    /// <summary>
    /// Gets namespaces required for migration operations.
    /// </summary>
    protected override IEnumerable<string> GetNamespaces(IEnumerable<MigrationOperation> operations)
    {
        return base.GetNamespaces(operations)
            .Concat(["EF.CH.Extensions"]);
    }

    /// <summary>
    /// Gets namespaces required for model snapshot.
    /// </summary>
    protected override IEnumerable<string> GetNamespaces(IModel model)
    {
        return base.GetNamespaces(model)
            .Concat(["EF.CH.Extensions"]);
    }
}
