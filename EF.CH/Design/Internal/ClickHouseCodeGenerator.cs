using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;

namespace EF.CH.Design.Internal;

/// <summary>
/// Generates provider-specific configuration code for scaffolded DbContext.
/// </summary>
public class ClickHouseCodeGenerator : ProviderCodeGenerator
{
    private static readonly MethodCallCodeFragment UseClickHouseMethodCall
        = new("UseClickHouse");

    public ClickHouseCodeGenerator(ProviderCodeGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    /// <summary>
    /// Generates the UseClickHouse method call for the OnConfiguring method.
    /// </summary>
    public override MethodCallCodeFragment GenerateUseProvider(
        string connectionString,
        MethodCallCodeFragment? providerOptions)
    {
        return new MethodCallCodeFragment(
            "UseClickHouse",
            providerOptions == null
                ? [connectionString]
                : [connectionString, new NestedClosureCodeFragment("x", providerOptions)]);
    }

    /// <summary>
    /// Generates the UseClickHouse method call with context options.
    /// </summary>
    public override MethodCallCodeFragment GenerateContextOptions()
    {
        return UseClickHouseMethodCall;
    }
}
