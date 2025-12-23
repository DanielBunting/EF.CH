using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Internal;

namespace EF.CH.Design.Internal;

/// <summary>
/// Custom model code generator that generates C# enum types from ClickHouse enum columns.
/// Extends the default CSharpModelGenerator and adds enum files to AdditionalFiles.
/// </summary>
#pragma warning disable EF1001 // Internal EF Core API usage
public class ClickHouseCSharpModelGenerator : CSharpModelGenerator
{
    private readonly ClickHouseEnumCodeGenerator _enumGenerator = new();

    public ClickHouseCSharpModelGenerator(
        ModelCodeGeneratorDependencies dependencies,
        IOperationReporter reporter,
        IServiceProvider serviceProvider)
        : base(dependencies, reporter, serviceProvider)
    {
    }

    /// <summary>
    /// Generates the scaffolded model, including enum files for ClickHouse enum columns.
    /// </summary>
    public override ScaffoldedModel GenerateModel(IModel model, ModelCodeGenerationOptions options)
    {
        var scaffoldedModel = base.GenerateModel(model, options);

        // Collect unique enum definitions from all properties
        var enumsToGenerate = CollectEnumDefinitions(model);
        var targetNamespace = options.ModelNamespace ?? options.ContextNamespace ?? "Generated";

        foreach (var (enumTypeName, enumDefinition) in enumsToGenerate)
        {
            var enumCode = _enumGenerator.GenerateEnumCode(enumTypeName, enumDefinition, targetNamespace);
            if (!string.IsNullOrEmpty(enumCode))
            {
                scaffoldedModel.AdditionalFiles.Add(new ScaffoldedFile { Path = $"{enumTypeName}.cs", Code = enumCode });
            }
        }

        return scaffoldedModel;
    }

    private static Dictionary<string, string> CollectEnumDefinitions(IModel model)
    {
        var enums = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var entityType in model.GetEntityTypes())
        {
            foreach (var property in entityType.GetProperties())
            {
                var enumTypeName = property.FindAnnotation(ClickHouseAnnotationNames.EnumTypeName)?.Value as string;
                var enumDefinition = property.FindAnnotation(ClickHouseAnnotationNames.EnumDefinition)?.Value as string;

                if (!string.IsNullOrEmpty(enumTypeName) && !string.IsNullOrEmpty(enumDefinition))
                {
                    // Use TryAdd to avoid duplicates when same enum is used in multiple columns
                    enums.TryAdd(enumTypeName, enumDefinition);
                }
            }
        }

        return enums;
    }
}
#pragma warning restore EF1001
