using EF.CH.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Design;

/// <summary>
/// Custom migrations scaffolder that splits migrations into individual step files for ClickHouse.
/// Each operation becomes its own migration to enable atomic execution and resume after failure.
/// </summary>
public class ClickHouseMigrationsScaffolder : MigrationsScaffolder
{
    private readonly ClickHouseMigrationsSplitter _splitter;
    private readonly IMigrationsCodeGenerator _codeGenerator;
    private readonly IMigrationsModelDiffer _modelDiffer;
    private readonly IModel _model;
    private readonly Type _contextType;

    // Store step migrations during scaffolding for later file writing
    private readonly List<StepMigrationData> _pendingStepMigrations = [];

    public ClickHouseMigrationsScaffolder(
        MigrationsScaffolderDependencies dependencies,
        ClickHouseMigrationsSplitter splitter)
        : base(dependencies)
    {
        _splitter = splitter;
        _codeGenerator = dependencies.MigrationsCodeGeneratorSelector.Select(null);
        _modelDiffer = dependencies.MigrationsModelDiffer;
        _model = dependencies.Model;
        _contextType = dependencies.CurrentContext.Context.GetType();
    }

    /// <summary>
    /// Scaffolds a migration, splitting it into individual step files.
    /// </summary>
    public override ScaffoldedMigration ScaffoldMigration(
        string migrationName,
        string? rootNamespace,
        string? subNamespace = null,
        string? language = null)
    {
        // Clear any pending steps from previous calls
        _pendingStepMigrations.Clear();

        // Get the base migration to extract operations
        var baseMigration = base.ScaffoldMigration(migrationName, rootNamespace, subNamespace, language);

        // Parse operations from the model differ
        var upOperations = GetUpOperations();

        // If only one operation, return the standard migration (no splitting needed)
        if (upOperations.Count <= 1)
        {
            return baseMigration;
        }

        // Split operations into steps
        var steps = _splitter.Split(upOperations, _model);
        var totalSteps = steps.Count;

        // Generate step migrations
        var migrationNamespace = GetMigrationNamespace(rootNamespace, subNamespace);
        var baseId = baseMigration.MigrationId; // e.g., "20250107100000_AddOrders"

        for (var i = 0; i < steps.Count; i++)
        {
            var step = steps[i];
            var stepSuffix = step.StepSuffix; // "001", "002", etc.
            var stepMigrationId = $"{baseId}_{stepSuffix}";
            var stepClassName = $"{migrationName}_{stepSuffix}";

            // Generate Up method code for this step only
            var stepUpCode = _codeGenerator.GenerateMigration(
                migrationNamespace,
                stepClassName,
                [step.Operation],
                []); // Empty down operations - we'll generate our own

            // Generate modified migration code with forward-only Down
            var stepMigrationCode = GenerateStepMigrationCode(
                migrationNamespace,
                stepClassName,
                stepMigrationId,
                step,
                i + 1,
                totalSteps);

            // Generate metadata (Designer.cs)
            var stepMetadataCode = _codeGenerator.GenerateMetadata(
                migrationNamespace,
                _contextType,
                stepClassName,
                stepMigrationId,
                _model);

            _pendingStepMigrations.Add(new StepMigrationData(
                StepIndex: i,
                MigrationId: stepMigrationId,
                ClassName: stepClassName,
                MigrationCode: stepMigrationCode,
                MetadataCode: stepMetadataCode,
                OperationDescription: step.OperationDescription));
        }

        // Return the first step as the primary migration (EF Core expects one ScaffoldedMigration)
        // The Save method will write all steps
        var firstStep = _pendingStepMigrations[0];
        return new ScaffoldedMigration(
            _codeGenerator.FileExtension,
            baseMigration.PreviousMigrationId,
            firstStep.MigrationCode,
            firstStep.MigrationId,
            firstStep.MetadataCode,
            baseMigration.MigrationSubNamespace,
            baseMigration.SnapshotCode,
            baseMigration.SnapshotName,
            baseMigration.SnapshotSubnamespace);
    }

    /// <summary>
    /// Saves the scaffolded migration to files, including all step files.
    /// </summary>
    public override MigrationFiles Save(
        string projectDir,
        ScaffoldedMigration migration,
        string? outputDir)
    {
        // If we have pending step migrations, write them all
        if (_pendingStepMigrations.Count > 1)
        {
            var result = new MigrationFiles();
            var directory = Path.Combine(projectDir, outputDir ?? "Migrations");

            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            // Write each step migration
            foreach (var step in _pendingStepMigrations)
            {
                var migrationFile = Path.Combine(directory, $"{step.MigrationId}{_codeGenerator.FileExtension}");
                var metadataFile = Path.Combine(directory, $"{step.MigrationId}.Designer{_codeGenerator.FileExtension}");

                File.WriteAllText(migrationFile, step.MigrationCode);
                File.WriteAllText(metadataFile, step.MetadataCode);

                result.MigrationFile = migrationFile; // Last one wins for compatibility
                result.MetadataFile = metadataFile;
            }

            // Write the snapshot (shared by all steps)
            if (!string.IsNullOrEmpty(migration.SnapshotCode))
            {
                var snapshotFile = Path.Combine(directory, $"{migration.SnapshotName}{_codeGenerator.FileExtension}");
                File.WriteAllText(snapshotFile, migration.SnapshotCode);
                result.SnapshotFile = snapshotFile;
            }

            _pendingStepMigrations.Clear();
            return result;
        }

        // Standard behavior for single-operation migrations
        return base.Save(projectDir, migration, outputDir);
    }

    /// <summary>
    /// Gets the up operations by comparing the current model with the snapshot.
    /// Uses the same approach as the base MigrationsScaffolder.
    /// </summary>
    private IReadOnlyList<MigrationOperation> GetUpOperations()
    {
        // Get the model snapshot if it exists
        var snapshotModel = Dependencies.MigrationsAssembly.ModelSnapshot?.Model;

        // Use the snapshot model processor to get the processed model
        IModel? sourceModel = null;
        if (snapshotModel != null)
        {
#pragma warning disable EF1001 // Internal API usage
            sourceModel = Dependencies.SnapshotModelProcessor.Process(snapshotModel);
#pragma warning restore EF1001
        }

        // Get the operations by diffing models
        return _modelDiffer.GetDifferences(sourceModel?.GetRelationalModel(), _model.GetRelationalModel());
    }

    /// <summary>
    /// Generates the full migration class code for a step, with forward-only Down method.
    /// </summary>
    private string GenerateStepMigrationCode(
        string migrationNamespace,
        string className,
        string migrationId,
        StepMigration step,
        int stepNumber,
        int totalSteps)
    {
        // Generate the Up method body using the code generator
        var upMethodCode = _codeGenerator.GenerateMigration(
            migrationNamespace,
            className,
            [step.Operation],
            []);

        // The code generator returns a full class - we need to modify it to use our Down method
        // This is a bit of a hack, but necessary to inject our forward-only Down
        var modifiedCode = upMethodCode.Replace(
            "protected override void Down(MigrationBuilder migrationBuilder)\n        {\n        }",
            $"protected override void Down(MigrationBuilder migrationBuilder)\n        {{\n            throw new ClickHouseDownMigrationNotSupportedException(\"{migrationId}\");\n        }}");

        // Also replace empty Down that might have different formatting
        modifiedCode = modifiedCode.Replace(
            "protected override void Down(MigrationBuilder migrationBuilder)\r\n        {\r\n        }",
            $"protected override void Down(MigrationBuilder migrationBuilder)\r\n        {{\r\n            throw new ClickHouseDownMigrationNotSupportedException(\"{migrationId}\");\r\n        }}");

        // Add using statement for the exception if not present
        if (!modifiedCode.Contains("using EF.CH.Infrastructure;"))
        {
            modifiedCode = modifiedCode.Replace(
                "using Microsoft.EntityFrameworkCore.Migrations;",
                "using EF.CH.Infrastructure;\nusing Microsoft.EntityFrameworkCore.Migrations;");
        }

        // Add step comment at the top
        var stepComment = $"// Step {stepNumber} of {totalSteps}: {step.OperationDescription}";
        modifiedCode = modifiedCode.Replace(
            "// <auto-generated />",
            $"// <auto-generated />\n{stepComment}");

        return modifiedCode;
    }

    private static string GetMigrationNamespace(string? rootNamespace, string? subNamespace)
    {
        if (string.IsNullOrEmpty(rootNamespace))
            return subNamespace ?? "Migrations";

        if (string.IsNullOrEmpty(subNamespace))
            return rootNamespace;

        return $"{rootNamespace}.{subNamespace}";
    }

    /// <summary>
    /// Internal record for storing step migration data during scaffolding.
    /// </summary>
    private sealed record StepMigrationData(
        int StepIndex,
        string MigrationId,
        string ClassName,
        string MigrationCode,
        string MetadataCode,
        string OperationDescription);
}
