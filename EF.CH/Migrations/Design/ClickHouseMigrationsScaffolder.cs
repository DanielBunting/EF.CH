using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using EF.CH.Infrastructure;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Design;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EF.CH.Migrations.Design;

/// <summary>
/// Data for a pending step migration that will be written by Save().
/// </summary>
internal sealed record StepMigrationData(
    string MigrationId,
    string MigrationName,
    string MigrationCode,
    string MigrationMetadataCode,
    string StepComment);

/// <summary>
/// Custom migrations scaffolder that splits migrations into individual step files.
/// This enables recovery from partial migration failures in ClickHouse, which lacks transactions.
/// </summary>
public class ClickHouseMigrationsScaffolder : MigrationsScaffolder
{
    private readonly ClickHouseMigrationsSplitter _splitter;
    private readonly IMigrationsCodeGeneratorSelector _codeGeneratorSelector;
    private readonly ICurrentDbContext _currentContext;
    private readonly IMigrationsIdGenerator _idGenerator;
    private readonly IMigrationsAssembly _migrationsAssembly;
    private readonly IMigrationsModelDiffer _modelDiffer;
    private readonly IModel _model;

    private readonly List<StepMigrationData> _pendingStepMigrations = [];
    private string? _sharedSnapshotCode;

#pragma warning disable EF1001 // Internal EF Core API usage
    public ClickHouseMigrationsScaffolder(
        MigrationsScaffolderDependencies dependencies,
        ClickHouseMigrationsSplitter splitter)
        : base(dependencies)
    {
        _splitter = splitter;
        _codeGeneratorSelector = dependencies.MigrationsCodeGeneratorSelector;
        _currentContext = dependencies.CurrentContext;
        _idGenerator = dependencies.MigrationsIdGenerator;
        _migrationsAssembly = dependencies.MigrationsAssembly;
        _modelDiffer = dependencies.MigrationsModelDiffer;
        _model = dependencies.SnapshotModelProcessor.Process(
            dependencies.Model.GetRelationalModel().Model);
    }
#pragma warning restore EF1001

    /// <summary>
    /// Scaffolds a migration, splitting multi-operation migrations into individual step files.
    /// </summary>
    [SuppressMessage("Usage", "EF1001:Internal EF Core API usage")]
    public override ScaffoldedMigration ScaffoldMigration(
        string migrationName,
        string? rootNamespace,
        string? subNamespace = null,
        string? language = null)
    {
        // Clear any previous pending migrations
        _pendingStepMigrations.Clear();
        _sharedSnapshotCode = null;

        // Get migration operations using model differ
        var operations = GetMigrationOperations();

        // If single operation or empty, use standard scaffolding
        if (operations.Count <= 1)
        {
            return base.ScaffoldMigration(migrationName, rootNamespace, subNamespace, language);
        }

        // Get the code generator
        var codeGenerator = _codeGeneratorSelector.Select(language);

        // Split operations into steps
        var steps = _splitter.Split(operations, _model);

        // Get the base timestamp for all step migrations
        var baseId = _idGenerator.GenerateId(migrationName);
        var timestamp = baseId.Split('_')[0]; // e.g., "20250107100000"

        // Generate the shared model snapshot (only once for all steps)
        var contextType = _currentContext.Context.GetType();
        var finalSubNamespace = subNamespace ?? "Migrations";
        var snapshotName = $"{contextType.Name}ModelSnapshot";

        _sharedSnapshotCode = codeGenerator.GenerateSnapshot(
            finalSubNamespace,
            contextType,
            snapshotName,
            _model);

        // Get previous migration ID
        var previousMigrationId = _migrationsAssembly.Migrations.LastOrDefault().Key;

        // Generate each step migration
        ScaffoldedMigration? firstStep = null;

        for (int i = 0; i < steps.Count; i++)
        {
            var step = steps[i];
            var stepId = $"{timestamp}_{migrationName}_{step.StepSuffix}";
            var stepClassName = $"_{stepId}"; // Underscore prefix for valid C# class name
            var stepComment = $"// Step {step.StepNumber} of {steps.Count}: {step.OperationDescription}";

            // Generate Up code for this step's single operation
            var upOperations = new List<MigrationOperation> { step.Operation };

            var migrationCode = GenerateStepMigrationCode(
                codeGenerator,
                stepId,
                stepClassName,
                upOperations,
                finalSubNamespace,
                contextType,
                stepComment);

            var metadataCode = codeGenerator.GenerateMetadata(
                finalSubNamespace,
                contextType,
                stepClassName,
                stepId,
                _model);

            if (i == 0)
            {
                // Return the first step as the primary scaffolded migration
                firstStep = new ScaffoldedMigration(
                    codeGenerator.FileExtension,
                    previousMigrationId,
                    migrationCode,
                    stepId,
                    metadataCode,
                    finalSubNamespace,
                    _sharedSnapshotCode,
                    snapshotName,
                    finalSubNamespace);
            }
            else
            {
                // Store additional steps for writing in Save()
                _pendingStepMigrations.Add(new StepMigrationData(
                    stepId,
                    stepClassName,
                    migrationCode,
                    metadataCode,
                    stepComment));
            }

            // Update previousMigrationId for next step
            previousMigrationId = stepId;
        }

        return firstStep!;
    }

    /// <summary>
    /// Saves the scaffolded migration and any additional step files.
    /// </summary>
    public override MigrationFiles Save(
        string projectDir,
        ScaffoldedMigration migration,
        string? outputDir)
    {
        // Save the primary (first step) migration using base implementation
        var files = base.Save(projectDir, migration, outputDir);

        // If we have pending step migrations, save them too
        if (_pendingStepMigrations.Count > 0)
        {
            var migrationsDir = Path.GetDirectoryName(files.MigrationFile)
                ?? Path.Combine(projectDir, outputDir ?? "Migrations");

            foreach (var step in _pendingStepMigrations)
            {
                var migrationFile = Path.Combine(migrationsDir, $"{step.MigrationId}.cs");
                var metadataFile = Path.Combine(migrationsDir, $"{step.MigrationId}.Designer.cs");

                File.WriteAllText(migrationFile, step.MigrationCode);
                File.WriteAllText(metadataFile, step.MigrationMetadataCode);
            }
        }

        _pendingStepMigrations.Clear();

        _sharedSnapshotCode = null;
        return files;
    }

    /// <summary>
    /// Gets the migration operations by diffing the model.
    /// Also enriches DropTableOperation with annotations from the previous model
    /// for proper phase ordering of MVs and dictionaries.
    /// </summary>
    [SuppressMessage("Usage", "EF1001:Internal EF Core API usage")]
    private IReadOnlyList<MigrationOperation> GetMigrationOperations()
    {
        var lastMigration = _migrationsAssembly.Migrations.LastOrDefault();
        IRelationalModel? lastModel = null;

        if (lastMigration.Value != null)
        {
            // Use reflection to get the target model from the last migration's metadata
            var metadataType = lastMigration.Value
                .GetCustomAttribute<DbContextAttribute>()?.ContextType
                ?.Assembly.GetTypes()
                .FirstOrDefault(t => t.Name.EndsWith("ModelSnapshot"));

            if (metadataType != null)
            {
                var snapshot = (ModelSnapshot?)Activator.CreateInstance(metadataType);
                lastModel = snapshot?.Model.GetRelationalModel();
            }
        }

        var operations = _modelDiffer.GetDifferences(lastModel, _model.GetRelationalModel());

        // Enrich CreateTableOperations with annotations from current model
        // so the splitter can distinguish MVs/dicts from regular tables
        operations = EnrichCreateOperationsWithAnnotations(operations, _model);

        // Enrich DropTableOperations with annotations from previous model
        // so the splitter can distinguish MVs/dicts from regular tables
        return EnrichDropOperationsWithAnnotations(operations, lastModel);
    }

    /// <summary>
    /// Enriches CreateTableOperation with annotations from the current model.
    /// This allows the splitter to correctly classify MV and dictionary creates.
    /// </summary>
    private static IReadOnlyList<MigrationOperation> EnrichCreateOperationsWithAnnotations(
        IReadOnlyList<MigrationOperation> operations,
        IModel model)
    {
        foreach (var op in operations)
        {
            if (op is CreateTableOperation createOp)
            {
                // Find the entity type for this table
                var entityType = model.GetEntityTypes()
                    .FirstOrDefault(e => string.Equals(e.GetTableName(), createOp.Name, StringComparison.OrdinalIgnoreCase)
                        && string.Equals(e.GetSchema() ?? model.GetDefaultSchema(), createOp.Schema, StringComparison.OrdinalIgnoreCase));

                if (entityType == null)
                    continue;

                // Copy MaterializedView annotations
                var isMv = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedView)?.Value;
                if (isMv is true)
                {
                    createOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

                    var mvSource = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewSource)?.Value;
                    if (mvSource != null)
                        createOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, mvSource);

                    var mvQuery = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery)?.Value;
                    if (mvQuery != null)
                        createOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, mvQuery);

                    var mvPopulate = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate)?.Value;
                    if (mvPopulate != null)
                        createOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewPopulate, mvPopulate);
                }

                // Copy Dictionary annotations
                var isDict = entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value;
                if (isDict is true)
                {
                    createOp.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);

                    var dictSource = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySource)?.Value;
                    if (dictSource != null)
                        createOp.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, dictSource);

                    var dictProvider = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySourceProvider)?.Value;
                    if (dictProvider != null)
                        createOp.AddAnnotation(ClickHouseAnnotationNames.DictionarySourceProvider, dictProvider);
                }
            }
        }

        return operations;
    }

    /// <summary>
    /// Enriches DropTableOperation with annotations from the previous model.
    /// This allows the splitter to correctly classify MV and dictionary drops.
    /// </summary>
    private static IReadOnlyList<MigrationOperation> EnrichDropOperationsWithAnnotations(
        IReadOnlyList<MigrationOperation> operations,
        IRelationalModel? previousModel)
    {
        if (previousModel == null)
            return operations;

        foreach (var op in operations)
        {
            if (op is DropTableOperation dropOp)
            {
                // Find the table in the previous model
                var previousTable = previousModel.Tables
                    .FirstOrDefault(t => string.Equals(t.Name, dropOp.Name, StringComparison.OrdinalIgnoreCase));

                var entityType = previousTable?.EntityTypeMappings.FirstOrDefault()?.TypeBase as IEntityType;
                if (entityType == null)
                    continue;

                // Copy MaterializedView annotations
                var isMv = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedView)?.Value;
                if (isMv is true)
                {
                    dropOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);

                    var mvSource = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewSource)?.Value;
                    if (mvSource != null)
                        dropOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, mvSource);

                    var mvQuery = entityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery)?.Value;
                    if (mvQuery != null)
                        dropOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery, mvQuery);
                }

                // Copy Dictionary annotations
                var isDict = entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value;
                if (isDict is true)
                {
                    dropOp.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);

                    var dictSource = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySource)?.Value;
                    if (dictSource != null)
                        dropOp.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, dictSource);
                }
            }
        }

        return operations;
    }

    /// <summary>
    /// Generates migration code for a single step with forward-only Down.
    /// </summary>
    private string GenerateStepMigrationCode(
        IMigrationsCodeGenerator codeGenerator,
        string migrationId,
        string migrationName,
        IReadOnlyList<MigrationOperation> upOperations,
        string @namespace,
        Type contextType,
        string stepComment)
    {
        // Generate the Up method code using the standard generator
        var standardCode = codeGenerator.GenerateMigration(
            @namespace,
            migrationName,
            upOperations,
            []); // Empty Down operations

        // Replace the empty Down method with one that throws
        var downMethodCode = GenerateForwardOnlyDownMethod(migrationId);
        var modifiedCode = ReplaceDownMethod(standardCode, downMethodCode, stepComment);

        // Ensure our exception namespace is included
        return EnsureExceptionNamespace(modifiedCode);
    }

    /// <summary>
    /// Generates a Down method that throws ClickHouseDownMigrationNotSupportedException.
    /// </summary>
    private static string GenerateForwardOnlyDownMethod(string migrationId)
    {
        return $@"        protected override void Down(MigrationBuilder migrationBuilder)
        {{
            throw new ClickHouseDownMigrationNotSupportedException(""{migrationId}"");
        }}";
    }

    /// <summary>
    /// Replaces the Down method in generated code with our forward-only version.
    /// </summary>
    private static string ReplaceDownMethod(string code, string newDownMethod, string stepComment)
    {
        // Find and replace the Down method
        const string downPattern = "protected override void Down(MigrationBuilder migrationBuilder)";
        var downIndex = code.IndexOf(downPattern, StringComparison.Ordinal);

        if (downIndex < 0)
            return code;

        // Find the opening brace of the Down method
        var braceStart = code.IndexOf('{', downIndex);
        if (braceStart < 0)
            return code;

        // Find the matching closing brace
        var braceEnd = FindMatchingBrace(code, braceStart);
        if (braceEnd < 0)
            return code;

        // Replace the entire Down method
        var beforeDown = code[..downIndex];
        var afterDown = code[(braceEnd + 1)..];

        // Add step comment after the class declaration if it exists
        var result = $"{beforeDown}{newDownMethod}{afterDown}";

        // Insert step comment after "// <auto-generated />"
        const string autoGenComment = "// <auto-generated />";
        var autoGenIndex = result.IndexOf(autoGenComment, StringComparison.Ordinal);
        if (autoGenIndex >= 0)
        {
            var insertPoint = autoGenIndex + autoGenComment.Length;
            result = result[..insertPoint] + Environment.NewLine + stepComment + result[insertPoint..];
        }

        return result;
    }

    /// <summary>
    /// Finds the index of the matching closing brace.
    /// </summary>
    private static int FindMatchingBrace(string code, int openBraceIndex)
    {
        var depth = 0;
        for (int i = openBraceIndex; i < code.Length; i++)
        {
            if (code[i] == '{')
                depth++;
            else if (code[i] == '}')
            {
                depth--;
                if (depth == 0)
                    return i;
            }
        }
        return -1;
    }

    /// <summary>
    /// Ensures the EF.CH.Infrastructure namespace is included for the exception.
    /// </summary>
    private static string EnsureExceptionNamespace(string code)
    {
        const string requiredUsing = "using EF.CH.Infrastructure;";

        if (code.Contains(requiredUsing))
            return code;

        // Find the first using statement
        var usingIndex = code.IndexOf("using ", StringComparison.Ordinal);
        if (usingIndex < 0)
            return code;

        // Insert our using statement
        return code[..usingIndex] + requiredUsing + Environment.NewLine + code[usingIndex..];
    }
}
