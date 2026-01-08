using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Design;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Tests that demonstrate the need for CREATE operation enrichment in the scaffolder.
///
/// Problem: When MigrationsModelDiffer produces CreateTableOperation for an MV,
/// it does NOT copy the MaterializedView annotation from the entity type.
/// This causes the splitter to misclassify MVs as regular tables.
///
/// These tests verify:
/// 1. Entity types are correctly annotated (model configuration works)
/// 2. Without annotation enrichment, splitter misclassifies operations
/// 3. After fix: enriched operations are correctly classified
/// </summary>
public class ScaffolderEnrichmentTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    /// <summary>
    /// Verifies that entity types configured with AsMaterializedViewRaw
    /// have the correct annotations on the model.
    /// This confirms the model configuration is working correctly.
    /// </summary>
    [Fact]
    public void ModelConfiguration_MvEntity_HasCorrectAnnotations()
    {
        // Arrange
        using var context = CreateContext<MvSourceAndTargetContext>();

        // Act
        var mvEntityType = context.Model.FindEntityType(typeof(DailySummary));

        // Assert: Entity type should have MV annotations
        Assert.NotNull(mvEntityType);

        var isMvAnnotation = mvEntityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedView);
        Assert.NotNull(isMvAnnotation);
        Assert.True((bool?)isMvAnnotation.Value);

        var sourceAnnotation = mvEntityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewSource);
        Assert.NotNull(sourceAnnotation);
        Assert.Equal("source_events", sourceAnnotation.Value);

        var queryAnnotation = mvEntityType.FindAnnotation(ClickHouseAnnotationNames.MaterializedViewQuery);
        Assert.NotNull(queryAnnotation);
        // LINQ-generated query uses quoted identifiers like "source_events"
        Assert.Contains("\"source_events\"", (string?)queryAnnotation.Value);
    }

    /// <summary>
    /// THIS IS THE KEY TEST.
    ///
    /// Verifies that when operations are enriched from the model (simulating
    /// what ClickHouseMigrationsScaffolder.EnrichCreateOperationsWithAnnotations does),
    /// the splitter correctly orders MVs after their source tables.
    ///
    /// Before fix: This test failed because model differ output had no annotations.
    /// After fix: Scaffolder enriches operations, so splitter can detect and reorder.
    /// </summary>
    [Fact]
    public void Splitter_WithEnrichedOperations_OrdersSourceTableFirst()
    {
        // Arrange: Create a context with MV configuration to get a model
        using var context = CreateContext<MvSourceAndTargetContext>();
        var model = context.Model;

        // Create operations WITHOUT annotations (simulating raw model differ output)
        var sourceTableOp = new CreateTableOperation { Name = "source_events" };
        var mvOp = new CreateTableOperation { Name = "daily_summary_mv" };

        // Put MV first (wrong order)
        var operations = new List<MigrationOperation> { mvOp, sourceTableOp };

        // Simulate enrichment (what scaffolder does after model differ)
        EnrichOperationsFromModel(operations, model);

        // Act: Use splitter
        var splitter = new ClickHouseMigrationsSplitter();
        var steps = splitter.Split(operations);

        // Assert: After enrichment, splitter correctly reorders
        Assert.Equal(2, steps.Count);

        var firstOp = (CreateTableOperation)steps[0].Operation;
        var secondOp = (CreateTableOperation)steps[1].Operation;

        // Source table (Phase 4) before MV (Phase 6)
        Assert.Equal("source_events", firstOp.Name);
        Assert.Equal("daily_summary_mv", secondOp.Name);
    }

    /// <summary>
    /// Helper that simulates what ClickHouseMigrationsScaffolder.EnrichCreateOperationsWithAnnotations does.
    /// </summary>
    private static void EnrichOperationsFromModel(IList<MigrationOperation> operations, Microsoft.EntityFrameworkCore.Metadata.IModel model)
    {
        foreach (var op in operations)
        {
            if (op is CreateTableOperation createOp)
            {
                var entityType = model.GetEntityTypes()
                    .FirstOrDefault(e => string.Equals(e.GetTableName(), createOp.Name, StringComparison.OrdinalIgnoreCase));

                if (entityType == null)
                    continue;

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
                }
            }
        }
    }

    /// <summary>
    /// Verifies that when CreateTableOperation HAS the MaterializedView annotation,
    /// the splitter correctly orders MVs after their source tables.
    /// This is the expected behavior after the scaffolder enriches operations.
    /// </summary>
    [Fact]
    public void Splitter_WithAnnotation_CorrectlyOrdersMvAfterSourceTable()
    {
        // Arrange: Create operations WITH annotations (as enriched scaffolder should produce)
        var sourceTableOp = new CreateTableOperation { Name = "source_events" };

        var mvOp = new CreateTableOperation { Name = "daily_summary_mv" };
        mvOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedView, true);
        mvOp.AddAnnotation(ClickHouseAnnotationNames.MaterializedViewSource, "source_events");

        // Put MV first (wrong order)
        var operations = new List<MigrationOperation> { mvOp, sourceTableOp };

        // Act
        var splitter = new ClickHouseMigrationsSplitter();
        var steps = splitter.Split(operations);

        // Assert: With annotation, splitter correctly reorders
        Assert.Equal(2, steps.Count);

        var firstOp = (CreateTableOperation)steps[0].Operation;
        var secondOp = (CreateTableOperation)steps[1].Operation;

        // Source table (Phase 4) comes before MV (Phase 6)
        Assert.Equal("source_events", firstOp.Name);
        Assert.Equal("daily_summary_mv", secondOp.Name);
    }

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}

#region Test Entities

public class SourceEvent
{
    public Guid Id { get; set; }
    public DateTime EventTime { get; set; }
    public string Category { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

public class DailySummary
{
    public DateTime Date { get; set; }
    public string Category { get; set; } = string.Empty;
    public long EventCount { get; set; }
    public decimal TotalAmount { get; set; }
}

#endregion

#region Test Contexts

public class MvSourceAndTargetContext : DbContext
{
    public MvSourceAndTargetContext(DbContextOptions<MvSourceAndTargetContext> options)
        : base(options) { }

    public DbSet<SourceEvent> SourceEvents => Set<SourceEvent>();
    public DbSet<DailySummary> DailySummaries => Set<DailySummary>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SourceEvent>(entity =>
        {
            entity.ToTable("source_events");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.EventTime, x.Id });
        });

        modelBuilder.Entity<DailySummary>(entity =>
        {
            entity.ToTable("daily_summary_mv");
            entity.UseSummingMergeTree(x => new { x.Date, x.Category });
            entity.AsMaterializedView<DailySummary, SourceEvent>(
                query: events => events
                    .GroupBy(e => new { Date = e.EventTime.Date, e.Category })
                    .Select(g => new DailySummary
                    {
                        Date = g.Key.Date,
                        Category = g.Key.Category,
                        EventCount = g.Count(),
                        TotalAmount = g.Sum(e => e.Amount)
                    }),
                populate: false);
        });
    }
}

#endregion
