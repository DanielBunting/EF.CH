using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Design;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Pins that the migrations scaffolder copies engine-knob annotations
/// (Engine, OrderBy, VersionColumn, IsDeletedColumn, etc.) from entity types
/// onto <see cref="CreateTableOperation"/>. Without this, the SQL generator's
/// model-fallback is load-bearing — meaning a scaffolded migration that runs
/// against a different model would emit different DDL.
/// </summary>
public class EngineAnnotationEnrichmentTests
{
    [Fact]
    public void Enrich_ReplacingMergeTreeWithIsDeleted_CopiesAllEngineAnnotations()
    {
        var builder = new ModelBuilder();
        builder.Entity<EnrichmentEntity>(e =>
        {
            e.ToTable("deletable");
            e.HasKey(x => x.Id);
            e.UseReplacingMergeTree(x => x.Id)
                .WithVersion(x => x.Version)
                .WithIsDeleted(x => x.IsDeleted);
        });
        var model = builder.FinalizeModel();

        var op = new CreateTableOperation { Name = "deletable" };
        EngineAnnotationEnricher.Enrich(op, model);

        Assert.Equal("ReplacingMergeTree", op.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(new[] { "Id" }, op.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
        Assert.Equal("Version", op.FindAnnotation(ClickHouseAnnotationNames.VersionColumn)?.Value);
        Assert.Equal("IsDeleted", op.FindAnnotation(ClickHouseAnnotationNames.IsDeletedColumn)?.Value);
    }

    [Fact]
    public void Enrich_PlainMergeTree_CopiesEngineAndOrderBy()
    {
        var builder = new ModelBuilder();
        builder.Entity<EnrichmentEntity>(e =>
        {
            e.ToTable("plain");
            e.HasKey(x => x.Id);
            e.UseMergeTree(x => new { x.Id, x.Version });
        });
        var model = builder.FinalizeModel();

        var op = new CreateTableOperation { Name = "plain" };
        EngineAnnotationEnricher.Enrich(op, model);

        Assert.Equal("MergeTree", op.FindAnnotation(ClickHouseAnnotationNames.Engine)?.Value);
        Assert.Equal(new[] { "Id", "Version" }, op.FindAnnotation(ClickHouseAnnotationNames.OrderBy)?.Value);
    }

    [Fact]
    public void Enrich_OperationNotInModel_IsNoOp()
    {
        var builder = new ModelBuilder();
        var model = builder.FinalizeModel();

        var op = new CreateTableOperation { Name = "ghost_table" };
        EngineAnnotationEnricher.Enrich(op, model);

        Assert.Null(op.FindAnnotation(ClickHouseAnnotationNames.Engine));
    }
}

internal sealed class EnrichmentEntity
{
    public Guid Id { get; set; }
    public long Version { get; set; }
    public byte IsDeleted { get; set; }
}
