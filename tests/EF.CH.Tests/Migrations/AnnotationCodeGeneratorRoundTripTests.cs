using EF.CH.Design.Internal;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Pins which entity-level ClickHouse annotations are translated to fluent API calls
/// by <see cref="ClickHouseAnnotationCodeGenerator"/> and which fall through as raw
/// <c>HasAnnotation(...)</c> calls in scaffolded migrations.
///
/// Annotations that are NOT translated still survive the snapshot — EF Core falls back
/// to <c>HasAnnotation</c> — but they don't re-emit through the proper fluent API on
/// scaffold, which means subsequent scaffolds (or doc-tooling that walks the calls list)
/// see the raw annotation rather than a typed call. Items in the "deferred gap" list
/// below are tracked as a follow-up to wire through.
/// </summary>
public class AnnotationCodeGeneratorRoundTripTests
{
    [Fact]
    public void Engine_OrderBy_PartitionBy_Ttl_ArePinnedToFluentApi()
    {
        var (entityType, generator) = Setup(b =>
        {
            b.HasKey(e => e.Id);
            b.UseMergeTree(e => e.Id);
            b.HasPartitionBy("toYYYYMM(Id)");
            b.HasTtl("Id + INTERVAL 1 DAY");
        });

        var rawAnnotations = string.Join(", ", entityType.GetAnnotations().Select(a => a.Name));
        var calls = GenerateCalls(entityType, generator);
        var emitted = string.Join(", ", calls.Select(c => c.Method));

        Assert.True(calls.Any(c => c.Method == "UseMergeTree"),
            $"UseMergeTree missing. Annotations: {rawAnnotations}. Emitted: {emitted}");
        Assert.True(calls.Any(c => c.Method == "HasPartitionBy"),
            $"HasPartitionBy missing. Annotations: {rawAnnotations}. Emitted: {emitted}");
        Assert.True(calls.Any(c => c.Method == "HasTtl"),
            $"HasTtl missing. Annotations: {rawAnnotations}. Emitted: {emitted}");
    }

    [Fact]
    public void EngineSettings_RoundTripsAsHasEngineSettingsCall()
    {
        var (entityType, generator) = Setup(b =>
        {
            b.HasKey(e => e.Id);
            b.UseMergeTree(e => e.Id);
            b.HasEngineSettings(new Dictionary<string, string> { ["index_granularity"] = "4096" });
        });

        var calls = GenerateCalls(entityType, generator);

        var settingsCall = Assert.Single(calls, c => c.Method == "HasEngineSettings");
        Assert.Single(settingsCall.Arguments);
        var dict = Assert.IsAssignableFrom<IDictionary<string, string>>(settingsCall.Arguments[0]!);
        Assert.Equal("4096", dict["index_granularity"]);
    }

    [Fact]
    public void ReplicatedPath_And_ReplicaName_RoundTripAsHasReplicationCall()
    {
        var (entityType, generator) = Setup(b =>
        {
            b.HasKey(e => e.Id);
            b.UseMergeTree(e => e.Id);
            b.HasAnnotation(ClickHouseAnnotationNames.IsReplicated, true);
            b.HasAnnotation(ClickHouseAnnotationNames.ReplicatedPath, "/clickhouse/tables/{shard}/test");
            b.HasAnnotation(ClickHouseAnnotationNames.ReplicaName, "{replica}");
        });

        var calls = GenerateCalls(entityType, generator);

        var replicationCall = Assert.Single(calls, c => c.Method == "HasReplication");
        Assert.Equal(2, replicationCall.Arguments.Count);
        Assert.Equal("/clickhouse/tables/{shard}/test", replicationCall.Arguments[0]);
        Assert.Equal("{replica}", replicationCall.Arguments[1]);
    }

    private static (IEntityType EntityType, IAnnotationCodeGenerator Generator) Setup(
        Action<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<RoundTripEntity>> configure)
    {
        var ctx = new RoundTripDbContext(configure);
        var entityType = ctx.Model.FindEntityType(typeof(RoundTripEntity))!;

        // IAnnotationCodeGenerator is a *design-time* service, registered through
        // ClickHouseDesignTimeServices, not through the runtime DI of a DbContext.
        var services = new ServiceCollection();
        new ClickHouseDesignTimeServices().ConfigureDesignTimeServices(services);
        var generator = services.BuildServiceProvider().GetRequiredService<IAnnotationCodeGenerator>();

        return (entityType, generator);
    }

    private static IReadOnlyList<MethodCallCodeFragment> GenerateCalls(IEntityType entityType, IAnnotationCodeGenerator generator)
    {
        var annotations = entityType.GetAnnotations().ToDictionary(a => a.Name, a => (IAnnotation)a);
        return generator.GenerateFluentApiCalls(entityType, annotations);
    }

    private sealed class RoundTripEntity { public int Id { get; set; } }

    private sealed class RoundTripDbContext : DbContext
    {
        private readonly Action<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<RoundTripEntity>> _configure;
        public RoundTripDbContext(Action<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<RoundTripEntity>> configure)
            => _configure = configure;
        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options
                .UseClickHouse("Host=localhost;Database=test")
                // Per-test isolation: EF Core caches the built model keyed by
                // DbContextOptions, so without this two tests with the same DbContext
                // type but different fluent calls would race for one cached model.
                .EnableServiceProviderCaching(false);
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _configure(modelBuilder.Entity<RoundTripEntity>());
    }
}
