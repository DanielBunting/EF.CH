using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Metadata.Attributes;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Config;

#region Test Entities

public class FluentEphemeralEntity
{
    public Guid Id { get; set; }
    public ulong UnhashedKey { get; set; }
    public ulong HashedKey { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class AttributeEphemeralEntity
{
    public Guid Id { get; set; }

    [EphemeralColumn]
    public ulong UnhashedKey { get; set; }

    [MaterializedColumn("sipHash64(\"UnhashedKey\")")]
    public ulong HashedKey { get; set; }

    [EphemeralColumn("now()")]
    public DateTime CreatedAt { get; set; }
}

public class ConflictingEphemeralEntity
{
    public Guid Id { get; set; }
    public ulong Value { get; set; }
}

#endregion

#region Test DbContexts

public class FluentEphemeralContext(DbContextOptions<FluentEphemeralContext> options) : DbContext(options)
{
    public DbSet<FluentEphemeralEntity> Entities => Set<FluentEphemeralEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FluentEphemeralEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("fluent_ephemeral_entities");
            entity.UseMergeTree(x => x.Id);

            entity.Property(e => e.UnhashedKey).HasEphemeralExpression();
            entity.Property(e => e.HashedKey).HasMaterializedExpression("sipHash64(\"UnhashedKey\")");
            entity.Property(e => e.CreatedAt).HasEphemeralExpression("now()");
        });
    }
}

public class AttributeEphemeralContext(DbContextOptions<AttributeEphemeralContext> options) : DbContext(options)
{
    public DbSet<AttributeEphemeralEntity> Entities => Set<AttributeEphemeralEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AttributeEphemeralEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("attribute_ephemeral_entities");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

#endregion

public class EphemeralColumnTests
{
    #region Annotation Tests

    [Fact]
    public void HasEphemeralExpression_NoDefault_SetsNullAnnotationValue()
    {
        using var context = CreateContext<FluentEphemeralContext>();
        var property = GetProperty(context, typeof(FluentEphemeralEntity), nameof(FluentEphemeralEntity.UnhashedKey));

        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.EphemeralExpression);

        Assert.NotNull(annotation);
        Assert.Null(annotation.Value);
    }

    [Fact]
    public void HasEphemeralExpression_WithDefault_SetsAnnotationValue()
    {
        using var context = CreateContext<FluentEphemeralContext>();
        var property = GetProperty(context, typeof(FluentEphemeralEntity), nameof(FluentEphemeralEntity.CreatedAt));

        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.EphemeralExpression);

        Assert.NotNull(annotation);
        Assert.Equal("now()", annotation.Value);
    }

    [Fact]
    public void HasEphemeralExpression_ConfiguresSaveBehaviors()
    {
        using var context = CreateContext<FluentEphemeralContext>();
        var property = GetProperty(context, typeof(FluentEphemeralEntity), nameof(FluentEphemeralEntity.UnhashedKey));

        Assert.Equal(PropertySaveBehavior.Save, property.GetBeforeSaveBehavior());
        Assert.Equal(PropertySaveBehavior.Ignore, property.GetAfterSaveBehavior());
        Assert.Equal(ValueGenerated.Never, property.ValueGenerated);
    }

    [Fact]
    public void EphemeralColumnAttribute_NoDefault_SetsAnnotationWithNullValue()
    {
        using var context = CreateContext<AttributeEphemeralContext>();
        var property = GetProperty(context, typeof(AttributeEphemeralEntity), nameof(AttributeEphemeralEntity.UnhashedKey));

        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.EphemeralExpression);

        Assert.NotNull(annotation);
        Assert.Null(annotation.Value);
        Assert.Equal(PropertySaveBehavior.Save, property.GetBeforeSaveBehavior());
        Assert.Equal(PropertySaveBehavior.Ignore, property.GetAfterSaveBehavior());
    }

    [Fact]
    public void EphemeralColumnAttribute_WithDefault_SetsAnnotationValue()
    {
        using var context = CreateContext<AttributeEphemeralContext>();
        var property = GetProperty(context, typeof(AttributeEphemeralEntity), nameof(AttributeEphemeralEntity.CreatedAt));

        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.EphemeralExpression);

        Assert.NotNull(annotation);
        Assert.Equal("now()", annotation.Value);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void CreateTable_EmitsEphemeralClause_NoDefault()
    {
        using var context = CreateContext<FluentEphemeralContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("\"UnhashedKey\" UInt64 EPHEMERAL", script);
    }

    [Fact]
    public void CreateTable_EmitsEphemeralClause_WithDefault()
    {
        using var context = CreateContext<FluentEphemeralContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("EPHEMERAL now()", script);
    }

    [Fact]
    public void CreateTable_EphemeralAndDerivedMaterializedCoexist()
    {
        using var context = CreateContext<FluentEphemeralContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("EPHEMERAL", script);
        Assert.Contains("MATERIALIZED sipHash64(\"UnhashedKey\")", script);
    }

    [Fact]
    public void CreateTable_AttributeDrivenEphemeralAlsoEmits()
    {
        using var context = CreateContext<AttributeEphemeralContext>();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("EPHEMERAL", script);
        Assert.Contains("MATERIALIZED sipHash64(\"UnhashedKey\")", script);
    }

    #endregion

    #region Query-Rewrite Tests

    [Fact]
    public void Select_AllColumns_RewritesEphemeralToTypedDefault()
    {
        using var context = CreateContext<FluentEphemeralContext>();

        var sql = context.Entities.ToQueryString();

        // Ephemeral columns become defaultValueOfTypeName(...) in the SELECT —
        // ClickHouse rejects referencing them as columns. Non-ephemeral columns
        // still come through as qualified column references.
        Assert.Contains("defaultValueOfTypeName('UInt64')", sql);
        Assert.Contains("\"HashedKey\"", sql);
        Assert.DoesNotContain("\".\"UnhashedKey\"", sql);
        Assert.DoesNotContain("\".\"CreatedAt\"", sql);
    }

    [Fact]
    public void Select_ExplicitProjection_RewritesEphemeralToTypedDefault()
    {
        using var context = CreateContext<FluentEphemeralContext>();

        var sql = context.Entities
            .Select(e => new { e.Id, e.UnhashedKey, e.HashedKey })
            .ToQueryString();

        Assert.Contains("defaultValueOfTypeName(", sql);
        Assert.Contains("\"HashedKey\"", sql);
    }

    #endregion

    #region Mutual-Exclusion Tests

    [Fact]
    public void Ephemeral_CombinedWith_Materialized_ThrowsOnValidate()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            BuildContext(entity =>
            {
                entity.Property(e => e.Value)
                    .HasEphemeralExpression()
                    .HasMaterializedExpression("42");
            }));

        Assert.Contains("mutually exclusive", ex.Message);
    }

    [Fact]
    public void Ephemeral_CombinedWith_Alias_ThrowsOnValidate()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            BuildContext(entity =>
            {
                entity.Property(e => e.Value)
                    .HasEphemeralExpression()
                    .HasAliasExpression("42");
            }));

        Assert.Contains("mutually exclusive", ex.Message);
    }

    [Fact]
    public void Ephemeral_CombinedWith_Default_ThrowsOnValidate()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            BuildContext(entity =>
            {
                entity.Property(e => e.Value)
                    .HasEphemeralExpression()
                    .HasDefaultExpression("42");
            }));

        Assert.Contains("mutually exclusive", ex.Message);
    }

    [Fact]
    public void Ephemeral_CombinedWith_Codec_ThrowsOnValidate()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            BuildContext(entity =>
            {
                entity.Property(e => e.Value)
                    .HasEphemeralExpression()
                    .HasCodec(c => c.ZSTD());
            }));

        Assert.Contains("cannot have a compression codec", ex.Message);
    }

    #endregion

    #region Test Infrastructure

    private static TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    private static IProperty GetProperty(DbContext context, Type entityClrType, string propertyName)
        => context.Model.FindEntityType(entityClrType)!.FindProperty(propertyName)!;

    /// <summary>
    /// Builds a minimal context with a single entity configured via the supplied
    /// action. Accessing <c>context.Model</c> forces validation to run.
    /// </summary>
    private static void BuildContext(Action<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<ConflictingEphemeralEntity>> configure)
    {
        var options = new DbContextOptionsBuilder<ConflictingContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        using var context = new ConflictingContext(options, configure);
        _ = context.Model; // triggers validation
    }

    private class ConflictingContext : DbContext
    {
        private readonly Action<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<ConflictingEphemeralEntity>> _configure;

        public ConflictingContext(
            DbContextOptions<ConflictingContext> options,
            Action<Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<ConflictingEphemeralEntity>> configure)
            : base(options)
        {
            _configure = configure;
        }

        public DbSet<ConflictingEphemeralEntity> Entities => Set<ConflictingEphemeralEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ConflictingEphemeralEntity>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.ToTable("conflicting_ephemeral_entities");
                entity.UseMergeTree(x => x.Id);
                _configure(entity);
            });
        }
    }

    #endregion
}

/// <summary>
/// Integration tests against a real ClickHouse instance. Confirms the whole
/// round trip: ephemeral column is written on INSERT, feeds a MATERIALIZED
/// column, and is silently rewritten to a typed default on read.
/// </summary>
public class EphemeralColumnIntegrationTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public async Task InitializeAsync() => await _container.StartAsync();

    public async Task DisposeAsync() => await _container.DisposeAsync();

    [Fact]
    public async Task Insert_WithEphemeralValue_DerivedMaterializedComputesHash()
    {
        var options = new DbContextOptionsBuilder<FluentEphemeralContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentEphemeralContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var idA = Guid.NewGuid();
        var idB = Guid.NewGuid();
        context.Entities.AddRange(
            new FluentEphemeralEntity { Id = idA, UnhashedKey = 42UL },
            new FluentEphemeralEntity { Id = idB, UnhashedKey = 42UL });
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();

        var rowA = await context.Entities.AsNoTracking().SingleAsync(e => e.Id == idA);
        var rowB = await context.Entities.AsNoTracking().SingleAsync(e => e.Id == idB);

        // Equal ephemeral inputs produce equal hashes (deterministic sipHash64).
        Assert.Equal(rowA.HashedKey, rowB.HashedKey);
        // Hash is non-zero for input 42.
        Assert.NotEqual(0UL, rowA.HashedKey);
        // Ephemeral column reads back as CLR default — rewritten to
        // defaultValueOfTypeName('UInt64') server-side.
        Assert.Equal(0UL, rowA.UnhashedKey);
    }

    [Fact]
    public async Task Insert_DifferentEphemeralValues_ProduceDifferentHashes()
    {
        var options = new DbContextOptionsBuilder<FluentEphemeralContext>()
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        await using var context = new FluentEphemeralContext(options);
        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        var idA = Guid.NewGuid();
        var idB = Guid.NewGuid();
        context.Entities.AddRange(
            new FluentEphemeralEntity { Id = idA, UnhashedKey = 100UL },
            new FluentEphemeralEntity { Id = idB, UnhashedKey = 200UL });
        await context.SaveChangesAsync();

        context.ChangeTracker.Clear();

        var rowA = await context.Entities.AsNoTracking().SingleAsync(e => e.Id == idA);
        var rowB = await context.Entities.AsNoTracking().SingleAsync(e => e.Id == idB);

        Assert.NotEqual(rowA.HashedKey, rowB.HashedKey);
    }
}
