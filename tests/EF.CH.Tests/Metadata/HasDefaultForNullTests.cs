using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Metadata;

#region Test Entities

public class HasDefaultForNullTestEntity
{
    public Guid Id { get; set; }
    public int? Score { get; set; }
    public string? Notes { get; set; }
}

public class HasDefaultForNullEntityWithGenerated
{
    public Guid Id { get; set; }
    public int? Score { get; set; }
}

#endregion

#region Test Contexts

public class TypedDefaultContext : DbContext
{
    public TypedDefaultContext(DbContextOptions<TypedDefaultContext> options) : base(options) { }

    public DbSet<HasDefaultForNullTestEntity> Entities => Set<HasDefaultForNullTestEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<HasDefaultForNullTestEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("typed_default_entities");
            entity.UseMergeTree(x => x.Id);
            entity.Property(e => e.Score).HasDefaultForNull(0);
            entity.Property(e => e.Notes).HasDefaultForNull("");
        });
    }
}

public class OverrideDefaultContext : DbContext
{
    public OverrideDefaultContext(DbContextOptions<OverrideDefaultContext> options) : base(options) { }

    public DbSet<HasDefaultForNullTestEntity> Entities => Set<HasDefaultForNullTestEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<HasDefaultForNullTestEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("override_default_entities");
            entity.UseMergeTree(x => x.Id);
            // Object overload — value not equal to default(int)
            entity.Property(e => e.Score).HasDefaultForNull((object)42);
        });
    }
}

public class ConflictingValueGeneratedContext : DbContext
{
    public ConflictingValueGeneratedContext(DbContextOptions<ConflictingValueGeneratedContext> options) : base(options) { }

    public DbSet<HasDefaultForNullEntityWithGenerated> Entities => Set<HasDefaultForNullEntityWithGenerated>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<HasDefaultForNullEntityWithGenerated>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("conflicting_entities");
            entity.UseMergeTree(x => x.Id);
            // Conflict: HasDefaultForNull combined with ValueGeneratedOnAdd
            entity.Property(e => e.Score)
                .HasDefaultForNull(0)
                .ValueGeneratedOnAdd();
        });
    }
}

#endregion

public class HasDefaultForNullTests
{
    [Fact]
    public void GetDefaultForNullValue_ReturnsConfiguredValue_ForInt()
    {
        using var context = CreateContext<TypedDefaultContext>();

        var entityType = context.Model.FindEntityType(typeof(HasDefaultForNullTestEntity))!;
        var property = entityType.FindProperty(nameof(HasDefaultForNullTestEntity.Score))!;

        Assert.Equal(0, property.GetDefaultForNullValue());
    }

    [Fact]
    public void GetDefaultForNullValue_ReturnsConfiguredValue_ForString()
    {
        using var context = CreateContext<TypedDefaultContext>();

        var entityType = context.Model.FindEntityType(typeof(HasDefaultForNullTestEntity))!;
        var property = entityType.FindProperty(nameof(HasDefaultForNullTestEntity.Notes))!;

        Assert.Equal("", property.GetDefaultForNullValue());
    }

    [Fact]
    public void GetDefaultForNullValue_ReturnsNull_WhenNotConfigured()
    {
        using var context = CreateContext<TypedDefaultContext>();

        var entityType = context.Model.FindEntityType(typeof(HasDefaultForNullTestEntity))!;
        // Id is not configured with HasDefaultForNull
        var property = entityType.FindProperty(nameof(HasDefaultForNullTestEntity.Id))!;

        Assert.Null(property.GetDefaultForNullValue());
    }

    [Fact]
    public void HasDefaultForNull_ObjectOverload_StoresOverrideValue()
    {
        using var context = CreateContext<OverrideDefaultContext>();

        var entityType = context.Model.FindEntityType(typeof(HasDefaultForNullTestEntity))!;
        var property = entityType.FindProperty(nameof(HasDefaultForNullTestEntity.Score))!;

        // Override value is 42, not the inferred default(int) of 0
        Assert.Equal(42, property.GetDefaultForNullValue());
    }

    [Fact]
    public void HasDefaultForNull_ObjectOverload_RejectsIncompatibleType()
    {
        var optionsBuilder = new DbContextOptionsBuilder<TypedDefaultContext>()
            .UseClickHouse("Host=localhost;Database=test");

        Assert.Throws<InvalidOperationException>(() =>
        {
            using var context = new BadOverrideContext(
                new DbContextOptionsBuilder<BadOverrideContext>()
                    .UseClickHouse("Host=localhost;Database=test")
                    .Options);
            // touching Model triggers OnModelCreating
            _ = context.Model;
        });
    }

    [Fact]
    public void Validation_ThrowsWhenCombinedWithValueGenerated()
    {
        using var context = CreateContext<ConflictingValueGeneratedContext>();

        var ex = Assert.Throws<InvalidOperationException>(() => _ = context.Model);
        Assert.Contains("HasDefaultForNull", ex.Message);
        Assert.Contains("value-generation", ex.Message);
    }

    private static TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}

#region Bad-override context (used to verify type validation in object overload)

public class BadOverrideContext : DbContext
{
    public BadOverrideContext(DbContextOptions<BadOverrideContext> options) : base(options) { }

    public DbSet<HasDefaultForNullTestEntity> Entities => Set<HasDefaultForNullTestEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<HasDefaultForNullTestEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("bad_override_entities");
            entity.UseMergeTree(x => x.Id);
            // Score is int? but the object value is a string — must throw
            entity.Property(e => e.Score).HasDefaultForNull((object)"oops");
        });
    }
}

#endregion
