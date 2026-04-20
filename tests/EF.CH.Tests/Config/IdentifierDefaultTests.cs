using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Xunit;

namespace EF.CH.Tests.Config;

#region Test Entity & Context

public class IdentifierDefaultEntity
{
    public ulong SerialId { get; set; }
    public Guid UuidV4 { get; set; }
    public Guid UuidV7 { get; set; }
    public string Ulid { get; set; } = string.Empty;
    public long SnowflakeId { get; set; }
    public DateTime OrderDate { get; set; }
}

public class IdentifierDefaultContext : DbContext
{
    public IdentifierDefaultContext(DbContextOptions<IdentifierDefaultContext> options)
        : base(options) { }

    public DbSet<IdentifierDefaultEntity> Entities => Set<IdentifierDefaultEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<IdentifierDefaultEntity>(entity =>
        {
            entity.HasKey(e => e.SerialId);
            entity.ToTable("identifier_default_entities");
            entity.UseMergeTree(x => x.OrderDate);

            entity.Property(e => e.SerialId).HasSerialIDDefault("order_counter");
            entity.Property(e => e.UuidV4).HasUuidV4Default();
            entity.Property(e => e.UuidV7).HasUuidV7Default();
            entity.Property(e => e.Ulid).HasUlidDefault();
            entity.Property(e => e.SnowflakeId).HasSnowflakeIDDefault();
        });
    }
}

#endregion

public class IdentifierDefaultTests
{
    [Fact]
    public void HasSerialIDDefault_SetsGenerateSerialIDExpression()
    {
        using var context = CreateContext();
        var property = GetProperty(context, nameof(IdentifierDefaultEntity.SerialId));

        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.DefaultExpression);

        Assert.NotNull(annotation);
        Assert.Equal("generateSerialID('order_counter')", annotation.Value);
        Assert.Equal(ValueGenerated.OnAdd, property.ValueGenerated);
    }

    [Fact]
    public void HasSerialIDDefault_EscapesSingleQuotesInCounterName()
    {
        var options = new DbContextOptionsBuilder<IdentifierDefaultContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        using var context = new IdentifierDefaultContext(options);
        var builder = new ModelBuilder();

        builder.Entity<IdentifierDefaultEntity>()
            .Property(e => e.SerialId)
            .HasSerialIDDefault("it's a counter");

        var property = builder.Model
            .FindEntityType(typeof(IdentifierDefaultEntity))!
            .FindProperty(nameof(IdentifierDefaultEntity.SerialId))!;

        Assert.Equal(
            "generateSerialID('it''s a counter')",
            property.FindAnnotation(ClickHouseAnnotationNames.DefaultExpression)!.Value);
    }

    [Fact]
    public void HasSerialIDDefault_NullCounter_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() =>
        {
            var builder = new ModelBuilder();
            builder.Entity<IdentifierDefaultEntity>()
                .Property(e => e.SerialId)
                .HasSerialIDDefault(null!);
        });
    }

    [Theory]
    [InlineData(nameof(IdentifierDefaultEntity.UuidV4), "generateUUIDv4()")]
    [InlineData(nameof(IdentifierDefaultEntity.UuidV7), "generateUUIDv7()")]
    [InlineData(nameof(IdentifierDefaultEntity.Ulid), "generateULID()")]
    [InlineData(nameof(IdentifierDefaultEntity.SnowflakeId), "generateSnowflakeID()")]
    public void GeneratorHelpers_SetCorrectDefaultAndValueGenerated(string propertyName, string expected)
    {
        using var context = CreateContext();
        var property = GetProperty(context, propertyName);

        var annotation = property.FindAnnotation(ClickHouseAnnotationNames.DefaultExpression);

        Assert.NotNull(annotation);
        Assert.Equal(expected, annotation.Value);
        Assert.Equal(ValueGenerated.OnAdd, property.ValueGenerated);
    }

    [Fact]
    public void CreateTable_EmitsGeneratorDefaultsInDdl()
    {
        using var context = CreateContext();

        var script = context.Database.GenerateCreateScript();

        Assert.Contains("DEFAULT generateSerialID('order_counter')", script);
        Assert.Contains("DEFAULT generateUUIDv4()", script);
        Assert.Contains("DEFAULT generateUUIDv7()", script);
        Assert.Contains("DEFAULT generateULID()", script);
        Assert.Contains("DEFAULT generateSnowflakeID()", script);
    }

    private static IdentifierDefaultContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<IdentifierDefaultContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return new IdentifierDefaultContext(options);
    }

    private static IProperty GetProperty(IdentifierDefaultContext context, string name)
        => context.Model
            .FindEntityType(typeof(IdentifierDefaultEntity))!
            .FindProperty(name)!;
}
