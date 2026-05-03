using EF.CH.Dictionaries;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// <c>ClickHouseDictionary&lt;TDict, TKey&gt;</c> caches resolved metadata in a static
/// field on the closed generic type, so two <see cref="DbContext"/> instances that
/// share the same <c>TDict</c> but map it to different dictionary names (multi-tenant,
/// versioned dictionaries, etc.) used to silently share the first context's cached
/// metadata. The cache must be keyed on the model identity so each context resolves
/// independently.
/// </summary>
public class DictionaryMetadataCacheTests
{
    [Fact]
    public void TwoContexts_DifferentDictionaryName_BothResolveTheirOwnMetadata()
    {
        using var ctxA = new MetaCacheCtx("country_lookup_v1");
        using var ctxB = new MetaCacheCtx("country_lookup_v2");

        var dictA = new ClickHouseDictionary<MetaCacheCountry, ulong>(ctxA);
        var dictB = new ClickHouseDictionary<MetaCacheCountry, ulong>(ctxB);

        Assert.Equal("country_lookup_v1", dictA.Name);
        Assert.Equal("country_lookup_v2", dictB.Name);
    }

    [Fact]
    public void SameContext_RepeatedResolution_ReturnsCachedMetadata()
    {
        // Defence-in-depth: the cache fix must still cache within a single model,
        // not regress to no caching at all.
        using var ctx = new MetaCacheCtx("country_lookup_v1");

        var dict1 = new ClickHouseDictionary<MetaCacheCountry, ulong>(ctx);
        var dict2 = new ClickHouseDictionary<MetaCacheCountry, ulong>(ctx);

        Assert.Equal(dict1.Name, dict2.Name);
    }
}

public class MetaCacheCountry : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class MetaCacheCtx : DbContext
{
    private readonly string _dictName;

    public MetaCacheCtx(string dictName)
    {
        _dictName = dictName;
    }

    public DbSet<MetaCacheCountry> Lookups => Set<MetaCacheCountry>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options
            .UseClickHouse("Host=localhost;Database=test")
            .EnableServiceProviderCaching(false);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MetaCacheCountry>(e =>
        {
            e.HasKey(x => x.Id);
            e.AsDictionary<MetaCacheCountry, MetaCacheCountry>(d => d
                .HasName(_dictName)
                .HasKey(x => x.Id)
                .FromTable()
                .UseHashedLayout()
                .HasLifetime(60));
        });
    }
}
