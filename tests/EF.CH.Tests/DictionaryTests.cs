using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests;

#region Test Entities

/// <summary>
/// Source table entity for dictionary tests.
/// </summary>
public class Country
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string IsoCode { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public bool IsActive { get; set; } = true;
}

/// <summary>
/// Dictionary entity for basic tests.
/// </summary>
public class CountryLookup : IClickHouseDictionary
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string IsoCode { get; set; } = string.Empty;
}

/// <summary>
/// Dictionary entity for composite key tests.
/// </summary>
public class RegionPricing : IClickHouseDictionary
{
    public string Region { get; set; } = string.Empty;
    public string ProductCategory { get; set; } = string.Empty;
    public decimal PriceMultiplier { get; set; }
}

/// <summary>
/// Source table for composite key dictionary.
/// </summary>
public class PricingRule
{
    public Guid Id { get; set; }
    public string Region { get; set; } = string.Empty;
    public string ProductCategory { get; set; } = string.Empty;
    public decimal PriceMultiplier { get; set; }
    public DateTime EffectiveDate { get; set; }
}

/// <summary>
/// Order entity for dictionary query translation tests.
/// </summary>
public class DictTestOrder
{
    public Guid Id { get; set; }
    public ulong CountryId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

#endregion

#region Configuration API Tests

public class DictionaryConfigurationTests
{
    [Fact]
    public void AsDictionary_WithKey_StoresAnnotations()
    {
        var builder = new ModelBuilder();

        builder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .UseHashedLayout());
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(CountryLookup))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value as bool?);
        Assert.Equal(new[] { "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns)?.Value);
        Assert.Equal(DictionaryLayout.Hashed, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);
        Assert.Equal("country", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySource)?.Value);
    }

    [Fact]
    public void AsDictionary_WithCompositeKey_StoresMultipleColumns()
    {
        var builder = new ModelBuilder();

        builder.Entity<PricingRule>(entity =>
        {
            entity.ToTable("pricing_rule");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Entity<RegionPricing>(entity =>
        {
            entity.AsDictionary<RegionPricing, PricingRule>(cfg => cfg
                .HasCompositeKey(x => new { x.Region, x.ProductCategory })
                .FromTable()
                .UseLayout(DictionaryLayout.ComplexKeyHashed));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(RegionPricing))!;

        var keyColumns = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns)?.Value as string[];
        Assert.NotNull(keyColumns);
        Assert.Equal(2, keyColumns.Length);
        Assert.Contains("Region", keyColumns);
        Assert.Contains("ProductCategory", keyColumns);
        Assert.Equal(DictionaryLayout.ComplexKeyHashed, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);
    }

    [Fact]
    public void AsDictionary_WithFlatLayout_StoresLayoutOptions()
    {
        var builder = new ModelBuilder();

        builder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .UseFlatLayout(opts => opts.MaxArraySize = 100000));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(CountryLookup))!;

        Assert.Equal(DictionaryLayout.Flat, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);
        var layoutOptions = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions)?.Value as Dictionary<string, object>;
        Assert.NotNull(layoutOptions);
        Assert.Equal(100000UL, layoutOptions["max_array_size"]);
    }

    [Fact]
    public void AsDictionary_WithCacheLayout_StoresSizeInCells()
    {
        var builder = new ModelBuilder();

        builder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .UseCacheLayout(opts => opts.SizeInCells = 50000));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(CountryLookup))!;

        Assert.Equal(DictionaryLayout.Cache, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);
        var layoutOptions = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions)?.Value as Dictionary<string, object>;
        Assert.NotNull(layoutOptions);
        Assert.Equal(50000L, layoutOptions["size_in_cells"]);
    }

    [Fact]
    public void AsDictionary_WithLifetime_StoresMinAndMax()
    {
        var builder = new ModelBuilder();

        builder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .HasLifetime(minSeconds: 60, maxSeconds: 600));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(CountryLookup))!;

        Assert.Equal(60, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin)?.Value);
        Assert.Equal(600, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax)?.Value);
    }

    [Fact]
    public void AsDictionary_WithNoAutoRefresh_SetsLifetimeToZero()
    {
        var builder = new ModelBuilder();

        builder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .HasNoAutoRefresh());
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(CountryLookup))!;

        Assert.Equal(0, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin)?.Value);
        Assert.Equal(0, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax)?.Value);
    }

    [Fact]
    public void AsDictionary_WithDefaults_StoresDefaultValues()
    {
        var builder = new ModelBuilder();

        builder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .HasDefault(x => x.Name, "Unknown")
                .HasDefault(x => x.IsoCode, "XX"));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(CountryLookup))!;

        var defaults = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryDefaults)?.Value as Dictionary<string, object>;
        Assert.NotNull(defaults);
        Assert.Equal("Unknown", defaults["Name"]);
        Assert.Equal("XX", defaults["IsoCode"]);
    }

    [Fact]
    public void AsDictionary_WithCustomName_OverridesDefault()
    {
        var builder = new ModelBuilder();

        builder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasName("custom_country_dict")
                .HasKey(x => x.Id)
                .FromTable());
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(CountryLookup))!;

        // The table name annotation should be set to the custom name
        Assert.Equal("custom_country_dict", entityType.GetTableName());
    }

    [Fact]
    public void AsDictionary_WithoutKey_ThrowsOnApply()
    {
        var builder = new ModelBuilder();

        builder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        // This should throw when Apply is called since no key is configured
        Assert.Throws<InvalidOperationException>(() =>
        {
            builder.Entity<CountryLookup>(entity =>
            {
                entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                    .FromTable());
                // No HasKey called - should throw
            });

            builder.FinalizeModel();
        });
    }
}

#endregion

#region DDL Generation Tests

public class DictionaryDdlTests
{
    [Fact]
    public void GenerateDictionary_BasicHashed_ProducesCorrectSql()
    {
        using var context = CreateDictionaryContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "country_lookup",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(ulong), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "IsoCode", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "country");
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.Hashed);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin, 0);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax, 300);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE DICTIONARY", sql);
        Assert.Contains("\"country_lookup\"", sql);
        Assert.Contains("PRIMARY KEY", sql);
        Assert.Contains("\"Id\"", sql);
        Assert.Contains("SOURCE(CLICKHOUSE(TABLE 'country'))", sql);
        Assert.Contains("LAYOUT(HASHED())", sql);
        // When min is 0, format is LIFETIME(max), not LIFETIME(MIN 0 MAX max)
        Assert.Contains("LIFETIME(300)", sql);
    }

    [Fact]
    public void GenerateDictionary_WithDefaults_IncludesDefaultClause()
    {
        using var context = CreateDictionaryContext();
        var generator = GetMigrationsSqlGenerator(context);

        var defaults = new Dictionary<string, object>
        {
            { "Name", "Unknown" },
            { "IsoCode", "XX" }
        };

        var operation = new CreateTableOperation
        {
            Name = "country_lookup",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(ulong), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "IsoCode", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "country");
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.Hashed);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryDefaults, defaults);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE DICTIONARY", sql);
        Assert.Contains("DEFAULT 'Unknown'", sql);
        Assert.Contains("DEFAULT 'XX'", sql);
    }

    [Fact]
    public void GenerateDictionary_CompositeKey_UsesTuple()
    {
        using var context = CreateDictionaryContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "region_pricing",
            Columns =
            {
                new AddColumnOperation { Name = "Region", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "ProductCategory", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "PriceMultiplier", ClrType = typeof(decimal), ColumnType = "Decimal(18, 4)" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "pricing_rule");
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Region", "ProductCategory" });
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.ComplexKeyHashed);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("CREATE DICTIONARY", sql);
        Assert.Contains("LAYOUT(COMPLEX_KEY_HASHED())", sql);
        Assert.Contains("\"Region\"", sql);
        Assert.Contains("\"ProductCategory\"", sql);
    }

    [Fact]
    public void GenerateDictionary_FlatLayout_ProducesCorrectLayoutClause()
    {
        using var context = CreateDictionaryContext();
        var generator = GetMigrationsSqlGenerator(context);

        var layoutOptions = new Dictionary<string, object>
        {
            { "max_array_size", 100000UL }
        };

        var operation = new CreateTableOperation
        {
            Name = "country_lookup",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(ulong), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "country");
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.Flat);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions, layoutOptions);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("LAYOUT(FLAT(MAX_ARRAY_SIZE 100000))", sql);
    }

    [Fact]
    public void GenerateDictionary_CacheLayout_ProducesSizeInCells()
    {
        using var context = CreateDictionaryContext();
        var generator = GetMigrationsSqlGenerator(context);

        var layoutOptions = new Dictionary<string, object>
        {
            { "size_in_cells", 50000L }
        };

        var operation = new CreateTableOperation
        {
            Name = "country_lookup",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(ulong), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "country");
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.Cache);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions, layoutOptions);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("LAYOUT(CACHE(SIZE_IN_CELLS 50000))", sql);
    }

    [Fact]
    public void GenerateDictionary_AllLayouts_ProducesCorrectLayoutClause()
    {
        var layouts = new[]
        {
            (DictionaryLayout.Flat, "LAYOUT(FLAT())"),
            (DictionaryLayout.Hashed, "LAYOUT(HASHED())"),
            (DictionaryLayout.HashedArray, "LAYOUT(HASHED_ARRAY())"),
            (DictionaryLayout.ComplexKeyHashed, "LAYOUT(COMPLEX_KEY_HASHED())"),
            (DictionaryLayout.ComplexKeyHashedArray, "LAYOUT(COMPLEX_KEY_HASHED_ARRAY())"),
            (DictionaryLayout.RangeHashed, "LAYOUT(RANGE_HASHED())"),
            (DictionaryLayout.Direct, "LAYOUT(DIRECT())")
        };

        foreach (var (layout, expectedLayoutClause) in layouts)
        {
            using var context = CreateDictionaryContext();
            var generator = GetMigrationsSqlGenerator(context);

            var operation = new CreateTableOperation
            {
                Name = "test_dict",
                Columns =
                {
                    new AddColumnOperation { Name = "Id", ClrType = typeof(ulong), ColumnType = "UInt64" },
                    new AddColumnOperation { Name = "Value", ClrType = typeof(string), ColumnType = "String" }
                }
            };

            operation.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
            operation.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "source_table");
            operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Id" });
            operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, layout);

            var sql = GenerateSql(generator, operation);

            Assert.Contains(expectedLayoutClause, sql);
        }
    }

    [Fact]
    public void GenerateDictionary_WithLifetimeRange_ProducesCorrectLifetimeClause()
    {
        using var context = CreateDictionaryContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "country_lookup",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(ulong), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "country");
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.Hashed);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin, 60);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax, 600);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("LIFETIME(MIN 60 MAX 600)", sql);
    }

    [Fact]
    public void GenerateDictionary_WithZeroLifetime_ProducesLifetimeZero()
    {
        using var context = CreateDictionaryContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "country_lookup",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(ulong), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, "country");
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns, new[] { "Id" });
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.Hashed);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin, 0);
        operation.AddAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax, 0);

        var sql = GenerateSql(generator, operation);

        Assert.Contains("LIFETIME(0)", sql);
    }

    private static DictionaryTestContext CreateDictionaryContext()
    {
        var options = new DbContextOptionsBuilder<DictionaryTestContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new DictionaryTestContext(options);
    }

    private static IMigrationsSqlGenerator GetMigrationsSqlGenerator(DbContext context)
    {
        return ((IInfrastructure<IServiceProvider>)context).Instance.GetService<IMigrationsSqlGenerator>()!;
    }

    private static string GenerateSql(IMigrationsSqlGenerator generator, MigrationOperation operation)
    {
        var commands = generator.Generate(new[] { operation });
        return string.Join("\n", commands.Select(c => c.CommandText));
    }
}

public class DictionaryTestContext : DbContext
{
    public DictionaryTestContext(DbContextOptions<DictionaryTestContext> options) : base(options)
    {
    }

    public DbSet<Country> Countries => Set<Country>();
    public DbSet<CountryLookup> CountryLookups => Set<CountryLookup>();
    public DbSet<DictTestOrder> DictTestOrders => Set<DictTestOrder>();

    // Dictionary accessor for LINQ query translation
    private ClickHouseDictionary<CountryLookup, ulong>? _countryDict;
    public ClickHouseDictionary<CountryLookup, ulong> CountryDict
        => _countryDict ??= new ClickHouseDictionary<CountryLookup, ulong>(this);

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Country>(entity =>
        {
            entity.ToTable("country");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        modelBuilder.Entity<CountryLookup>(entity =>
        {
            entity.AsDictionary<CountryLookup, Country>(cfg => cfg
                .HasKey(x => x.Id)
                .FromTable()
                .UseHashedLayout()
                .HasLifetime(300));
        });

        modelBuilder.Entity<DictTestOrder>(entity =>
        {
            entity.ToTable("dict_test_order");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
        });
    }
}

#endregion

#region Dictionary Layout Tests

public class DictionaryLayoutTests
{
    [Fact]
    public void DictionaryLayout_HasAllExpectedValues()
    {
        var layouts = Enum.GetValues<DictionaryLayout>();

        Assert.Contains(DictionaryLayout.Flat, layouts);
        Assert.Contains(DictionaryLayout.Hashed, layouts);
        Assert.Contains(DictionaryLayout.HashedArray, layouts);
        Assert.Contains(DictionaryLayout.ComplexKeyHashed, layouts);
        Assert.Contains(DictionaryLayout.ComplexKeyHashedArray, layouts);
        Assert.Contains(DictionaryLayout.RangeHashed, layouts);
        Assert.Contains(DictionaryLayout.Cache, layouts);
        Assert.Contains(DictionaryLayout.Direct, layouts);
    }

    [Fact]
    public void FlatLayoutOptions_CanSetMaxArraySize()
    {
        var options = new FlatLayoutOptions
        {
            MaxArraySize = 500000
        };

        Assert.Equal(500000UL, options.MaxArraySize);
    }

    [Fact]
    public void HashedLayoutOptions_CanSetShardCountAndSparse()
    {
        var options = new HashedLayoutOptions
        {
            ShardCount = 4,
            Sparse = true
        };

        Assert.Equal(4, options.ShardCount);
        Assert.True(options.Sparse);
    }

    [Fact]
    public void CacheLayoutOptions_RequiresSizeInCells()
    {
        var options = new CacheLayoutOptions
        {
            SizeInCells = 100000
        };

        Assert.Equal(100000, options.SizeInCells);
    }
}

#endregion

#region Dictionary Metadata Tests

public class DictionaryMetadataTests
{
    [Fact]
    public void DictionaryMetadata_StoresCorrectProperties()
    {
        var metadata = new DictionaryMetadata<CountryLookup, ulong>(
            name: "country_lookup",
            keyType: typeof(ulong),
            entityType: typeof(CountryLookup),
            keyPropertyName: "Id"
        );

        Assert.Equal("country_lookup", metadata.Name);
        Assert.Equal(typeof(ulong), metadata.KeyType);
        Assert.Equal(typeof(CountryLookup), metadata.EntityType);
        Assert.Equal("Id", metadata.KeyPropertyName);
    }
}

#endregion

#region ClickHouseDictionary Runtime Tests

public class ClickHouseDictionaryRuntimeTests
{
    [Fact]
    public void Constructor_WithContext_ResolvesMetadataFromModel()
    {
        using var context = CreateContext();

        // Use the new runtime discovery constructor
        var dict = new ClickHouseDictionary<CountryLookup, ulong>(context);

        // Should resolve the name from the model annotations
        Assert.Equal("country_lookup", dict.Name);
    }

    [Fact]
    public void Constructor_WithNonDictionaryEntity_ThrowsInvalidOperationException()
    {
        using var context = CreateContext();

        // Country is not configured as a dictionary
        var exception = Assert.Throws<InvalidOperationException>(() =>
            new ClickHouseDictionary<Country, ulong>(context));

        Assert.Contains("not configured as a dictionary", exception.Message);
    }

    [Fact]
    public void Get_ThrowsInvalidOperationException_WhenCalledDirectly()
    {
        using var context = CreateContext();
        var metadata = new DictionaryMetadata<CountryLookup, ulong>(
            "country_lookup", typeof(ulong), typeof(CountryLookup), "Id");
        var dict = new ClickHouseDictionary<CountryLookup, ulong>(context, metadata);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            dict.Get(1UL, x => x.Name));

        Assert.Contains("LINQ expressions", exception.Message);
    }

    [Fact]
    public void GetOrDefault_ThrowsInvalidOperationException_WhenCalledDirectly()
    {
        using var context = CreateContext();
        var metadata = new DictionaryMetadata<CountryLookup, ulong>(
            "country_lookup", typeof(ulong), typeof(CountryLookup), "Id");
        var dict = new ClickHouseDictionary<CountryLookup, ulong>(context, metadata);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            dict.GetOrDefault(1UL, x => x.Name, "Default"));

        Assert.Contains("LINQ expressions", exception.Message);
    }

    [Fact]
    public void ContainsKey_ThrowsInvalidOperationException_WhenCalledDirectly()
    {
        using var context = CreateContext();
        var metadata = new DictionaryMetadata<CountryLookup, ulong>(
            "country_lookup", typeof(ulong), typeof(CountryLookup), "Id");
        var dict = new ClickHouseDictionary<CountryLookup, ulong>(context, metadata);

        var exception = Assert.Throws<InvalidOperationException>(() =>
            dict.ContainsKey(1UL));

        Assert.Contains("LINQ expressions", exception.Message);
    }

    [Fact]
    public void Dictionary_HasCorrectName()
    {
        using var context = CreateContext();
        var metadata = new DictionaryMetadata<CountryLookup, ulong>(
            "country_lookup", typeof(ulong), typeof(CountryLookup), "Id");
        var dict = new ClickHouseDictionary<CountryLookup, ulong>(context, metadata);

        Assert.Equal("country_lookup", dict.Name);
    }

    private static DictionaryTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<DictionaryTestContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new DictionaryTestContext(options);
    }
}

#endregion

#region Marker Interface Tests

public class MarkerInterfaceTests
{
    [Fact]
    public void CountryLookup_ImplementsIClickHouseDictionary()
    {
        var entity = new CountryLookup();
        Assert.IsAssignableFrom<IClickHouseDictionary>(entity);
    }

    [Fact]
    public void RegionPricing_ImplementsIClickHouseDictionary()
    {
        var entity = new RegionPricing();
        Assert.IsAssignableFrom<IClickHouseDictionary>(entity);
    }

    [Fact]
    public void Country_DoesNotImplementIClickHouseDictionary()
    {
        var entity = new Country();
        Assert.False(entity is IClickHouseDictionary);
    }
}

#endregion

#region SQL Translation Tests

public class DictionarySqlTranslationTests
{
    [Fact]
    public void Get_TranslatesToDictGet_WithCorrectAttributeName()
    {
        using var context = CreateContext();

        var query = context.DictTestOrders
            .Select(o => new
            {
                o.Id,
                CountryName = context.CountryDict.Get(o.CountryId, c => c.Name)
            });

        var sql = query.ToQueryString();

        Assert.Contains("dictGet('country_lookup', 'Name',", sql);
    }

    [Fact]
    public void Get_TranslatesMultipleAttributes()
    {
        using var context = CreateContext();

        var query = context.DictTestOrders
            .Select(o => new
            {
                o.Id,
                CountryName = context.CountryDict.Get(o.CountryId, c => c.Name),
                CountryCode = context.CountryDict.Get(o.CountryId, c => c.IsoCode)
            });

        var sql = query.ToQueryString();

        Assert.Contains("dictGet('country_lookup', 'Name',", sql);
        Assert.Contains("dictGet('country_lookup', 'IsoCode',", sql);
    }

    [Fact]
    public void GetOrDefault_TranslatesToDictGetOrDefault()
    {
        using var context = CreateContext();

        var query = context.DictTestOrders
            .Select(o => new
            {
                o.Id,
                CountryName = context.CountryDict.GetOrDefault(o.CountryId, c => c.Name, "Unknown")
            });

        var sql = query.ToQueryString();

        Assert.Contains("dictGetOrDefault('country_lookup', 'Name',", sql);
        Assert.Contains("'Unknown'", sql);
    }

    [Fact]
    public void ContainsKey_TranslatesToDictHas()
    {
        using var context = CreateContext();

        var query = context.DictTestOrders
            .Where(o => context.CountryDict.ContainsKey(o.CountryId));

        var sql = query.ToQueryString();

        Assert.Contains("dictHas('country_lookup',", sql);
    }

    [Fact]
    public void ContainsKey_InProjection_TranslatesToDictHas()
    {
        using var context = CreateContext();

        var query = context.DictTestOrders
            .Select(o => new
            {
                o.Id,
                HasCountry = context.CountryDict.ContainsKey(o.CountryId)
            });

        var sql = query.ToQueryString();

        Assert.Contains("dictHas('country_lookup',", sql);
    }

    private static DictionaryTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<DictionaryTestContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new DictionaryTestContext(options);
    }
}

#endregion
