using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.External;
using EF.CH.Metadata;
using EF.CH.Migrations.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EF.CH.Tests;

#region Test Entities for External Tables

/// <summary>
/// External PostgreSQL entity for testing.
/// </summary>
public class ExternalCustomer
{
    public Guid ExternalId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// External PostgreSQL entity with all properties for testing.
/// </summary>
public class ExternalProduct
{
    public int ProductId { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

/// <summary>
/// Local ClickHouse entity for JOIN tests.
/// </summary>
public class LocalOrder
{
    public Guid Id { get; set; }
    public Guid CustomerId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

/// <summary>
/// Dictionary entity for standalone API tests.
/// </summary>
public class ProductCategory : IClickHouseDictionary
{
    public uint Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
}

/// <summary>
/// Source entity for dictionary tests.
/// </summary>
public class CategorySource
{
    public uint Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public bool IsActive { get; set; }
}

#endregion

#region External Entity Configuration Tests

public class ExternalEntityConfigurationTests
{
    [Fact]
    public void ExternalPostgresEntity_StoresIsExternalAnnotation()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)?.Value as bool?);
        Assert.Equal("postgresql", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)?.Value);
    }

    [Fact]
    public void ExternalPostgresEntity_StoresTableAndSchema()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("my_customers", "sales")
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        Assert.Equal("my_customers", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalTable)?.Value);
        Assert.Equal("sales", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalSchema)?.Value);
    }

    [Fact]
    public void ExternalPostgresEntity_DefaultsToSnakeCaseTableName()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        Assert.Equal("external_customer", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalTable)?.Value);
        Assert.Equal("public", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalSchema)?.Value);
    }

    [Fact]
    public void ExternalPostgresEntity_StoresLiteralConnectionValues()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "pg.example.com:5432")
                .Database(value: "production")
                .User(value: "readonly_user")
                .Password(value: "secret123")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        Assert.Equal("pg.example.com:5432", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue)?.Value);
        Assert.Equal("production", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalDatabaseValue)?.Value);
        Assert.Equal("readonly_user", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUserValue)?.Value);
        Assert.Equal("secret123", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordValue)?.Value);
    }

    [Fact]
    public void ExternalPostgresEntity_StoresEnvironmentVariableReferences()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(env: "PG_HOST")
                .Database(env: "PG_DATABASE")
                .Credentials("PG_USER", "PG_PASSWORD")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        Assert.Equal("PG_HOST", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv)?.Value);
        Assert.Equal("PG_DATABASE", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalDatabaseEnv)?.Value);
        Assert.Equal("PG_USER", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUserEnv)?.Value);
        Assert.Equal("PG_PASSWORD", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordEnv)?.Value);
    }

    [Fact]
    public void ExternalPostgresEntity_StoresProfileName()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c.UseProfile("production-pg")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        Assert.Equal("production-pg", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile)?.Value);
    }

    [Fact]
    public void ExternalPostgresEntity_IsReadOnlyByDefault()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        // ReadOnly annotation should be true (meaning inserts are NOT allowed)
        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalReadOnly)?.Value as bool?);
    }

    [Fact]
    public void ExternalPostgresEntity_AllowInsertsDisablesReadOnly()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .AllowInserts()
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        // ReadOnly annotation should be false (meaning inserts ARE allowed)
        Assert.False(entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalReadOnly)?.Value as bool?);
    }

    [Fact]
    public void ExternalPostgresEntity_MarksEntityAsKeyless()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        // External entities must be keyless
        Assert.Empty(entityType.GetKeys());
    }
}

#endregion

#region External Config Resolver Tests

public class ExternalConfigResolverTests
{
    [Fact]
    public void IsExternalTableFunction_ReturnsTrueForExternalEntity()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver();
        Assert.True(resolver.IsExternalTableFunction(entityType));
    }

    [Fact]
    public void IsExternalTableFunction_ReturnsFalseForRegularEntity()
    {
        var builder = new ModelBuilder();

        builder.Entity<LocalOrder>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(LocalOrder))!;

        var resolver = new ExternalConfigResolver();
        Assert.False(resolver.IsExternalTableFunction(entityType));
    }

    [Fact]
    public void AreInsertsEnabled_ReturnsFalseForReadOnlyEntity()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .ReadOnly()
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver();
        Assert.False(resolver.AreInsertsEnabled(entityType));
    }

    [Fact]
    public void AreInsertsEnabled_ReturnsTrueWhenAllowInserts()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .AllowInserts()
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver();
        Assert.True(resolver.AreInsertsEnabled(entityType));
    }

    [Fact]
    public void ResolvePostgresTableFunction_GeneratesCorrectSqlWithLiteralValues()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers", "sales")
            .Connection(c => c
                .HostPort(value: "pg.example.com:5432")
                .Database(value: "production")
                .User(value: "readonly")
                .Password(value: "secret")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolvePostgresTableFunction(entityType);

        Assert.Equal(
            "postgresql('pg.example.com:5432', 'production', 'customers', 'readonly', 'secret', 'sales')",
            sql);
    }

    [Fact]
    public void ResolvePostgresTableFunction_EscapesSingleQuotes()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customer's_table")
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "db'name")
                .User(value: "user'name")
                .Password(value: "pass'word")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolvePostgresTableFunction(entityType);

        Assert.Contains("\\'", sql);
        Assert.Contains("customer\\'s_table", sql);
    }

    [Fact]
    public void ResolvePostgresTableFunction_ResolvesFromConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["TEST_PG_HOST"] = "config-host:5432",
                ["TEST_PG_DB"] = "config-db",
                ["TEST_PG_USER"] = "config-user",
                ["TEST_PG_PASS"] = "config-pass"
            })
            .Build();

        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(env: "TEST_PG_HOST")
                .Database(env: "TEST_PG_DB")
                .User(env: "TEST_PG_USER")
                .Password(env: "TEST_PG_PASS")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver(configuration);
        var sql = resolver.ResolvePostgresTableFunction(entityType);

        Assert.Contains("config-host:5432", sql);
        Assert.Contains("config-db", sql);
        Assert.Contains("config-user", sql);
        Assert.Contains("config-pass", sql);
    }

    [Fact]
    public void ResolvePostgresTableFunction_ResolvesFromProfile()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ExternalConnections:production-pg:HostPort"] = "prod.example.com:5432",
                ["ExternalConnections:production-pg:Database"] = "prod_db",
                ["ExternalConnections:production-pg:User"] = "prod_user",
                ["ExternalConnections:production-pg:Password"] = "prod_secret",
                ["ExternalConnections:production-pg:Schema"] = "app"
            })
            .Build();

        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c.UseProfile("production-pg")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver(configuration);
        var sql = resolver.ResolvePostgresTableFunction(entityType);

        Assert.Equal(
            "postgresql('prod.example.com:5432', 'prod_db', 'customers', 'prod_user', 'prod_secret', 'app')",
            sql);
    }

    [Fact]
    public void ResolvePostgresTableFunction_ThrowsForMissingConfiguration()
    {
        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(env: "NONEXISTENT_ENV_VAR")
                .Database(value: "db")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            resolver.ResolvePostgresTableFunction(entityType));

        Assert.Contains("NONEXISTENT_ENV_VAR", ex.Message);
    }

    [Fact]
    public void ResolvePostgresTableFunction_ThrowsForMissingProfile()
    {
        var configuration = new ConfigurationBuilder().Build();

        var builder = new ModelBuilder();

        builder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c.UseProfile("nonexistent-profile")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver(configuration);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            resolver.ResolvePostgresTableFunction(entityType));

        Assert.Contains("nonexistent-profile", ex.Message);
    }

    [Fact]
    public void ResolvePostgresTableFunction_ThrowsForUnsupportedProvider()
    {
        var builder = new ModelBuilder();

        // Manually set an unsupported provider
        builder.Entity<ExternalCustomer>(entity =>
        {
            entity.HasNoKey();
            entity.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
            entity.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "mysql");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalCustomer))!;

        var resolver = new ExternalConfigResolver();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            resolver.ResolvePostgresTableFunction(entityType));

        Assert.Contains("mysql", ex.Message);
        Assert.Contains("unsupported", ex.Message.ToLower());
    }
}

#endregion

#region External Entity DDL Tests

public class ExternalEntityDdlTests
{
    [Fact]
    public void GenerateCreateTable_SkipsExternalEntity()
    {
        using var context = CreateTestContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "external_customer",
            Columns =
            {
                new AddColumnOperation { Name = "ExternalId", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Name", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Email", ClrType = typeof(string), ColumnType = "String" }
            }
        };

        // Mark as external entity
        operation.AddAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);

        var sql = GenerateSql(generator, operation);

        // Should be empty - no DDL generated for external entities
        Assert.True(string.IsNullOrWhiteSpace(sql));
    }

    [Fact]
    public void GenerateCreateTable_GeneratesDdlForRegularEntity()
    {
        using var context = CreateTestContext();
        var generator = GetMigrationsSqlGenerator(context);

        var operation = new CreateTableOperation
        {
            Name = "local_order",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(Guid), ColumnType = "UUID" },
                new AddColumnOperation { Name = "Amount", ClrType = typeof(decimal), ColumnType = "Decimal(18, 4)" }
            }
        };

        operation.AddAnnotation(ClickHouseAnnotationNames.Engine, "MergeTree");
        operation.AddAnnotation(ClickHouseAnnotationNames.OrderBy, new[] { "Id" });

        var sql = GenerateSql(generator, operation);

        // Should generate DDL for regular entities
        Assert.Contains("CREATE TABLE", sql);
        Assert.Contains("local_order", sql);
    }

    private static ExternalEntityTestContext CreateTestContext()
    {
        var options = new DbContextOptionsBuilder<ExternalEntityTestContext>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;

        return new ExternalEntityTestContext(options);
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

public class ExternalEntityTestContext : DbContext
{
    public ExternalEntityTestContext(DbContextOptions<ExternalEntityTestContext> options) : base(options)
    {
    }

    public DbSet<ExternalCustomer> ExternalCustomers => Set<ExternalCustomer>();
    public DbSet<LocalOrder> LocalOrders => Set<LocalOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ExternalPostgresEntity<ExternalCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "localhost:5432")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        modelBuilder.Entity<LocalOrder>(entity =>
        {
            entity.ToTable("local_order");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
        });
    }
}

#endregion

#region Standalone Dictionary API Tests

public class StandaloneDictionaryApiTests
{
    [Fact]
    public void Dictionary_StoresIsDictionaryAnnotation()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .Layout(DictionaryLayout.Hashed)
            .Lifetime(300));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value as bool?);
    }

    [Fact]
    public void Dictionary_StoresPrimaryKeyColumn()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .Layout(DictionaryLayout.Hashed));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        var keyColumns = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns)?.Value as string[];
        Assert.NotNull(keyColumns);
        Assert.Single(keyColumns);
        Assert.Equal("Id", keyColumns[0]);
    }

    [Fact]
    public void Dictionary_StoresLayout()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .Layout(DictionaryLayout.Flat));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        Assert.Equal(DictionaryLayout.Flat, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);
    }

    [Fact]
    public void Dictionary_UseFlatLayoutSetsLayoutAndOptions()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .UseFlatLayout(50000));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        Assert.Equal(DictionaryLayout.Flat, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);

        var layoutOptions = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions)?.Value as Dictionary<string, object>;
        Assert.NotNull(layoutOptions);
        Assert.Equal(50000, layoutOptions["max_array_size"]);
    }

    [Fact]
    public void Dictionary_UseCacheLayoutSetsLayoutAndSize()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .UseCacheLayout(100000));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        Assert.Equal(DictionaryLayout.Cache, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);

        var layoutOptions = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayoutOptions)?.Value as Dictionary<string, object>;
        Assert.NotNull(layoutOptions);
        Assert.Equal(100000, layoutOptions["size_in_cells"]);
    }

    [Fact]
    public void Dictionary_StoresLifetime()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .UseHashedLayout()
            .Lifetime(600));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        Assert.Equal(0, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin)?.Value);
        Assert.Equal(600, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax)?.Value);
    }

    [Fact]
    public void Dictionary_StoresLifetimeMinMax()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .UseHashedLayout()
            .Lifetime(60, 600));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        Assert.Equal(60, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin)?.Value);
        Assert.Equal(600, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax)?.Value);
    }

    [Fact]
    public void Dictionary_ManualReloadOnlySetsLifetimeToZero()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .UseHashedLayout()
            .ManualReloadOnly());

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        Assert.Equal(0, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin)?.Value);
        Assert.Equal(0, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax)?.Value);
    }

    [Fact]
    public void Dictionary_StoresSourceTable()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .UseHashedLayout());

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        // Source table is derived from TSource type name (snake_case)
        Assert.Equal("category_source", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySource)?.Value);
    }

    [Fact]
    public void Dictionary_StoresDefaults()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .UseHashedLayout()
            .HasDefault(c => c.Name, "Unknown")
            .HasDefault(c => c.Description, "No description"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        var defaults = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryDefaults)?.Value as Dictionary<string, object>;
        Assert.NotNull(defaults);
        Assert.Equal("Unknown", defaults["Name"]);
        Assert.Equal("No description", defaults["Description"]);
    }

    [Fact]
    public void Dictionary_SetsTableName()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .UseHashedLayout());

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        Assert.Equal("product_category_dict", entityType.GetTableName());
    }

    [Fact]
    public void Dictionary_MarksAsKeyless()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
            .PrimaryKey(c => c.Id)
            .UseHashedLayout());

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ProductCategory))!;

        // Dictionaries are keyless in EF Core
        Assert.Empty(entityType.GetKeys());
    }

    [Fact]
    public void Dictionary_ThrowsWithoutPrimaryKey()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        Assert.Throws<InvalidOperationException>(() =>
        {
            builder.Dictionary<ProductCategory, CategorySource>("product_category_dict", cfg => cfg
                .UseHashedLayout());
            builder.FinalizeModel();
        });
    }

    [Fact]
    public void Dictionary_SupportsCompositePrimaryKey()
    {
        var builder = new ModelBuilder();

        builder.Entity<CategorySource>(entity =>
        {
            entity.ToTable("category_source");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => x.Id);
        });

        // CompositeKey requires an anonymous type
        builder.Dictionary<RegionPricing, PricingRule>("region_pricing_dict", cfg => cfg
            .CompositePrimaryKey(r => new { r.Region, r.ProductCategory })
            .Layout(DictionaryLayout.ComplexKeyHashed));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(RegionPricing))!;

        var keyColumns = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns)?.Value as string[];
        Assert.NotNull(keyColumns);
        Assert.Equal(2, keyColumns.Length);
        Assert.Contains("Region", keyColumns);
        Assert.Contains("ProductCategory", keyColumns);
    }
}

#endregion
