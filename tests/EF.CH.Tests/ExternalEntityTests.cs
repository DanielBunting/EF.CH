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

#region MySQL External Entity Test Entities

/// <summary>
/// External MySQL entity for testing.
/// </summary>
public class ExternalMySqlCustomer
{
    public int CustomerId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// External MySQL entity for INSERT tests with replace_query support.
/// </summary>
public class ExternalMySqlInventory
{
    public int ProductId { get; set; }
    public int Quantity { get; set; }
    public DateTime LastUpdated { get; set; }
}

#endregion

#region ODBC External Entity Test Entities

/// <summary>
/// External ODBC entity for testing (e.g., MSSQL).
/// </summary>
public class ExternalOdbcSalesData
{
    public long SaleId { get; set; }
    public string Region { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public DateTime SaleDate { get; set; }
}

#endregion

#region Redis External Entity Test Entities

/// <summary>
/// External Redis entity for testing.
/// </summary>
public class ExternalRedisSession
{
    public string SessionId { get; set; } = string.Empty;
    public ulong UserId { get; set; }
    public string Data { get; set; } = string.Empty;
    public DateTime ExpiresAt { get; set; }
}

/// <summary>
/// Redis entity with various types for structure generation testing.
/// </summary>
public class ExternalRedisMetrics
{
    public string Key { get; set; } = string.Empty;
    public int Count { get; set; }
    public long TotalBytes { get; set; }
    public double AverageLatency { get; set; }
    public bool IsActive { get; set; }
}

#endregion

#region MySQL External Entity Configuration Tests

public class ExternalMySqlEntityConfigurationTests
{
    [Fact]
    public void ExternalMySqlEntity_StoresIsExternalAnnotation()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)?.Value as bool?);
        Assert.Equal("mysql", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)?.Value);
    }

    [Fact]
    public void ExternalMySqlEntity_StoresTableName()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("my_customers")
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        Assert.Equal("my_customers", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalTable)?.Value);
    }

    [Fact]
    public void ExternalMySqlEntity_DefaultsToSnakeCaseTableName()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        Assert.Equal("external_my_sql_customer", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalTable)?.Value);
    }

    [Fact]
    public void ExternalMySqlEntity_StoresLiteralConnectionValues()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "mysql.example.com:3306")
                .Database(value: "production")
                .User(value: "readonly_user")
                .Password(value: "secret123")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        Assert.Equal("mysql.example.com:3306", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue)?.Value);
        Assert.Equal("production", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalDatabaseValue)?.Value);
        Assert.Equal("readonly_user", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUserValue)?.Value);
        Assert.Equal("secret123", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordValue)?.Value);
    }

    [Fact]
    public void ExternalMySqlEntity_StoresEnvironmentVariableReferences()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(env: "MYSQL_HOST")
                .Database(env: "MYSQL_DATABASE")
                .Credentials("MYSQL_USER", "MYSQL_PASSWORD")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        Assert.Equal("MYSQL_HOST", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv)?.Value);
        Assert.Equal("MYSQL_DATABASE", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalDatabaseEnv)?.Value);
        Assert.Equal("MYSQL_USER", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalUserEnv)?.Value);
        Assert.Equal("MYSQL_PASSWORD", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordEnv)?.Value);
    }

    [Fact]
    public void ExternalMySqlEntity_StoresProfileName()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c.UseProfile("production-mysql")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        Assert.Equal("production-mysql", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile)?.Value);
    }

    [Fact]
    public void ExternalMySqlEntity_IsReadOnlyByDefault()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalReadOnly)?.Value as bool?);
    }

    [Fact]
    public void ExternalMySqlEntity_AllowInsertsDisablesReadOnly()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .AllowInserts()
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        Assert.False(entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalReadOnly)?.Value as bool?);
    }

    [Fact]
    public void ExternalMySqlEntity_UseReplaceForInserts_StoresAnnotation()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlInventory>(ext => ext
            .FromTable("inventory")
            .AllowInserts()
            .UseReplaceForInserts()
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlInventory))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalMySqlReplaceQuery)?.Value as bool?);
    }

    [Fact]
    public void ExternalMySqlEntity_OnDuplicateKey_StoresClause()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlInventory>(ext => ext
            .FromTable("inventory")
            .AllowInserts()
            .OnDuplicateKey("UPDATE quantity = VALUES(quantity)")
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlInventory))!;

        Assert.Equal("UPDATE quantity = VALUES(quantity)",
            entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalMySqlOnDuplicateClause)?.Value);
    }

    [Fact]
    public void ExternalMySqlEntity_MarksEntityAsKeyless()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        Assert.Empty(entityType.GetKeys());
    }
}

#endregion

#region MySQL External Config Resolver Tests

public class ExternalMySqlConfigResolverTests
{
    [Fact]
    public void ResolveMySqlTableFunction_GeneratesCorrectSqlWithLiteralValues()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "mysql.example.com:3306")
                .Database(value: "production")
                .User(value: "readonly")
                .Password(value: "secret")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveMySqlTableFunction(entityType);

        Assert.Equal(
            "mysql('mysql.example.com:3306', 'production', 'customers', 'readonly', 'secret')",
            sql);
    }

    [Fact]
    public void ResolveMySqlTableFunction_EscapesSingleQuotes()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customer's_table")
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "db'name")
                .User(value: "user'name")
                .Password(value: "pass'word")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveMySqlTableFunction(entityType);

        Assert.Contains("\\'", sql);
        Assert.Contains("customer\\'s_table", sql);
    }

    [Fact]
    public void ResolveMySqlTableFunction_ResolvesFromConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["TEST_MYSQL_HOST"] = "config-host:3306",
                ["TEST_MYSQL_DB"] = "config-db",
                ["TEST_MYSQL_USER"] = "config-user",
                ["TEST_MYSQL_PASS"] = "config-pass"
            })
            .Build();

        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(env: "TEST_MYSQL_HOST")
                .Database(env: "TEST_MYSQL_DB")
                .User(env: "TEST_MYSQL_USER")
                .Password(env: "TEST_MYSQL_PASS")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        var resolver = new ExternalConfigResolver(configuration);
        var sql = resolver.ResolveMySqlTableFunction(entityType);

        Assert.Contains("config-host:3306", sql);
        Assert.Contains("config-db", sql);
        Assert.Contains("config-user", sql);
        Assert.Contains("config-pass", sql);
    }

    [Fact]
    public void ResolveMySqlTableFunction_ResolvesFromProfile()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ExternalConnections:production-mysql:HostPort"] = "prod.mysql.com:3306",
                ["ExternalConnections:production-mysql:Database"] = "prod_db",
                ["ExternalConnections:production-mysql:User"] = "prod_user",
                ["ExternalConnections:production-mysql:Password"] = "prod_secret"
            })
            .Build();

        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c.UseProfile("production-mysql")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        var resolver = new ExternalConfigResolver(configuration);
        var sql = resolver.ResolveMySqlTableFunction(entityType);

        Assert.Equal(
            "mysql('prod.mysql.com:3306', 'prod_db', 'customers', 'prod_user', 'prod_secret')",
            sql);
    }

    [Fact]
    public void ResolveMySqlTableFunction_ThrowsForWrongProvider()
    {
        var builder = new ModelBuilder();

        // Manually set PostgreSQL provider but try to resolve as MySQL
        builder.Entity<ExternalMySqlCustomer>(entity =>
        {
            entity.HasNoKey();
            entity.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
            entity.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "postgresql");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        var resolver = new ExternalConfigResolver();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            resolver.ResolveMySqlTableFunction(entityType));

        Assert.Contains("postgresql", ex.Message);
        Assert.Contains("mysql", ex.Message.ToLower());
    }

    [Fact]
    public void ResolveTableFunction_DispatchesToMySql()
    {
        var builder = new ModelBuilder();

        builder.ExternalMySqlEntity<ExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(value: "localhost:3306")
                .Database(value: "testdb")
                .User(value: "user")
                .Password(value: "pass")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalMySqlCustomer))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveTableFunction(entityType);

        Assert.StartsWith("mysql(", sql);
    }
}

#endregion

#region ODBC External Entity Configuration Tests

public class ExternalOdbcEntityConfigurationTests
{
    [Fact]
    public void ExternalOdbcEntity_StoresIsExternalAnnotation()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(value: "MsSqlProd")
            .Database("reporting"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)?.Value as bool?);
        Assert.Equal("odbc", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)?.Value);
    }

    [Fact]
    public void ExternalOdbcEntity_StoresTableName()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales_data")
            .Dsn(value: "MsSqlProd"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        Assert.Equal("sales_data", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalTable)?.Value);
    }

    [Fact]
    public void ExternalOdbcEntity_DefaultsToSnakeCaseTableName()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .Dsn(value: "MsSqlProd"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        Assert.Equal("external_odbc_sales_data", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalTable)?.Value);
    }

    [Fact]
    public void ExternalOdbcEntity_StoresLiteralDsnValue()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(value: "MyMsSqlDSN"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        Assert.Equal("MyMsSqlDSN", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalOdbcDsnValue)?.Value);
    }

    [Fact]
    public void ExternalOdbcEntity_StoresDsnEnvironmentVariable()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(env: "MSSQL_DSN"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        Assert.Equal("MSSQL_DSN", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalOdbcDsnEnv)?.Value);
    }

    [Fact]
    public void ExternalOdbcEntity_StoresDatabaseName()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(value: "MsSqlProd")
            .Database("reporting"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        Assert.Equal("reporting", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalDatabaseValue)?.Value);
    }

    [Fact]
    public void ExternalOdbcEntity_IsReadOnlyByDefault()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(value: "MsSqlProd"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalReadOnly)?.Value as bool?);
    }

    [Fact]
    public void ExternalOdbcEntity_AllowInsertsDisablesReadOnly()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(value: "MsSqlProd")
            .AllowInserts());

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        Assert.False(entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalReadOnly)?.Value as bool?);
    }

    [Fact]
    public void ExternalOdbcEntity_MarksEntityAsKeyless()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(value: "MsSqlProd"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        Assert.Empty(entityType.GetKeys());
    }
}

#endregion

#region ODBC External Config Resolver Tests

public class ExternalOdbcConfigResolverTests
{
    [Fact]
    public void ResolveOdbcTableFunction_GeneratesCorrectSqlWithLiteralValues()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(value: "MsSqlProd")
            .Database("reporting"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveOdbcTableFunction(entityType);

        Assert.Equal("odbc('MsSqlProd', 'reporting', 'sales')", sql);
    }

    [Fact]
    public void ResolveOdbcTableFunction_HandlesEmptyDatabase()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(value: "MsSqlProd"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveOdbcTableFunction(entityType);

        Assert.Equal("odbc('MsSqlProd', '', 'sales')", sql);
    }

    [Fact]
    public void ResolveOdbcTableFunction_EscapesSingleQuotes()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sale's_table")
            .Dsn(value: "DSN'Name")
            .Database("db'name"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveOdbcTableFunction(entityType);

        Assert.Contains("\\'", sql);
        Assert.Contains("sale\\'s_table", sql);
        Assert.Contains("DSN\\'Name", sql);
    }

    [Fact]
    public void ResolveOdbcTableFunction_ResolvesFromConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["TEST_MSSQL_DSN"] = "ConfigDSN"
            })
            .Build();

        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(env: "TEST_MSSQL_DSN")
            .Database("reporting"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        var resolver = new ExternalConfigResolver(configuration);
        var sql = resolver.ResolveOdbcTableFunction(entityType);

        Assert.Equal("odbc('ConfigDSN', 'reporting', 'sales')", sql);
    }

    [Fact]
    public void ResolveOdbcTableFunction_ThrowsForMissingDsn()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(env: "NONEXISTENT_DSN_VAR"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        var resolver = new ExternalConfigResolver();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            resolver.ResolveOdbcTableFunction(entityType));

        Assert.Contains("NONEXISTENT_DSN_VAR", ex.Message);
    }

    [Fact]
    public void ResolveOdbcTableFunction_ThrowsForWrongProvider()
    {
        var builder = new ModelBuilder();

        builder.Entity<ExternalOdbcSalesData>(entity =>
        {
            entity.HasNoKey();
            entity.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
            entity.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "mysql");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        var resolver = new ExternalConfigResolver();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            resolver.ResolveOdbcTableFunction(entityType));

        Assert.Contains("mysql", ex.Message);
        Assert.Contains("odbc", ex.Message.ToLower());
    }

    [Fact]
    public void ResolveTableFunction_DispatchesToOdbc()
    {
        var builder = new ModelBuilder();

        builder.ExternalOdbcEntity<ExternalOdbcSalesData>(ext => ext
            .FromTable("sales")
            .Dsn(value: "MsSqlProd"));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalOdbcSalesData))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveTableFunction(entityType);

        Assert.StartsWith("odbc(", sql);
    }
}

#endregion

#region Redis External Entity Configuration Tests

public class ExternalRedisEntityConfigurationTests
{
    [Fact]
    public void ExternalRedisEntity_StoresIsExternalAnnotation()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction)?.Value as bool?);
        Assert.Equal("redis", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalProvider)?.Value);
    }

    [Fact]
    public void ExternalRedisEntity_StoresKeyColumnFromExpression()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.Equal("SessionId", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRedisKeyColumn)?.Value);
    }

    [Fact]
    public void ExternalRedisEntity_StoresKeyColumnFromString()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn("session_id")
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.Equal("session_id", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRedisKeyColumn)?.Value);
    }

    [Fact]
    public void ExternalRedisEntity_StoresExplicitStructure()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Structure("session_id String, user_id UInt64, data String")
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.Equal("session_id String, user_id UInt64, data String",
            entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRedisStructure)?.Value);
    }

    [Fact]
    public void ExternalRedisEntity_StoresConnectionValues()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c
                .HostPort(value: "redis.example.com:6379")
                .Password(value: "redis-secret")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.Equal("redis.example.com:6379", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue)?.Value);
        Assert.Equal("redis-secret", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordValue)?.Value);
    }

    [Fact]
    public void ExternalRedisEntity_StoresEnvironmentVariables()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c
                .HostPort(env: "REDIS_HOST")
                .Password(env: "REDIS_PASSWORD")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.Equal("REDIS_HOST", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalHostPortEnv)?.Value);
        Assert.Equal("REDIS_PASSWORD", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalPasswordEnv)?.Value);
    }

    [Fact]
    public void ExternalRedisEntity_StoresDbIndex()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c
                .HostPort(value: "localhost:6379")
                .DbIndex(5)));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.Equal(5, entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRedisDbIndex)?.Value);
    }

    [Fact]
    public void ExternalRedisEntity_StoresPoolSize()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c
                .HostPort(value: "localhost:6379")
                .PoolSize(32)));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.Equal(32, entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalRedisPoolSize)?.Value);
    }

    [Fact]
    public void ExternalRedisEntity_DbIndexValidation_ThrowsForNegative()
    {
        var builder = new ModelBuilder();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
                .KeyColumn(x => x.SessionId)
                .Connection(c => c
                    .HostPort(value: "localhost:6379")
                    .DbIndex(-1))));
    }

    [Fact]
    public void ExternalRedisEntity_DbIndexValidation_ThrowsForTooLarge()
    {
        var builder = new ModelBuilder();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
                .KeyColumn(x => x.SessionId)
                .Connection(c => c
                    .HostPort(value: "localhost:6379")
                    .DbIndex(16))));
    }

    [Fact]
    public void ExternalRedisEntity_PoolSizeValidation_ThrowsForZero()
    {
        var builder = new ModelBuilder();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
                .KeyColumn(x => x.SessionId)
                .Connection(c => c
                    .HostPort(value: "localhost:6379")
                    .PoolSize(0))));
    }

    [Fact]
    public void ExternalRedisEntity_StoresProfileName()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c.UseProfile("production-redis")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.Equal("production-redis", entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalConnectionProfile)?.Value);
    }

    [Fact]
    public void ExternalRedisEntity_IsReadOnlyByDefault()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalReadOnly)?.Value as bool?);
    }

    [Fact]
    public void ExternalRedisEntity_AllowInsertsDisablesReadOnly()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .AllowInserts()
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.False(entityType.FindAnnotation(ClickHouseAnnotationNames.ExternalReadOnly)?.Value as bool?);
    }

    [Fact]
    public void ExternalRedisEntity_MarksEntityAsKeyless()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        Assert.Empty(entityType.GetKeys());
    }

    [Fact]
    public void ExternalRedisEntity_ThrowsWithoutKeyColumn()
    {
        var builder = new ModelBuilder();

        Assert.Throws<InvalidOperationException>(() =>
        {
            builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
                .Connection(c => c.HostPort(value: "localhost:6379")));
            builder.FinalizeModel();
        });
    }
}

#endregion

#region Redis External Config Resolver Tests

public class ExternalRedisConfigResolverTests
{
    [Fact]
    public void ResolveRedisTableFunction_GeneratesCorrectSqlWithLiteralValues()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn("SessionId")
            .Structure("SessionId String, UserId UInt64, Data String, ExpiresAt DateTime64(3)")
            .Connection(c => c
                .HostPort(value: "redis.example.com:6379")
                .Password(value: "secret")
                .DbIndex(2)));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveRedisTableFunction(entityType);

        Assert.Equal(
            "redis('redis.example.com:6379', 'SessionId', 'SessionId String, UserId UInt64, Data String, ExpiresAt DateTime64(3)', 2, 'secret')",
            sql);
    }

    [Fact]
    public void ResolveRedisTableFunction_DefaultsDbIndexToZero()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn("SessionId")
            .Structure("SessionId String")
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveRedisTableFunction(entityType);

        Assert.Contains(", 0, '')", sql);
    }

    [Fact]
    public void ResolveRedisTableFunction_AutoGeneratesStructureFromEntity()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisMetrics>(ext => ext
            .KeyColumn(x => x.Key)
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisMetrics))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveRedisTableFunction(entityType);

        // Should contain auto-generated structure with ClickHouse types
        Assert.Contains("Key String", sql);
        Assert.Contains("Count Int32", sql);
        Assert.Contains("TotalBytes Int64", sql);
        Assert.Contains("AverageLatency Float64", sql);
        Assert.Contains("IsActive Bool", sql);
    }

    [Fact]
    public void ResolveRedisTableFunction_EscapesSingleQuotes()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn("SessionId")
            .Structure("id String")
            .Connection(c => c
                .HostPort(value: "host'name:6379")
                .Password(value: "pass'word")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveRedisTableFunction(entityType);

        Assert.Contains("host\\'name:6379", sql);
        Assert.Contains("pass\\'word", sql);
    }

    [Fact]
    public void ResolveRedisTableFunction_ResolvesFromConfiguration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["TEST_REDIS_HOST"] = "config-redis:6379",
                ["TEST_REDIS_PASS"] = "config-secret"
            })
            .Build();

        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn("SessionId")
            .Structure("SessionId String")
            .Connection(c => c
                .HostPort(env: "TEST_REDIS_HOST")
                .Password(env: "TEST_REDIS_PASS")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        var resolver = new ExternalConfigResolver(configuration);
        var sql = resolver.ResolveRedisTableFunction(entityType);

        Assert.Contains("config-redis:6379", sql);
        Assert.Contains("config-secret", sql);
    }

    [Fact]
    public void ResolveRedisTableFunction_PasswordIsOptional()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn("SessionId")
            .Structure("SessionId String")
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveRedisTableFunction(entityType);

        // Should have empty password
        Assert.EndsWith(", 0, '')", sql);
    }

    [Fact]
    public void ResolveRedisTableFunction_ThrowsForWrongProvider()
    {
        var builder = new ModelBuilder();

        builder.Entity<ExternalRedisSession>(entity =>
        {
            entity.HasNoKey();
            entity.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
            entity.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "postgresql");
            entity.HasAnnotation(ClickHouseAnnotationNames.ExternalRedisKeyColumn, "SessionId");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        var resolver = new ExternalConfigResolver();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            resolver.ResolveRedisTableFunction(entityType));

        Assert.Contains("postgresql", ex.Message);
        Assert.Contains("redis", ex.Message.ToLower());
    }

    [Fact]
    public void ResolveRedisTableFunction_ThrowsForMissingKeyColumn()
    {
        var builder = new ModelBuilder();

        // Manually create an entity without key column annotation
        builder.Entity<ExternalRedisSession>(entity =>
        {
            entity.HasNoKey();
            entity.HasAnnotation(ClickHouseAnnotationNames.IsExternalTableFunction, true);
            entity.HasAnnotation(ClickHouseAnnotationNames.ExternalProvider, "redis");
            entity.HasAnnotation(ClickHouseAnnotationNames.ExternalHostPortValue, "localhost:6379");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        var resolver = new ExternalConfigResolver();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            resolver.ResolveRedisTableFunction(entityType));

        Assert.Contains("key column", ex.Message.ToLower());
    }

    [Fact]
    public void ResolveTableFunction_DispatchesToRedis()
    {
        var builder = new ModelBuilder();

        builder.ExternalRedisEntity<ExternalRedisSession>(ext => ext
            .KeyColumn(x => x.SessionId)
            .Structure("SessionId String")
            .Connection(c => c.HostPort(value: "localhost:6379")));

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExternalRedisSession))!;

        var resolver = new ExternalConfigResolver();
        var sql = resolver.ResolveTableFunction(entityType);

        Assert.StartsWith("redis(", sql);
    }
}

#endregion
