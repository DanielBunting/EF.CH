using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Tests for external dictionary sources (PostgreSQL, MySQL, HTTP).
/// </summary>
public class ExternalDictionaryTests
{
    #region Test Entities

    public class ExtCountryLookup : IClickHouseDictionary
    {
        public ulong Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string IsoCode { get; set; } = string.Empty;
    }

    public class ExtProductLookup : IClickHouseDictionary
    {
        public int ProductId { get; set; }
        public string ProductName { get; set; } = string.Empty;
        public decimal Price { get; set; }
    }

    public class ExtApiDataLookup : IClickHouseDictionary
    {
        public string Key { get; set; } = string.Empty;
        public string Value { get; set; } = string.Empty;
    }

    #endregion

    #region Configuration Tests

    [Fact]
    public void PostgreSql_Configuration_StoresAnnotationsCorrectly()
    {
        // Arrange & Act
        var builder = new ModelBuilder();

        builder.Entity<ExtCountryLookup>(entity =>
        {
            entity.AsDictionary<ExtCountryLookup>(cfg => cfg
                .HasKey(x => x.Id)
                .FromPostgreSql(pg => pg
                    .FromTable("countries", schema: "public")
                    .Connection(c => c
                        .HostPort(env: "PG_HOST")
                        .Database(env: "PG_DATABASE")
                        .Credentials("PG_USER", "PG_PASSWORD"))
                    .Where("is_active = true")
                    .InvalidateQuery("SELECT max(updated_at) FROM countries"))
                .UseHashedLayout()
                .HasLifetime(minSeconds: 60, maxSeconds: 300)
                .HasDefault(x => x.Name, "Unknown"));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExtCountryLookup))!;

        // Assert
        Assert.True(entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value is true);
        Assert.Equal("postgresql", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySourceProvider)?.Value);
        Assert.Equal("countries", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryExternalTable)?.Value);
        Assert.Equal("public", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryExternalSchema)?.Value);
        Assert.Equal("is_active = true", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryExternalWhere)?.Value);
        Assert.Equal("SELECT max(updated_at) FROM countries", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryInvalidateQuery)?.Value);
        Assert.Equal(DictionaryLayout.Hashed, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);
        Assert.Equal(60, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMin)?.Value);
        Assert.Equal(300, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetimeMax)?.Value);
        Assert.Equal(new[] { "Id" }, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns)?.Value as string[]);

        var defaults = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryDefaults)?.Value as Dictionary<string, object>;
        Assert.NotNull(defaults);
        Assert.Equal("Unknown", defaults["Name"]);
    }

    [Fact]
    public void MySql_Configuration_StoresAnnotationsCorrectly()
    {
        // Arrange & Act
        var builder = new ModelBuilder();

        builder.Entity<ExtProductLookup>(entity =>
        {
            entity.AsDictionary<ExtProductLookup>(cfg => cfg
                .HasKey(x => x.ProductId)
                .FromMySql(mysql => mysql
                    .FromTable("products")
                    .Connection(c => c
                        .Host(value: "mysql-server")
                        .Port(value: 3306)
                        .Database(value: "inventory")
                        .User(value: "root")
                        .Password(value: "password"))
                    .FailOnConnectionLoss(true))
                .UseFlatLayout()
                .HasLifetime(600));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExtProductLookup))!;

        // Assert
        Assert.Equal("mysql", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySourceProvider)?.Value);
        Assert.Equal("products", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryExternalTable)?.Value);
        Assert.Equal(true, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryMySqlFailOnConnectionLoss)?.Value);
        Assert.Equal(DictionaryLayout.Flat, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);
    }

    [Fact]
    public void Http_Configuration_StoresAnnotationsCorrectly()
    {
        // Arrange & Act
        var builder = new ModelBuilder();

        builder.Entity<ExtApiDataLookup>(entity =>
        {
            entity.AsDictionary<ExtApiDataLookup>(cfg => cfg
                .HasKey(x => x.Key)
                .FromHttp(http => http
                    .Url(value: "http://api.example.com/data")
                    .Format("JSONEachRow")
                    .Header("Authorization", "Bearer token123"))
                .UseCacheLayout(opts => opts.SizeInCells = 10000)
                .HasLifetime(60));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExtApiDataLookup))!;

        // Assert
        Assert.Equal("http", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySourceProvider)?.Value);
        Assert.Equal("http://api.example.com/data", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryHttpUrl)?.Value);
        Assert.Equal("JSONEachRow", entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryHttpFormat)?.Value);
        Assert.Equal(DictionaryLayout.Cache, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);
    }

    [Fact]
    public void CompositeKey_Configuration_StoresKeyColumnsCorrectly()
    {
        // Arrange & Act
        var builder = new ModelBuilder();

        builder.Entity<ExtProductLookup>(entity =>
        {
            entity.AsDictionary<ExtProductLookup>(cfg => cfg
                .HasCompositeKey(x => new { x.ProductId, x.ProductName })
                .FromPostgreSql(pg => pg
                    .FromTable("products")
                    .Connection(c => c.UseProfile("PostgresMain")))
                .UseLayout(DictionaryLayout.ComplexKeyHashed));
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(ExtProductLookup))!;

        // Assert
        var keyColumns = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryKeyColumns)?.Value as string[];

        Assert.NotNull(keyColumns);
        Assert.Equal(2, keyColumns.Length);
        Assert.Contains("ProductId", keyColumns);
        Assert.Contains("ProductName", keyColumns);
        Assert.Equal(DictionaryLayout.ComplexKeyHashed, entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value);
    }

    [Fact]
    public void Configuration_ThrowsIfNoSourceSpecified()
    {
        // Arrange & Act & Assert
        Assert.Throws<InvalidOperationException>(() =>
        {
            var builder = new ModelBuilder();

            builder.Entity<ExtCountryLookup>(entity =>
            {
                entity.AsDictionary<ExtCountryLookup>(cfg => cfg
                    .HasKey(x => x.Id)
                    .UseHashedLayout());
                // Missing FromPostgreSql/FromMySql/FromHttp
            });

            builder.FinalizeModel();
        });
    }

    [Fact]
    public void Configuration_ThrowsIfNoKeySpecified()
    {
        // Arrange & Act & Assert
        Assert.Throws<InvalidOperationException>(() =>
        {
            var builder = new ModelBuilder();

            builder.Entity<ExtCountryLookup>(entity =>
            {
                entity.AsDictionary<ExtCountryLookup>(cfg => cfg
                    .FromPostgreSql(pg => pg
                        .FromTable("countries")
                        .Connection(c => c.HostPort(value: "localhost:5432")))
                    .UseHashedLayout());
                // Missing HasKey
            });

            builder.FinalizeModel();
        });
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void DdlGeneration_PostgreSql_GeneratesCorrectSql()
    {
        // Arrange
        Environment.SetEnvironmentVariable("TEST_PG_HOST", "pghost:5432");
        Environment.SetEnvironmentVariable("TEST_PG_DB", "testdb");
        Environment.SetEnvironmentVariable("TEST_PG_USER", "pguser");
        Environment.SetEnvironmentVariable("TEST_PG_PASS", "pgpass");

        try
        {
            var builder = new ModelBuilder();

            builder.Entity<ExtCountryLookup>(entity =>
            {
                entity.AsDictionary<ExtCountryLookup>(cfg => cfg
                    .HasKey(x => x.Id)
                    .FromPostgreSql(pg => pg
                        .FromTable("countries", schema: "public")
                        .Connection(c => c
                            .HostPort(env: "TEST_PG_HOST")
                            .Database(env: "TEST_PG_DB")
                            .Credentials("TEST_PG_USER", "TEST_PG_PASS"))
                        .Where("is_active = true"))
                    .UseHashedLayout()
                    .HasLifetime(300)
                    .HasDefault(x => x.Name, "Unknown"));
            });

            var model = builder.FinalizeModel();
            var resolver = new DictionaryConfigResolver();
            var entityType = model.FindEntityType(typeof(ExtCountryLookup))!;

            // Act
            var ddl = resolver.GenerateCreateDictionaryDdl(entityType);

            // Assert
            Assert.Contains("CREATE DICTIONARY IF NOT EXISTS", ddl);
            Assert.Contains("\"ext_country_lookup\"", ddl);
            Assert.Contains("\"Id\" UInt64", ddl);
            Assert.Contains("\"Name\" String DEFAULT 'Unknown'", ddl);
            Assert.Contains("\"IsoCode\" String", ddl);
            Assert.Contains("PRIMARY KEY \"Id\"", ddl);
            Assert.Contains("SOURCE(POSTGRESQL(", ddl);
            Assert.Contains("host 'pghost'", ddl);
            Assert.Contains("port 5432", ddl);
            Assert.Contains("user 'pguser'", ddl);
            Assert.Contains("password 'pgpass'", ddl);
            Assert.Contains("db 'testdb'", ddl);
            Assert.Contains("table 'countries'", ddl);
            Assert.Contains("schema 'public'", ddl);
            Assert.Contains("where 'is_active = true'", ddl);
            Assert.Contains("LAYOUT(HASHED())", ddl);
            Assert.Contains("LIFETIME(300)", ddl);
        }
        finally
        {
            Environment.SetEnvironmentVariable("TEST_PG_HOST", null);
            Environment.SetEnvironmentVariable("TEST_PG_DB", null);
            Environment.SetEnvironmentVariable("TEST_PG_USER", null);
            Environment.SetEnvironmentVariable("TEST_PG_PASS", null);
        }
    }

    [Fact]
    public void DdlGeneration_MySql_GeneratesCorrectSql()
    {
        // Arrange
        var builder = new ModelBuilder();

        builder.Entity<ExtProductLookup>(entity =>
        {
            entity.AsDictionary<ExtProductLookup>(cfg => cfg
                .HasKey(x => x.ProductId)
                .FromMySql(mysql => mysql
                    .FromTable("products")
                    .Connection(c => c
                        .Host(value: "mysql-host")
                        .Port(value: 3306)
                        .Database(value: "inventory")
                        .User(value: "root")
                        .Password(value: "secret"))
                    .FailOnConnectionLoss(true)
                    .InvalidateQuery("SELECT MAX(updated_at) FROM products"))
                .UseHashedLayout()
                .HasLifetime(minSeconds: 30, maxSeconds: 120));
        });

        var model = builder.FinalizeModel();
        var resolver = new DictionaryConfigResolver();
        var entityType = model.FindEntityType(typeof(ExtProductLookup))!;

        // Act
        var ddl = resolver.GenerateCreateDictionaryDdl(entityType);

        // Assert
        Assert.Contains("CREATE DICTIONARY IF NOT EXISTS", ddl);
        Assert.Contains("\"ext_product_lookup\"", ddl);
        Assert.Contains("SOURCE(MYSQL(", ddl);
        Assert.Contains("host 'mysql-host'", ddl);
        Assert.Contains("port 3306", ddl);
        Assert.Contains("user 'root'", ddl);
        Assert.Contains("password 'secret'", ddl);
        Assert.Contains("db 'inventory'", ddl);
        Assert.Contains("table 'products'", ddl);
        Assert.Contains("fail_on_connection_loss 'true'", ddl);
        Assert.Contains("invalidate_query 'SELECT MAX(updated_at) FROM products'", ddl);
        Assert.Contains("LIFETIME(MIN 30 MAX 120)", ddl);
    }

    [Fact]
    public void DdlGeneration_Http_GeneratesCorrectSql()
    {
        // Arrange
        var builder = new ModelBuilder();

        builder.Entity<ExtApiDataLookup>(entity =>
        {
            entity.AsDictionary<ExtApiDataLookup>(cfg => cfg
                .HasKey(x => x.Key)
                .FromHttp(http => http
                    .Url(value: "https://api.example.com/lookup")
                    .Format("JSONEachRow")
                    .CredentialsLiteral("apiuser", "apikey")
                    .Header("X-API-Version", "2.0"))
                .UseCacheLayout(opts => opts.SizeInCells = 50000)
                .HasLifetime(60));
        });

        var model = builder.FinalizeModel();
        var resolver = new DictionaryConfigResolver();
        var entityType = model.FindEntityType(typeof(ExtApiDataLookup))!;

        // Act
        var ddl = resolver.GenerateCreateDictionaryDdl(entityType);

        // Assert
        Assert.Contains("CREATE DICTIONARY IF NOT EXISTS", ddl);
        Assert.Contains("\"ext_api_data_lookup\"", ddl);
        Assert.Contains("SOURCE(HTTP(", ddl);
        Assert.Contains("url 'https://api.example.com/lookup'", ddl);
        Assert.Contains("format 'JSONEachRow'", ddl);
        Assert.Contains("credentials(user 'apiuser' password 'apikey')", ddl);
        Assert.Contains("'X-API-Version' '2.0'", ddl);
        Assert.Contains("LAYOUT(CACHE(SIZE_IN_CELLS 50000))", ddl);
        Assert.Contains("LIFETIME(60)", ddl);
    }

    [Fact]
    public void DdlGeneration_CompositeKey_GeneratesCorrectPrimaryKey()
    {
        // Arrange
        var builder = new ModelBuilder();

        builder.Entity<ExtProductLookup>(entity =>
        {
            entity.AsDictionary<ExtProductLookup>(cfg => cfg
                .HasCompositeKey(x => new { x.ProductId, x.ProductName })
                .FromMySql(mysql => mysql
                    .FromTable("products")
                    .Connection(c => c
                        .HostPort(value: "localhost:3306")
                        .Database(value: "db")
                        .User(value: "user")
                        .Password(value: "pass")))
                .UseLayout(DictionaryLayout.ComplexKeyHashed));
        });

        var model = builder.FinalizeModel();
        var resolver = new DictionaryConfigResolver();
        var entityType = model.FindEntityType(typeof(ExtProductLookup))!;

        // Act
        var ddl = resolver.GenerateCreateDictionaryDdl(entityType);

        // Assert
        Assert.Contains("PRIMARY KEY (\"ProductId\", \"ProductName\")", ddl);
        Assert.Contains("LAYOUT(COMPLEX_KEY_HASHED())", ddl);
    }

    #endregion

    #region Resolver Tests

    [Fact]
    public void Resolver_IsExternalDictionary_ReturnsTrueForPostgreSql()
    {
        // Arrange
        var builder = new ModelBuilder();

        builder.Entity<ExtCountryLookup>(entity =>
        {
            entity.AsDictionary<ExtCountryLookup>(cfg => cfg
                .HasKey(x => x.Id)
                .FromPostgreSql(pg => pg
                    .FromTable("countries")
                    .Connection(c => c.UseProfile("Test"))));
        });

        var model = builder.FinalizeModel();
        var resolver = new DictionaryConfigResolver();
        var entityType = model.FindEntityType(typeof(ExtCountryLookup))!;

        // Act & Assert
        Assert.True(resolver.IsDictionary(entityType));
        Assert.True(resolver.IsExternalDictionary(entityType));
        Assert.Equal("postgresql", resolver.GetSourceProvider(entityType));
    }

    [Fact]
    public void Resolver_GenerateDropDictionary_ReturnsCorrectSql()
    {
        // Arrange
        var builder = new ModelBuilder();

        builder.Entity<ExtCountryLookup>(entity =>
        {
            entity.AsDictionary<ExtCountryLookup>(cfg => cfg
                .HasKey(x => x.Id)
                .FromPostgreSql(pg => pg
                    .FromTable("countries")
                    .Connection(c => c.UseProfile("Test"))));
        });

        var model = builder.FinalizeModel();
        var resolver = new DictionaryConfigResolver();
        var entityType = model.FindEntityType(typeof(ExtCountryLookup))!;

        // Act
        var ddl = resolver.GenerateDropDictionaryDdl(entityType);

        // Assert
        Assert.Equal("DROP DICTIONARY IF EXISTS \"ext_country_lookup\"", ddl);
    }

    [Fact]
    public void Resolver_GenerateReloadDictionary_ReturnsCorrectSql()
    {
        // Arrange
        var builder = new ModelBuilder();

        builder.Entity<ExtCountryLookup>(entity =>
        {
            entity.AsDictionary<ExtCountryLookup>(cfg => cfg
                .HasKey(x => x.Id)
                .FromPostgreSql(pg => pg
                    .FromTable("countries")
                    .Connection(c => c.UseProfile("Test"))));
        });

        var model = builder.FinalizeModel();
        var resolver = new DictionaryConfigResolver();
        var entityType = model.FindEntityType(typeof(ExtCountryLookup))!;

        // Act
        var ddl = resolver.GenerateReloadDictionaryDdl(entityType);

        // Assert
        Assert.Equal("SYSTEM RELOAD DICTIONARY \"ext_country_lookup\"", ddl);
    }

    #endregion

    #region Profile Resolution Tests

    [Fact]
    public void Resolver_WithProfile_ResolvesCredentialsFromConfiguration()
    {
        // Arrange
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ExternalConnections:PostgresMain:HostPort"] = "pg.example.com:5432",
                ["ExternalConnections:PostgresMain:Database"] = "maindb",
                ["ExternalConnections:PostgresMain:User"] = "dbuser",
                ["ExternalConnections:PostgresMain:Password"] = "dbpass"
            })
            .Build();

        var builder = new ModelBuilder();

        builder.Entity<ExtCountryLookup>(entity =>
        {
            entity.AsDictionary<ExtCountryLookup>(cfg => cfg
                .HasKey(x => x.Id)
                .FromPostgreSql(pg => pg
                    .FromTable("countries")
                    .Connection(c => c.UseProfile("PostgresMain")))
                .UseHashedLayout()
                .HasLifetime(300));
        });

        var model = builder.FinalizeModel();
        var resolver = new DictionaryConfigResolver(configuration);
        var entityType = model.FindEntityType(typeof(ExtCountryLookup))!;

        // Act
        var ddl = resolver.GenerateCreateDictionaryDdl(entityType);

        // Assert
        Assert.Contains("host 'pg.example.com'", ddl);
        Assert.Contains("port 5432", ddl);
        Assert.Contains("db 'maindb'", ddl);
        Assert.Contains("user 'dbuser'", ddl);
        Assert.Contains("password 'dbpass'", ddl);
    }

    #endregion
}
