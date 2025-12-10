using EF.CH.Dictionaries;
using EF.CH.Extensions;
using EF.CH.Metadata;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class DictionaryTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    #region Fluent API Tests

    [Fact]
    public void AsDictionary_SetsCorrectAnnotations()
    {
        using var context = CreateContext<DictionaryTestContext>();
        var entityType = context.Model.FindEntityType(typeof(ProductDimension));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value);

        var source = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySource)?.Value;
        Assert.NotNull(source);
        Assert.IsType<ClickHouseTableSource>(source);
        Assert.Equal("products", ((ClickHouseTableSource)source).Table);

        var layout = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLayout)?.Value;
        Assert.NotNull(layout);
        Assert.IsAssignableFrom<DictionaryLayout>(layout);
        Assert.Equal("HASHED()", ((DictionaryLayout)layout).ToSql());

        var lifetime = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionaryLifetime)?.Value;
        Assert.NotNull(lifetime);
        Assert.Equal(300, ((DictionaryLifetime)lifetime).MinSeconds);
        Assert.Equal(360, ((DictionaryLifetime)lifetime).MaxSeconds);
    }

    [Fact]
    public void AsDictionary_WithHttpSource_SetsCorrectAnnotations()
    {
        using var context = CreateContext<HttpDictionaryTestContext>();
        var entityType = context.Model.FindEntityType(typeof(CurrencyRate));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value);

        var source = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySource)?.Value;
        Assert.NotNull(source);
        Assert.IsType<HttpDictionarySource>(source);
        Assert.Equal("https://api.example.com/rates", ((HttpDictionarySource)source).Url);
        Assert.Equal(HttpDictionaryFormat.JSONEachRow, ((HttpDictionarySource)source).Format);
    }

    [Fact]
    public void AsDictionary_WithFileSource_SetsCorrectAnnotations()
    {
        using var context = CreateContext<FileDictionaryTestContext>();
        var entityType = context.Model.FindEntityType(typeof(CountryMapping));

        Assert.NotNull(entityType);
        Assert.True((bool?)entityType.FindAnnotation(ClickHouseAnnotationNames.Dictionary)?.Value);

        var source = entityType.FindAnnotation(ClickHouseAnnotationNames.DictionarySource)?.Value;
        Assert.NotNull(source);
        Assert.IsType<FileDictionarySource>(source);
        Assert.Equal("/data/countries.csv", ((FileDictionarySource)source).Path);
        Assert.Equal(FileDictionaryFormat.CSV, ((FileDictionarySource)source).Format);
    }

    [Fact]
    public void AsDictionary_WithoutSource_ThrowsException()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
        {
            using var context = CreateContext<InvalidDictionaryContext>();
            // Force model building
            _ = context.Model;
        });

        Assert.Contains("must have a source defined", ex.Message);
    }

    #endregion

    #region DDL Generation Tests

    [Fact]
    public void MigrationsSqlGenerator_GeneratesCreateDictionary()
    {
        using var context = CreateContext<DictionaryTestContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var createTableOp = new CreateTableOperation
        {
            Name = "products_dict",
            Columns =
            {
                new AddColumnOperation { Name = "ProductId", ClrType = typeof(ulong), ColumnType = "UInt64" },
                new AddColumnOperation { Name = "ProductName", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "Category", ClrType = typeof(string), ColumnType = "String" }
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "ProductId" } }
        };

        createTableOp.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        createTableOp.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, new ClickHouseTableSource { Table = "products" });
        createTableOp.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.Hashed());
        createTableOp.AddAnnotation(ClickHouseAnnotationNames.DictionaryLifetime, new DictionaryLifetime(300, 360));

        var commands = generator.Generate(new[] { createTableOp }, model);
        var sql = commands.First().CommandText;

        // Verify CREATE DICTIONARY syntax
        Assert.Contains("CREATE DICTIONARY", sql);
        Assert.Contains("\"products_dict\"", sql);
        Assert.Contains("\"ProductId\" UInt64", sql);
        Assert.Contains("\"ProductName\" String", sql);
        Assert.Contains("\"Category\" String", sql);
        Assert.Contains("PRIMARY KEY", sql);
        Assert.Contains("SOURCE(CLICKHOUSE(TABLE 'products'))", sql);
        Assert.Contains("LAYOUT(HASHED())", sql);
        Assert.Contains("LIFETIME(MIN 300 MAX 360)", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesCreateDictionary_WithHttpSource()
    {
        using var context = CreateContext<HttpDictionaryTestContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var createTableOp = new CreateTableOperation
        {
            Name = "currency_rates_dict",
            Columns =
            {
                new AddColumnOperation { Name = "CurrencyCode", ClrType = typeof(string), ColumnType = "String" },
                new AddColumnOperation { Name = "ExchangeRate", ClrType = typeof(decimal), ColumnType = "Decimal(18,4)" }
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "CurrencyCode" } }
        };

        createTableOp.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        createTableOp.AddAnnotation(ClickHouseAnnotationNames.DictionarySource,
            new HttpDictionarySource { Url = "https://api.example.com/rates", Format = HttpDictionaryFormat.JSONEachRow });
        createTableOp.AddAnnotation(ClickHouseAnnotationNames.DictionaryLayout, DictionaryLayout.Flat());
        createTableOp.AddAnnotation(ClickHouseAnnotationNames.DictionaryLifetime, new DictionaryLifetime(60, 120));

        var commands = generator.Generate(new[] { createTableOp }, model);
        var sql = commands.First().CommandText;

        Assert.Contains("CREATE DICTIONARY", sql);
        Assert.Contains("SOURCE(HTTP(URL 'https://api.example.com/rates' FORMAT JSONEachRow))", sql);
        Assert.Contains("LAYOUT(FLAT())", sql);
        Assert.Contains("LIFETIME(MIN 60 MAX 120)", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_GeneratesDropDictionary()
    {
        using var context = CreateContext<DictionaryTestContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var dropTableOp = new DropTableOperation { Name = "products_dict" };
        dropTableOp.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);

        var commands = generator.Generate(new[] { dropTableOp }, model);
        var sql = commands.First().CommandText;

        Assert.Contains("DROP DICTIONARY IF EXISTS", sql);
        Assert.Contains("\"products_dict\"", sql);
    }

    [Fact]
    public void MigrationsSqlGenerator_ThrowsWhenNoSource()
    {
        using var context = CreateContext<DictionaryTestContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var createTableOp = new CreateTableOperation
        {
            Name = "bad_dict",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(int), ColumnType = "Int32" }
            },
            PrimaryKey = new AddPrimaryKeyOperation { Columns = new[] { "Id" } }
        };

        createTableOp.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        // No source annotation

        var ex = Assert.Throws<InvalidOperationException>(
            () => generator.Generate(new[] { createTableOp }, model));

        Assert.Contains("must have a source defined", ex.Message);
    }

    [Fact]
    public void MigrationsSqlGenerator_ThrowsWhenNoKey()
    {
        using var context = CreateContext<DictionaryTestContext>();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var model = context.Model;

        var createTableOp = new CreateTableOperation
        {
            Name = "bad_dict",
            Columns =
            {
                new AddColumnOperation { Name = "Id", ClrType = typeof(int), ColumnType = "Int32" }
            }
            // No primary key
        };

        createTableOp.AddAnnotation(ClickHouseAnnotationNames.Dictionary, true);
        createTableOp.AddAnnotation(ClickHouseAnnotationNames.DictionarySource, new ClickHouseTableSource { Table = "source" });

        var ex = Assert.Throws<InvalidOperationException>(
            () => generator.Generate(new[] { createTableOp }, model));

        Assert.Contains("must have a primary key defined", ex.Message);
    }

    #endregion

    #region Layout Tests

    [Fact]
    public void DictionaryLayout_Flat_GeneratesCorrectSql()
    {
        var layout = DictionaryLayout.Flat();
        Assert.Equal("FLAT()", layout.ToSql());

        var layoutWithSize = DictionaryLayout.Flat(initialArraySize: 1000, maxArraySize: 100000);
        Assert.Equal("FLAT(INITIAL_ARRAY_SIZE 1000 MAX_ARRAY_SIZE 100000)", layoutWithSize.ToSql());
    }

    [Fact]
    public void DictionaryLayout_Hashed_GeneratesCorrectSql()
    {
        var layout = DictionaryLayout.Hashed();
        Assert.Equal("HASHED()", layout.ToSql());

        var layoutWithSize = DictionaryLayout.Hashed(initialSize: 10000);
        Assert.Equal("HASHED(INITIAL_SIZE 10000)", layoutWithSize.ToSql());
    }

    [Fact]
    public void DictionaryLayout_Cache_GeneratesCorrectSql()
    {
        var layout = DictionaryLayout.Cache(sizeInCells: 100000);
        Assert.Equal("CACHE(SIZE_IN_CELLS 100000)", layout.ToSql());
    }

    [Fact]
    public void DictionaryLayout_RangeHashed_GeneratesCorrectSql()
    {
        var layout = DictionaryLayout.RangeHashed();
        Assert.Equal("RANGE_HASHED()", layout.ToSql());
    }

    [Fact]
    public void DictionaryLayout_Direct_GeneratesCorrectSql()
    {
        var layout = DictionaryLayout.Direct();
        Assert.Equal("DIRECT()", layout.ToSql());
    }

    [Fact]
    public void DictionaryLayout_ComplexKeyHashed_GeneratesCorrectSql()
    {
        var layout = DictionaryLayout.ComplexKeyHashed();
        Assert.Equal("COMPLEX_KEY_HASHED()", layout.ToSql());
    }

    #endregion

    #region Source Tests

    [Fact]
    public void ClickHouseTableSource_GeneratesCorrectSql()
    {
        var source = new ClickHouseTableSource { Table = "products" };
        Assert.Equal("CLICKHOUSE(TABLE 'products')", source.ToSql());

        var sourceWithOptions = new ClickHouseTableSource
        {
            Table = "products",
            Database = "mydb",
            Host = "remote.host",
            Port = 9000,
            Where = "active = 1"
        };
        var sql = sourceWithOptions.ToSql();
        Assert.Contains("HOST 'remote.host'", sql);
        Assert.Contains("PORT 9000", sql);
        Assert.Contains("DB 'mydb'", sql);
        Assert.Contains("TABLE 'products'", sql);
        Assert.Contains("WHERE active = 1", sql);
    }

    [Fact]
    public void HttpDictionarySource_GeneratesCorrectSql()
    {
        var source = new HttpDictionarySource
        {
            Url = "https://api.example.com/data",
            Format = HttpDictionaryFormat.JSONEachRow
        };
        Assert.Equal("HTTP(URL 'https://api.example.com/data' FORMAT JSONEachRow)", source.ToSql());
    }

    [Fact]
    public void FileDictionarySource_GeneratesCorrectSql()
    {
        var source = new FileDictionarySource
        {
            Path = "/data/file.csv",
            Format = FileDictionaryFormat.CSV
        };
        Assert.Equal("FILE(PATH '/data/file.csv' FORMAT CSV)", source.ToSql());
    }

    #endregion

    #region Lifetime Tests

    [Fact]
    public void DictionaryLifetime_Range_GeneratesCorrectSql()
    {
        var lifetime = DictionaryLifetime.Range(300, 360);
        Assert.Equal("LIFETIME(MIN 300 MAX 360)", lifetime.ToSql());
    }

    [Fact]
    public void DictionaryLifetime_Fixed_GeneratesCorrectSql()
    {
        var lifetime = DictionaryLifetime.Fixed(600);
        Assert.Equal("LIFETIME(600)", lifetime.ToSql());
    }

    [Fact]
    public void DictionaryLifetime_NoRefresh_GeneratesCorrectSql()
    {
        var lifetime = DictionaryLifetime.NoRefresh;
        Assert.Equal("LIFETIME(0)", lifetime.ToSql());
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CreateDictionary_FromTable_ExecutesSuccessfully()
    {
        await using var context = CreateContext<DictionaryTestContext>();

        // Create source table first
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS products (
                ProductId UInt64,
                ProductName String,
                Category String
            ) ENGINE = MergeTree()
            ORDER BY ProductId
        ");

        // Insert some data
        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO products (ProductId, ProductName, Category) VALUES
            (1, 'Widget', 'Hardware'),
            (2, 'Gadget', 'Electronics'),
            (3, 'Gizmo', 'Hardware')
        ");

        // Create the dictionary with explicit credentials for the Testcontainers ClickHouse
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE DICTIONARY IF NOT EXISTS products_dict (
                ProductId UInt64,
                ProductName String,
                Category String
            )
            PRIMARY KEY ProductId
            SOURCE(CLICKHOUSE(TABLE 'products' USER 'clickhouse' PASSWORD 'clickhouse'))
            LAYOUT(HASHED())
            LIFETIME(MIN 300 MAX 360)
        ");

        // Query using dictGet - need to alias the result for EF Core's SqlQueryRaw
        var result = await context.Database.SqlQueryRaw<string>(
            "SELECT dictGet('products_dict', 'ProductName', toUInt64(1)) AS Value"
        ).FirstAsync();

        Assert.Equal("Widget", result);
    }

    [Fact]
    public async Task DictGet_InQuery_ExecutesSuccessfully()
    {
        await using var context = CreateContext<DictionaryTestContext>();

        // Create source table and dictionary
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS categories (
                CategoryId UInt32,
                CategoryName String
            ) ENGINE = MergeTree()
            ORDER BY CategoryId
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO categories (CategoryId, CategoryName) VALUES
            (1, 'Electronics'),
            (2, 'Books'),
            (3, 'Clothing')
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            CREATE DICTIONARY IF NOT EXISTS categories_dict (
                CategoryId UInt32,
                CategoryName String
            )
            PRIMARY KEY CategoryId
            SOURCE(CLICKHOUSE(TABLE 'categories' USER 'clickhouse' PASSWORD 'clickhouse'))
            LAYOUT(FLAT())
            LIFETIME(0)
        ");

        // Create orders table
        await context.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS orders (
                OrderId UInt64,
                CategoryId UInt32,
                Amount Decimal(18, 4)
            ) ENGINE = MergeTree()
            ORDER BY OrderId
        ");

        await context.Database.ExecuteSqlRawAsync(@"
            INSERT INTO orders (OrderId, CategoryId, Amount) VALUES
            (1, 1, 99.99),
            (2, 2, 29.99),
            (3, 1, 149.99)
        ");

        // Query with dictGet
        var results = await context.Database.SqlQueryRaw<OrderWithCategory>(
            @"SELECT
                OrderId,
                CategoryId,
                Amount,
                dictGet('categories_dict', 'CategoryName', CategoryId) AS CategoryName
            FROM orders
            ORDER BY OrderId"
        ).ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal("Electronics", results[0].CategoryName);
        Assert.Equal("Books", results[1].CategoryName);
        Assert.Equal("Electronics", results[2].CategoryName);
    }

    #endregion

    private TContext CreateContext<TContext>() where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}

#region Test Entities

public class ProductDimension
{
    public ulong ProductId { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public string Category { get; set; } = string.Empty;
}

public class CurrencyRate
{
    public string CurrencyCode { get; set; } = string.Empty;
    public decimal ExchangeRate { get; set; }
}

public class CountryMapping
{
    public string CountryCode { get; set; } = string.Empty;
    public string CountryName { get; set; } = string.Empty;
}

public class InvalidDictEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class OrderWithCategory
{
    public ulong OrderId { get; set; }
    public uint CategoryId { get; set; }
    public decimal Amount { get; set; }
    public string CategoryName { get; set; } = string.Empty;
}

#endregion

#region Test Contexts

public class DictionaryTestContext : DbContext
{
    public DictionaryTestContext(DbContextOptions<DictionaryTestContext> options)
        : base(options) { }

    public DbSet<ProductDimension> Products => Set<ProductDimension>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ProductDimension>(entity =>
        {
            entity.ToTable("products_dict");
            entity.HasKey(e => e.ProductId);
            entity.AsDictionary(dict => dict
                .FromTable("products")
                .WithLayout(DictionaryLayout.Hashed())
                .WithLifetime(300, 360));
        });
    }
}

public class HttpDictionaryTestContext : DbContext
{
    public HttpDictionaryTestContext(DbContextOptions<HttpDictionaryTestContext> options)
        : base(options) { }

    public DbSet<CurrencyRate> CurrencyRates => Set<CurrencyRate>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<CurrencyRate>(entity =>
        {
            entity.ToTable("currency_rates_dict");
            entity.HasKey(e => e.CurrencyCode);
            entity.AsDictionary(dict => dict
                .FromHttp("https://api.example.com/rates", HttpDictionaryFormat.JSONEachRow)
                .WithLayout(DictionaryLayout.Flat())
                .WithLifetime(60, 120));
        });
    }
}

public class FileDictionaryTestContext : DbContext
{
    public FileDictionaryTestContext(DbContextOptions<FileDictionaryTestContext> options)
        : base(options) { }

    public DbSet<CountryMapping> Countries => Set<CountryMapping>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<CountryMapping>(entity =>
        {
            entity.ToTable("countries_dict");
            entity.HasKey(e => e.CountryCode);
            entity.AsDictionary(dict => dict
                .FromFile("/data/countries.csv", FileDictionaryFormat.CSV)
                .WithLayout(DictionaryLayout.Hashed())
                .WithLifetime(0)); // Static, no refresh
        });
    }
}

public class InvalidDictionaryContext : DbContext
{
    public InvalidDictionaryContext(DbContextOptions<InvalidDictionaryContext> options)
        : base(options) { }

    public DbSet<InvalidDictEntity> Entities => Set<InvalidDictEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InvalidDictEntity>(entity =>
        {
            entity.ToTable("invalid_dict");
            entity.HasKey(e => e.Id);
            // This should throw - no source defined
            entity.AsDictionary(dict => dict
                .WithLayout(DictionaryLayout.Hashed()));
        });
    }
}

#endregion
