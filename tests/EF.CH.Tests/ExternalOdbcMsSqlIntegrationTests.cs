using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Images;
using DotNet.Testcontainers.Networks;
using EF.CH.Extensions;
using EF.CH.External;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;
using Xunit;

namespace EF.CH.Tests;

/// <summary>
/// Integration tests for external ODBC entities using Testcontainers.
/// Tests the odbc() table function integration between ClickHouse and SQL Server.
///
/// NOTE: These tests require building a custom ClickHouse image with ODBC drivers.
/// The image build happens once at test fixture initialization.
/// </summary>
[Collection("ODBC Tests")]
public class ExternalOdbcMsSqlIntegrationTests : IAsyncLifetime
{
    private const string MsSqlNetworkAlias = "mssql-db";
    private const string ClickHouseOdbcImageName = "ef-ch-clickhouse-odbc";
    private const string ClickHouseOdbcImageTag = "test";
    private const string DsnName = "MsSqlTest";

    private readonly INetwork _network = new NetworkBuilder()
        .WithName(Guid.NewGuid().ToString("D"))
        .Build();

    private MsSqlContainer _msSqlContainer = null!;
    private IContainer _clickHouseContainer = null!;
    private IFutureDockerImage? _clickHouseOdbcImage;

    public async Task InitializeAsync()
    {
        // Create network first
        await _network.CreateAsync();

        // Build custom ClickHouse image with ODBC drivers
        await BuildClickHouseOdbcImageAsync();

        // Create MSSQL container
        _msSqlContainer = new MsSqlBuilder()
            .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
            .WithNetwork(_network)
            .WithNetworkAliases(MsSqlNetworkAlias)
            .WithPassword("P@ssw0rd123!")
            .Build();

        // Start MSSQL first to get the connection ready
        await _msSqlContainer.StartAsync();

        // Generate odbc.ini content for this test run
        var odbcIniContent = GenerateOdbcIni();

        // Create ClickHouse container with ODBC configuration mounted
        _clickHouseContainer = new ContainerBuilder()
            .WithImage($"{ClickHouseOdbcImageName}:{ClickHouseOdbcImageTag}")
            .WithNetwork(_network)
            .WithResourceMapping(Encoding.UTF8.GetBytes(odbcIniContent), "/etc/odbc.ini")
            .WithPortBinding(8123, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(8123)))
            .Build();

        await _clickHouseContainer.StartAsync();

        // Set environment variables
        SetupEnvironmentVariables();
    }

    private async Task BuildClickHouseOdbcImageAsync()
    {
        var dockerfilePath = Path.Combine(
            AppContext.BaseDirectory,
            "..", "..", "..",
            "Docker", "Dockerfile.clickhouse-odbc");

        // Check if the Dockerfile exists at the expected location
        if (!File.Exists(dockerfilePath))
        {
            // Try alternate path for when running from solution root
            dockerfilePath = Path.Combine(
                Directory.GetCurrentDirectory(),
                "tests", "EF.CH.Tests", "Docker", "Dockerfile.clickhouse-odbc");
        }

        if (!File.Exists(dockerfilePath))
        {
            throw new FileNotFoundException(
                $"Dockerfile.clickhouse-odbc not found. Searched paths include: {dockerfilePath}");
        }

        var dockerfileDir = Path.GetDirectoryName(dockerfilePath)!;

        _clickHouseOdbcImage = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(dockerfileDir)
            .WithDockerfile("Dockerfile.clickhouse-odbc")
            .WithName($"{ClickHouseOdbcImageName}:{ClickHouseOdbcImageTag}")
            .WithCleanUp(false) // Keep image for reuse
            .Build();

        await _clickHouseOdbcImage.CreateAsync();
    }

    private string GenerateOdbcIni()
    {
        // Generate odbc.ini with DSN pointing to MSSQL container via network alias
        return $"""
            [{DsnName}]
            Driver = /opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1
            Server = {MsSqlNetworkAlias}
            Port = 1433
            Database = master
            TrustServerCertificate = yes
            """;
    }

    private void SetupEnvironmentVariables()
    {
        Environment.SetEnvironmentVariable("MSSQL_DSN", DsnName);
    }

    public async Task DisposeAsync()
    {
        // Clean up environment variables
        Environment.SetEnvironmentVariable("MSSQL_DSN", null);

        if (_clickHouseContainer != null)
        {
            await _clickHouseContainer.DisposeAsync();
        }

        if (_msSqlContainer != null)
        {
            await _msSqlContainer.DisposeAsync();
        }

        await _network.DisposeAsync();

        // Note: We don't dispose the image to allow reuse across test runs
    }

    [Fact]
    public async Task CanQueryExternalOdbcTable()
    {
        // Arrange: Set up MSSQL with data
        await CreateMsSqlTableAndData();

        // Act: Query from ClickHouse using external ODBC entity
        await using var chContext = CreateClickHouseContext();

        var customers = await chContext.ExternalOdbcCustomers
            .Where(c => c.Name.StartsWith("A") || c.Name.StartsWith("B"))
            .OrderBy(c => c.Name)
            .ToListAsync();

        // Assert
        Assert.Equal(2, customers.Count);
        Assert.Equal("Alice", customers[0].Name);
        Assert.Equal("Bob", customers[1].Name);
    }

    [Fact]
    public async Task CanQueryExternalOdbcWithProjection()
    {
        // Arrange
        await CreateMsSqlProductsAndData();

        // Act
        await using var chContext = CreateClickHouseContext();

        var products = await chContext.ExternalOdbcProducts
            .Where(p => p.Price > 20m)
            .Select(p => new { p.Sku, p.Name })
            .ToListAsync();

        // Assert
        Assert.Equal(2, products.Count);
        Assert.Contains(products, p => p.Sku == "SKU-002");
        Assert.Contains(products, p => p.Sku == "SKU-003");
    }

    [Fact]
    public async Task CanUseAggregationsOnExternalOdbcTable()
    {
        // Arrange
        await CreateMsSqlProductsAndData();

        // Act
        await using var chContext = CreateClickHouseContext();

        var count = await chContext.ExternalOdbcProducts.CountAsync();
        var minPrice = await chContext.ExternalOdbcProducts.MinAsync(p => p.Price);
        var maxPrice = await chContext.ExternalOdbcProducts.MaxAsync(p => p.Price);

        // Assert
        Assert.Equal(3, count);
        Assert.Equal(19.99m, minPrice);
        Assert.Equal(39.99m, maxPrice);
    }

    [Fact]
    public async Task GeneratedSql_ContainsOdbcFunction()
    {
        // Arrange
        await using var chContext = CreateClickHouseContext();

        // Act
        var query = chContext.ExternalOdbcCustomers
            .Where(c => c.Name == "Test");

        var sql = query.ToQueryString();

        // Assert
        Assert.Contains("odbc(", sql);
        Assert.Contains("customers", sql);
    }

    [Fact]
    public async Task CanInsertIntoExternalOdbcTable_ViaRawSql()
    {
        // Arrange: Set up MSSQL with the table
        await CreateMsSqlTableAndData();

        // Act: Insert via ClickHouse INSERT INTO FUNCTION using raw SQL
        await using var chContext = CreateClickHouseContext();

        // ODBC function format: odbc('DSN', 'database', 'table')
        var sql = $"""
            INSERT INTO FUNCTION odbc('{DsnName}', 'master', 'customers')
            (Name, Email) VALUES ('Diana', 'diana@example.com')
            """;

        await chContext.Database.ExecuteSqlRawAsync(sql);

        // Assert: Verify in MSSQL
        await using var connection = new SqlConnection(_msSqlContainer.GetConnectionString());
        await connection.OpenAsync();

        await using var cmd = new SqlCommand("SELECT Name, Email FROM customers WHERE Name = 'Diana'", connection);
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("Diana", reader.GetString(0));
        Assert.Equal("diana@example.com", reader.GetString(1));
    }

    #region Helper Methods

    private async Task CreateMsSqlTableAndData()
    {
        await using var connection = new SqlConnection(_msSqlContainer.GetConnectionString());
        await connection.OpenAsync();

        // Create table
        await using var createCmd = new SqlCommand("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='customers' AND xtype='U')
            CREATE TABLE customers (
                Id INT IDENTITY(1,1) PRIMARY KEY,
                Name NVARCHAR(255) NOT NULL,
                Email NVARCHAR(255) NOT NULL
            )
            """, connection);
        await createCmd.ExecuteNonQueryAsync();

        // Clear existing data
        await using var deleteCmd = new SqlCommand("DELETE FROM customers", connection);
        await deleteCmd.ExecuteNonQueryAsync();

        // Insert test data
        await using var insertCmd = new SqlCommand("""
            INSERT INTO customers (Name, Email) VALUES
            ('Alice', 'alice@example.com'),
            ('Bob', 'bob@example.com'),
            ('Charlie', 'charlie@example.com')
            """, connection);
        await insertCmd.ExecuteNonQueryAsync();
    }

    private async Task CreateMsSqlProductsAndData()
    {
        await using var connection = new SqlConnection(_msSqlContainer.GetConnectionString());
        await connection.OpenAsync();

        // Create table
        await using var createCmd = new SqlCommand("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='products' AND xtype='U')
            CREATE TABLE products (
                Id INT IDENTITY(1,1) PRIMARY KEY,
                Sku NVARCHAR(50) NOT NULL,
                Name NVARCHAR(255) NOT NULL,
                Price DECIMAL(18, 4) NOT NULL
            )
            """, connection);
        await createCmd.ExecuteNonQueryAsync();

        // Clear existing data
        await using var deleteCmd = new SqlCommand("DELETE FROM products", connection);
        await deleteCmd.ExecuteNonQueryAsync();

        // Insert test data
        await using var insertCmd = new SqlCommand("""
            INSERT INTO products (Sku, Name, Price) VALUES
            ('SKU-001', 'Widget', 19.99),
            ('SKU-002', 'Gadget', 29.99),
            ('SKU-003', 'Gizmo', 39.99)
            """, connection);
        await insertCmd.ExecuteNonQueryAsync();
    }

    private ClickHouseOdbcTestContext CreateClickHouseContext()
    {
        var host = _clickHouseContainer.Hostname;
        var port = _clickHouseContainer.GetMappedPublicPort(8123);
        var connectionString = $"Host={host};Port={port};Database=default";

        var options = new DbContextOptionsBuilder<ClickHouseOdbcTestContext>()
            .UseClickHouse(connectionString)
            .Options;

        return new ClickHouseOdbcTestContext(options);
    }

    #endregion
}

#region ODBC External Entities and Context

public class ChExternalOdbcCustomer
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}

public class ChExternalOdbcProduct
{
    public int Id { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

public class ClickHouseOdbcTestContext : DbContext
{
    public ClickHouseOdbcTestContext(DbContextOptions<ClickHouseOdbcTestContext> options)
        : base(options)
    {
    }

    public DbSet<ChExternalOdbcCustomer> ExternalOdbcCustomers => Set<ChExternalOdbcCustomer>();
    public DbSet<ChExternalOdbcProduct> ExternalOdbcProducts => Set<ChExternalOdbcProduct>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // External ODBC customers (read-only by default)
        modelBuilder.ExternalOdbcEntity<ChExternalOdbcCustomer>(ext => ext
            .FromTable("customers")
            .Dsn(env: "MSSQL_DSN")
            .Database("master"));

        // External ODBC products (read-only by default)
        modelBuilder.ExternalOdbcEntity<ChExternalOdbcProduct>(ext => ext
            .FromTable("products")
            .Dsn(env: "MSSQL_DSN")
            .Database("master"));
    }
}

#endregion

/// <summary>
/// Collection definition for ODBC tests to ensure they don't run in parallel
/// with other integration tests that may conflict with shared resources.
/// </summary>
[CollectionDefinition("ODBC Tests")]
public class OdbcTestsCollection : ICollectionFixture<OdbcTestsFixture>
{
}

/// <summary>
/// Fixture for ODBC tests that can be shared across test classes.
/// Currently empty but can be used for shared setup/teardown.
/// </summary>
public class OdbcTestsFixture
{
}
