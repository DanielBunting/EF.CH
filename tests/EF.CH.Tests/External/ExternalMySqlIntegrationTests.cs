using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using EF.CH.Extensions;
using EF.CH.External;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using Testcontainers.ClickHouse;
using Testcontainers.MySql;
using Xunit;

namespace EF.CH.Tests.External;

/// <summary>
/// Integration tests for external MySQL entities using Testcontainers.
/// Tests the mysql() table function integration between ClickHouse and MySQL.
/// </summary>
public class ExternalMySqlIntegrationTests : IAsyncLifetime
{
    private const string MySqlNetworkAlias = "mysql-db";
    private const string MySqlDatabase = "testdb";
    private const string MySqlUser = "testuser";
    private const string MySqlPassword = "testpassword";

    private readonly INetwork _network = new NetworkBuilder()
        .WithName(Guid.NewGuid().ToString("D"))
        .Build();

    private MySqlContainer _mySqlContainer = null!;
    private ClickHouseContainer _clickHouseContainer = null!;

    public async Task InitializeAsync()
    {
        // Create network first
        await _network.CreateAsync();

        // Create containers on the shared network
        _mySqlContainer = new MySqlBuilder()
            .WithImage("mysql:8.0")
            .WithNetwork(_network)
            .WithNetworkAliases(MySqlNetworkAlias)
            .WithDatabase(MySqlDatabase)
            .WithUsername(MySqlUser)
            .WithPassword(MySqlPassword)
            .Build();

        _clickHouseContainer = new ClickHouseBuilder()
            .WithImage("clickhouse/clickhouse-server:latest")
            .WithNetwork(_network)
            .Build();

        // Start both containers in parallel
        await Task.WhenAll(
            _clickHouseContainer.StartAsync(),
            _mySqlContainer.StartAsync());

        // Set environment variables using the internal network address
        SetupEnvironmentVariables();
    }

    private void SetupEnvironmentVariables()
    {
        // Use the internal network alias and port for container-to-container communication
        Environment.SetEnvironmentVariable("MYSQL_HOST", $"{MySqlNetworkAlias}:3306");
        Environment.SetEnvironmentVariable("MYSQL_DATABASE", MySqlDatabase);
        Environment.SetEnvironmentVariable("MYSQL_USER", MySqlUser);
        Environment.SetEnvironmentVariable("MYSQL_PASSWORD", MySqlPassword);
    }

    public async Task DisposeAsync()
    {
        // Clean up environment variables
        Environment.SetEnvironmentVariable("MYSQL_HOST", null);
        Environment.SetEnvironmentVariable("MYSQL_DATABASE", null);
        Environment.SetEnvironmentVariable("MYSQL_USER", null);
        Environment.SetEnvironmentVariable("MYSQL_PASSWORD", null);

        await _clickHouseContainer.DisposeAsync();
        await _mySqlContainer.DisposeAsync();
        await _network.DisposeAsync();
    }

    [Fact]
    public async Task CanQueryExternalMySqlTable()
    {
        // Arrange: Set up MySQL with data
        await CreateMySqlTableAndData();

        // Act: Query from ClickHouse using external entity
        await using var chContext = CreateClickHouseContext();

        var customers = await chContext.ExternalMySqlCustomers
            .Where(c => c.name.StartsWith("A") || c.name.StartsWith("B"))
            .OrderBy(c => c.name)
            .ToListAsync();

        // Assert
        Assert.Equal(2, customers.Count);
        Assert.Equal("Alice", customers[0].name);
        Assert.Equal("Bob", customers[1].name);
    }

    [Fact]
    public async Task CanQueryExternalMySqlWithProjection()
    {
        // Arrange
        await CreateMySqlProductsAndData();

        // Act
        await using var chContext = CreateClickHouseContext();

        var products = await chContext.ExternalMySqlProducts
            .Where(p => p.price > 20.0)
            .Select(p => new { p.sku, p.name })
            .ToListAsync();

        // Assert
        Assert.Equal(2, products.Count);
        Assert.Contains(products, p => p.sku == "SKU-002");
        Assert.Contains(products, p => p.sku == "SKU-003");
    }

    [Fact]
    public async Task CanUseAggregationsOnExternalMySqlTable()
    {
        // Arrange
        await CreateMySqlProductsAndData();

        // Act
        await using var chContext = CreateClickHouseContext();

        var count = await chContext.ExternalMySqlProducts.CountAsync();
        var minPrice = await chContext.ExternalMySqlProducts.MinAsync(p => p.price);
        var maxPrice = await chContext.ExternalMySqlProducts.MaxAsync(p => p.price);

        // Assert
        Assert.Equal(3, count);
        Assert.Equal(19.99, minPrice, precision: 2);
        Assert.Equal(39.99, maxPrice, precision: 2);
    }

    [Fact]
    public async Task GeneratedSql_ContainsMySqlFunction()
    {
        // Arrange
        await using var chContext = CreateClickHouseContext();

        // Act
        var query = chContext.ExternalMySqlCustomers
            .Where(c => c.name == "Test");

        var sql = query.ToQueryString();

        // Assert
        Assert.Contains("mysql(", sql);
        Assert.Contains("customers", sql);
    }

    [Fact]
    public async Task CanInsertIntoExternalMySqlTable_ViaRawSql()
    {
        // Arrange: Set up MySQL with the table
        await CreateMySqlTableAndData();

        // Act: Insert via ClickHouse INSERT INTO FUNCTION using raw SQL
        await using var chContext = CreateClickHouseContext();

        var sql = $"""
            INSERT INTO FUNCTION mysql('{MySqlNetworkAlias}:3306', '{MySqlDatabase}', 'customers', '{MySqlUser}', '{MySqlPassword}')
            (name, email) VALUES ('Diana', 'diana@example.com')
            """;

        await chContext.Database.ExecuteSqlRawAsync(sql);

        // Assert: Verify in MySQL
        await using var connection = new MySqlConnection(_mySqlContainer.GetConnectionString());
        await connection.OpenAsync();

        await using var cmd = new MySqlCommand("SELECT name, email FROM customers WHERE name = 'Diana'", connection);
        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("Diana", reader.GetString("name"));
        Assert.Equal("diana@example.com", reader.GetString("email"));
    }

    #region Helper Methods

    private async Task CreateMySqlTableAndData()
    {
        await using var connection = new MySqlConnection(_mySqlContainer.GetConnectionString());
        await connection.OpenAsync();

        // Create table
        await using var createCmd = new MySqlCommand("""
            CREATE TABLE IF NOT EXISTS customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL
            )
            """, connection);
        await createCmd.ExecuteNonQueryAsync();

        // Clear existing data
        await using var deleteCmd = new MySqlCommand("DELETE FROM customers", connection);
        await deleteCmd.ExecuteNonQueryAsync();

        // Insert test data
        await using var insertCmd = new MySqlCommand("""
            INSERT INTO customers (name, email) VALUES
            ('Alice', 'alice@example.com'),
            ('Bob', 'bob@example.com'),
            ('Charlie', 'charlie@example.com')
            """, connection);
        await insertCmd.ExecuteNonQueryAsync();
    }

    private async Task CreateMySqlProductsAndData()
    {
        await using var connection = new MySqlConnection(_mySqlContainer.GetConnectionString());
        await connection.OpenAsync();

        // Create table
        // Note: Using DOUBLE instead of DECIMAL because ClickHouse's mysql() function
        // maps MySQL DECIMAL to String, causing type mismatches
        await using var createCmd = new MySqlCommand("""
            CREATE TABLE IF NOT EXISTS products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sku VARCHAR(50) NOT NULL,
                name VARCHAR(255) NOT NULL,
                price DOUBLE NOT NULL
            )
            """, connection);
        await createCmd.ExecuteNonQueryAsync();

        // Clear existing data
        await using var deleteCmd = new MySqlCommand("DELETE FROM products", connection);
        await deleteCmd.ExecuteNonQueryAsync();

        // Insert test data
        await using var insertCmd = new MySqlCommand("""
            INSERT INTO products (sku, name, price) VALUES
            ('SKU-001', 'Widget', 19.99),
            ('SKU-002', 'Gadget', 29.99),
            ('SKU-003', 'Gizmo', 39.99)
            """, connection);
        await insertCmd.ExecuteNonQueryAsync();
    }

    private ClickHouseMySqlTestContext CreateClickHouseContext()
    {
        var options = new DbContextOptionsBuilder<ClickHouseMySqlTestContext>()
            .UseClickHouse(_clickHouseContainer.GetConnectionString())
            .Options;

        return new ClickHouseMySqlTestContext(options);
    }

    #endregion
}

#region MySQL External Entities and Context

public class ChExternalMySqlCustomer
{
    public int id { get; set; }
    public string name { get; set; } = string.Empty;
    public string email { get; set; } = string.Empty;
}

public class ChExternalMySqlProduct
{
    public int id { get; set; }
    public string sku { get; set; } = string.Empty;
    public string name { get; set; } = string.Empty;
    public double price { get; set; }  // Using double because ClickHouse maps MySQL DECIMAL to String
}

public class ClickHouseMySqlTestContext : DbContext
{
    public ClickHouseMySqlTestContext(DbContextOptions<ClickHouseMySqlTestContext> options)
        : base(options)
    {
    }

    public DbSet<ChExternalMySqlCustomer> ExternalMySqlCustomers => Set<ChExternalMySqlCustomer>();
    public DbSet<ChExternalMySqlProduct> ExternalMySqlProducts => Set<ChExternalMySqlProduct>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // External MySQL customers (read-only)
        modelBuilder.ExternalMySqlEntity<ChExternalMySqlCustomer>(ext => ext
            .FromTable("customers")
            .Connection(c => c
                .HostPort(env: "MYSQL_HOST")
                .Database(env: "MYSQL_DATABASE")
                .Credentials("MYSQL_USER", "MYSQL_PASSWORD")));

        // External MySQL products (read-only)
        modelBuilder.ExternalMySqlEntity<ChExternalMySqlProduct>(ext => ext
            .FromTable("products")
            .Connection(c => c
                .HostPort(env: "MYSQL_HOST")
                .Database(env: "MYSQL_DATABASE")
                .Credentials("MYSQL_USER", "MYSQL_PASSWORD")));
    }
}

#endregion
