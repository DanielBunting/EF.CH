using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Networks;
using EF.CH.Extensions;
using EF.CH.External;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Testcontainers.PostgreSql;
using Xunit;

namespace EF.CH.Tests;

/// <summary>
/// Integration tests for external PostgreSQL entities using Testcontainers.
/// Tests the postgresql() table function integration between ClickHouse and PostgreSQL.
/// </summary>
public class ExternalEntityIntegrationTests : IAsyncLifetime
{
    private const string PostgresNetworkAlias = "postgres-db";

    private readonly INetwork _network = new NetworkBuilder()
        .WithName(Guid.NewGuid().ToString("D"))
        .Build();

    private PostgreSqlContainer _postgresContainer = null!;
    private ClickHouseContainer _clickHouseContainer = null!;

    public async Task InitializeAsync()
    {
        // Create network first
        await _network.CreateAsync();

        // Create containers on the shared network
        _postgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .WithNetwork(_network)
            .WithNetworkAliases(PostgresNetworkAlias)
            .Build();

        _clickHouseContainer = new ClickHouseBuilder()
            .WithImage("clickhouse/clickhouse-server:latest")
            .WithNetwork(_network)
            .Build();

        // Start both containers in parallel
        await Task.WhenAll(
            _clickHouseContainer.StartAsync(),
            _postgresContainer.StartAsync());

        // Set environment variables using the internal network address
        SetupEnvironmentVariables();
    }

    private void SetupEnvironmentVariables()
    {
        // Use the internal network alias and port for container-to-container communication
        Environment.SetEnvironmentVariable("PG_HOST", $"{PostgresNetworkAlias}:5432");
        Environment.SetEnvironmentVariable("PG_DATABASE", "postgres");
        Environment.SetEnvironmentVariable("PG_USER", "postgres");
        Environment.SetEnvironmentVariable("PG_PASSWORD", "postgres");
    }

    public async Task DisposeAsync()
    {
        // Clean up environment variables
        Environment.SetEnvironmentVariable("PG_HOST", null);
        Environment.SetEnvironmentVariable("PG_DATABASE", null);
        Environment.SetEnvironmentVariable("PG_USER", null);
        Environment.SetEnvironmentVariable("PG_PASSWORD", null);

        await _clickHouseContainer.DisposeAsync();
        await _postgresContainer.DisposeAsync();
        await _network.DisposeAsync();
    }

    [Fact]
    public async Task CanQueryExternalPostgresTable()
    {
        // Arrange: Set up PostgreSQL with data
        await using var pgContext = CreatePostgresContext();
        await pgContext.Database.EnsureCreatedAsync();

        pgContext.Customers.AddRange(
            new PgCustomer { Name = "Alice", Email = "alice@example.com" },
            new PgCustomer { Name = "Bob", Email = "bob@example.com" },
            new PgCustomer { Name = "Charlie", Email = "charlie@example.com" }
        );
        await pgContext.SaveChangesAsync();

        // Act: Query from ClickHouse using external entity
        await using var chContext = CreateClickHouseContext();

        var customers = await chContext.ExternalCustomers
            .Where(c => c.name.StartsWith("A") || c.name.StartsWith("B"))
            .OrderBy(c => c.name)
            .ToListAsync();

        // Assert
        Assert.Equal(2, customers.Count);
        Assert.Equal("Alice", customers[0].name);
        Assert.Equal("Bob", customers[1].name);
    }

    [Fact]
    public async Task CanQueryExternalPostgresWithProjection()
    {
        // Arrange
        await using var pgContext = CreatePostgresContext();
        await pgContext.Database.EnsureCreatedAsync();

        pgContext.Products.AddRange(
            new PgProduct { Sku = "SKU-001", Name = "Widget", Price = 19.99m },
            new PgProduct { Sku = "SKU-002", Name = "Gadget", Price = 29.99m },
            new PgProduct { Sku = "SKU-003", Name = "Gizmo", Price = 39.99m }
        );
        await pgContext.SaveChangesAsync();

        // Act
        await using var chContext = CreateClickHouseContext();

        var products = await chContext.ExternalProducts
            .Where(p => p.price > 20m)
            .Select(p => new { p.sku, p.name })
            .ToListAsync();

        // Assert
        Assert.Equal(2, products.Count);
        Assert.Contains(products, p => p.sku == "SKU-002");
        Assert.Contains(products, p => p.sku == "SKU-003");
    }

    [Fact]
    public async Task CanJoinExternalPostgresWithNativeClickHouse()
    {
        // Arrange: Set up PostgreSQL customers
        await using var pgContext = CreatePostgresContext();
        await pgContext.Database.EnsureCreatedAsync();

        var alice = new PgCustomer { Name = "Alice", Email = "alice@example.com" };
        var bob = new PgCustomer { Name = "Bob", Email = "bob@example.com" };
        pgContext.Customers.AddRange(alice, bob);
        await pgContext.SaveChangesAsync();

        // Arrange: Set up ClickHouse orders
        await using var chContext = CreateClickHouseContext();
        await chContext.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "orders" (
                "Id" UUID,
                "CustomerId" Int32,
                "Amount" Decimal(18, 4),
                "OrderDate" DateTime64(3)
            )
            ENGINE = MergeTree()
            ORDER BY ("OrderDate", "Id")
            """);

        var now = DateTime.UtcNow;
        chContext.Orders.AddRange(
            new ChOrder { Id = Guid.NewGuid(), CustomerId = alice.Id, Amount = 100.00m, OrderDate = now.AddDays(-2) },
            new ChOrder { Id = Guid.NewGuid(), CustomerId = alice.Id, Amount = 150.00m, OrderDate = now.AddDays(-1) },
            new ChOrder { Id = Guid.NewGuid(), CustomerId = bob.Id, Amount = 200.00m, OrderDate = now }
        );
        await chContext.SaveChangesAsync();

        // Act: Join ClickHouse orders with external PostgreSQL customers
        var orderSummary = await chContext.Orders
            .Join(
                chContext.ExternalCustomers,
                o => o.CustomerId,
                c => c.id,
                (o, c) => new { CustomerName = c.name, o.Amount })
            .GroupBy(x => x.CustomerName)
            .Select(g => new { CustomerName = g.Key, TotalAmount = g.Sum(x => x.Amount) })
            .OrderBy(x => x.CustomerName)
            .ToListAsync();

        // Assert
        Assert.Equal(2, orderSummary.Count);
        Assert.Equal("Alice", orderSummary[0].CustomerName);
        Assert.Equal(250.00m, orderSummary[0].TotalAmount);
        Assert.Equal("Bob", orderSummary[1].CustomerName);
        Assert.Equal(200.00m, orderSummary[1].TotalAmount);
    }

    [Fact]
    public async Task CanInsertIntoExternalPostgresTable_ViaRawSql()
    {
        // Arrange: Set up PostgreSQL with the table
        await using var pgContext = CreatePostgresContext();
        await pgContext.Database.EnsureCreatedAsync();

        // Act: Insert via ClickHouse INSERT INTO FUNCTION using raw SQL
        // Note: EF Core's change tracker can't work with keyless external entities,
        // so we use raw SQL to test the INSERT INTO FUNCTION feature
        await using var chContext = CreateClickHouseContext();

        // ClickHouse requires column names to match the remote table exactly
        var sql = $"""
            INSERT INTO FUNCTION postgresql('{PostgresNetworkAlias}:5432', 'postgres', 'customers', 'postgres', 'postgres', 'public')
            (name, email) VALUES ('Diana', 'diana@example.com')
            """;

        await chContext.Database.ExecuteSqlRawAsync(sql);

        // Assert: Verify in PostgreSQL
        await using var pgVerifyContext = CreatePostgresContext();
        var customer = await pgVerifyContext.Customers
            .FirstOrDefaultAsync(c => c.Name == "Diana");

        Assert.NotNull(customer);
        Assert.Equal("diana@example.com", customer.Email);
    }

    [Fact]
    public void ExternalEntity_IsKeyless_CannotBeTracked()
    {
        // External entities are keyless, so EF Core's change tracker cannot work with them.
        // This is by design - external entities use table functions and don't support
        // EF Core's normal Add/Update/Remove operations.
        // INSERT support is available via raw SQL (INSERT INTO FUNCTION).

        using var chContext = CreateClickHouseContext();

        // Act & Assert: EF Core throws when trying to track keyless entities
        var ex = Assert.Throws<InvalidOperationException>(() =>
            chContext.ExternalCustomers.Add(new ChExternalCustomer
            {
                name = "Test",
                email = "test@example.com"
            }));

        Assert.Contains("does not have a primary key", ex.Message);
    }

    [Fact]
    public async Task CanUseAggregationsOnExternalTable()
    {
        // Arrange
        await using var pgContext = CreatePostgresContext();
        await pgContext.Database.EnsureCreatedAsync();

        pgContext.Products.AddRange(
            new PgProduct { Sku = "A1", Name = "Product A", Price = 10.00m },
            new PgProduct { Sku = "A2", Name = "Product A2", Price = 20.00m },
            new PgProduct { Sku = "B1", Name = "Product B", Price = 30.00m }
        );
        await pgContext.SaveChangesAsync();

        // Act
        await using var chContext = CreateClickHouseContext();

        // Use simpler aggregations that don't have type conversion issues
        var count = await chContext.ExternalProducts.CountAsync();
        var minPrice = await chContext.ExternalProducts.MinAsync(p => p.price);
        var maxPrice = await chContext.ExternalProducts.MaxAsync(p => p.price);

        // Assert
        Assert.Equal(3, count);
        Assert.Equal(10.00m, minPrice);
        Assert.Equal(30.00m, maxPrice);
    }

    [Fact]
    public async Task GeneratedSql_ContainsPostgresqlFunction()
    {
        // Arrange
        await using var chContext = CreateClickHouseContext();

        // Act
        var query = chContext.ExternalCustomers
            .Where(c => c.name == "Test");

        var sql = query.ToQueryString();

        // Assert
        Assert.Contains("postgresql(", sql);
        Assert.Contains("customers", sql);
    }

    #region Helper Methods

    private PostgresTestContext CreatePostgresContext()
    {
        var options = new DbContextOptionsBuilder<PostgresTestContext>()
            .UseNpgsql(_postgresContainer.GetConnectionString())
            .Options;

        return new PostgresTestContext(options);
    }

    private ClickHouseExternalTestContext CreateClickHouseContext()
    {
        var options = new DbContextOptionsBuilder<ClickHouseExternalTestContext>()
            .UseClickHouse(_clickHouseContainer.GetConnectionString())
            .Options;

        return new ClickHouseExternalTestContext(options);
    }

    #endregion
}

#region PostgreSQL Entities and Context

public class PgCustomer
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}

public class PgProduct
{
    public int Id { get; set; }
    public string Sku { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
}

public class PostgresTestContext : DbContext
{
    public PostgresTestContext(DbContextOptions<PostgresTestContext> options) : base(options) { }

    public DbSet<PgCustomer> Customers => Set<PgCustomer>();
    public DbSet<PgProduct> Products => Set<PgProduct>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PgCustomer>(entity =>
        {
            entity.ToTable("customers");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id").UseIdentityColumn();
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Email).HasColumnName("email");
        });

        modelBuilder.Entity<PgProduct>(entity =>
        {
            entity.ToTable("products");
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Id).HasColumnName("id").UseIdentityColumn();
            entity.Property(e => e.Sku).HasColumnName("sku");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Price).HasColumnName("price");
        });
    }
}

#endregion

#region ClickHouse Entities and Context

public class ChExternalCustomer
{
    public int id { get; set; }
    public string name { get; set; } = string.Empty;
    public string email { get; set; } = string.Empty;
}

public class ChExternalProduct
{
    public int id { get; set; }
    public string sku { get; set; } = string.Empty;
    public string name { get; set; } = string.Empty;
    public decimal price { get; set; }
}

public class ChOrder
{
    public Guid Id { get; set; }
    public int CustomerId { get; set; }
    public decimal Amount { get; set; }
    public DateTime OrderDate { get; set; }
}

public class ClickHouseExternalTestContext : DbContext
{
    public ClickHouseExternalTestContext(DbContextOptions<ClickHouseExternalTestContext> options)
        : base(options)
    {
    }

    public DbSet<ChExternalCustomer> ExternalCustomers => Set<ChExternalCustomer>();
    public DbSet<ChExternalProduct> ExternalProducts => Set<ChExternalProduct>();
    public DbSet<ChOrder> Orders => Set<ChOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // External PostgreSQL customers (read-only)
        modelBuilder.ExternalPostgresEntity<ChExternalCustomer>(ext => ext
            .FromTable("customers", "public")
            .Connection(c => c
                .HostPort(env: "PG_HOST")
                .Database(env: "PG_DATABASE")
                .Credentials("PG_USER", "PG_PASSWORD")));

        // External PostgreSQL products (read-only)
        modelBuilder.ExternalPostgresEntity<ChExternalProduct>(ext => ext
            .FromTable("products", "public")
            .Connection(c => c
                .HostPort(env: "PG_HOST")
                .Database(env: "PG_DATABASE")
                .Credentials("PG_USER", "PG_PASSWORD")));

        // Native ClickHouse orders
        modelBuilder.Entity<ChOrder>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(e => e.Id);
            entity.UseMergeTree(x => new { x.OrderDate, x.Id });
        });
    }
}

#endregion
