using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using Xunit;

namespace EF.CH.SystemTests.External;

/// <summary>
/// Integration tests for the <c>ExternalMySqlEntity</c> mapping. ClickHouse queries MySQL
/// through the <c>mysql()</c> table function. Uses <see cref="MySqlFixture"/> so ClickHouse
/// can reach MySQL by container hostname.
/// </summary>
[Collection(MySqlCollection.Name)]
public sealed class MySqlExternalEntityTests
{
    private readonly MySqlFixture _fx;
    public MySqlExternalEntityTests(MySqlFixture fx) => _fx = fx;

    [Fact]
    public async Task ExternalMySqlEntity_QueryProjection()
    {
        // Note: DECIMAL columns from MySQL surface as String through ClickHouse's mysql() table
        // function in this driver/version, which can't auto-cast to Decimal at the EF reader.
        // Sticking to int + string columns here; decimal-via-MySQL is a separate type-mapping concern.
        await SeedMySqlAsync(
            (1, "Widget", 100),
            (2, "Gadget", 200),
            (3, "Gizmo", 300));

        await using var ctx = CreateContext();

        var products = await ctx.ExternalProducts
            .OrderBy(p => p.id)
            .ToListAsync();

        Assert.Equal(3, products.Count);
        Assert.Equal("Widget", products[0].name);
        Assert.Equal(200, products[1].stock);
    }

    private async Task SeedMySqlAsync(params (int id, string name, int stock)[] rows)
    {
        await using var conn = new MySqlConnection(_fx.MySqlConnectionString);
        await conn.OpenAsync();
        await Exec(conn, "DROP TABLE IF EXISTS products");
        await Exec(conn, @"
            CREATE TABLE products (
                id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                stock INT NOT NULL
            )");
        foreach (var r in rows)
        {
            await using var cmd = new MySqlCommand("INSERT INTO products (id, name, stock) VALUES (@i, @n, @s)", conn);
            cmd.Parameters.AddWithValue("@i", r.id);
            cmd.Parameters.AddWithValue("@n", r.name);
            cmd.Parameters.AddWithValue("@s", r.stock);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task Exec(MySqlConnection conn, string sql)
    {
        await using var cmd = new MySqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private MySqlExtCtx CreateContext()
    {
        Environment.SetEnvironmentVariable("MYSQL_HOST", $"{_fx.MySqlHostAlias}:3306");
        Environment.SetEnvironmentVariable("MYSQL_DATABASE", _fx.MySqlDatabase);
        Environment.SetEnvironmentVariable("MYSQL_USER", _fx.MySqlUser);
        Environment.SetEnvironmentVariable("MYSQL_PASSWORD", _fx.MySqlPassword);

        var options = new DbContextOptionsBuilder<MySqlExtCtx>()
            .UseClickHouse(_fx.ClickHouseConnectionString)
            .Options;
        return new MySqlExtCtx(options);
    }

    public sealed class ChProduct
    {
        public int id { get; set; }
        public string name { get; set; } = string.Empty;
        public int stock { get; set; }
    }

    public sealed class MySqlExtCtx(DbContextOptions<MySqlExtCtx> o) : DbContext(o)
    {
        public DbSet<ChProduct> ExternalProducts => Set<ChProduct>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.ExternalMySqlEntity<ChProduct>(ext => ext
                .FromTable("products")
                .Connection(c => c
                    .HostPort(env: "MYSQL_HOST")
                    .Database(env: "MYSQL_DATABASE")
                    .Credentials("MYSQL_USER", "MYSQL_PASSWORD")));
        }
    }
}
