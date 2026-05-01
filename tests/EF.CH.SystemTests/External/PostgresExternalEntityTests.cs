using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Xunit;

namespace EF.CH.SystemTests.External;

/// <summary>
/// Integration tests for the <c>ExternalPostgresEntity</c> mapping. ClickHouse queries
/// PostgreSQL through the <c>postgresql()</c> table function. Uses
/// <see cref="PostgresFixture"/> so ClickHouse can reach Postgres by container hostname.
/// </summary>
[Collection(PostgresCollection.Name)]
public sealed class PostgresExternalEntityTests
{
    private readonly PostgresFixture _fx;
    public PostgresExternalEntityTests(PostgresFixture fx) => _fx = fx;

    [Fact]
    public async Task ExternalPostgresEntity_Query_ReturnsRows()
    {
        await SeedPostgresAsync(
            (1, "Alice", "alice@example.com"),
            (2, "Bob", "bob@example.com"),
            (3, "Charlie", "charlie@example.com"));

        await using var ctx = CreateContext();

        var customers = await ctx.ExternalCustomers
            .Where(c => c.name.StartsWith("A") || c.name.StartsWith("B"))
            .OrderBy(c => c.name)
            .ToListAsync();

        Assert.Equal(2, customers.Count);
        Assert.Equal("Alice", customers[0].name);
        Assert.Equal("Bob", customers[1].name);
    }

    [Fact]
    public async Task ExternalPostgresEntity_GeneratedSql_ContainsPostgresqlFunction()
    {
        await using var ctx = CreateContext();
        var sql = ctx.ExternalCustomers.Where(c => c.name == "X").ToQueryString();
        Assert.Contains("postgresql(", sql);
        Assert.Contains("customers", sql);
    }

    private async Task SeedPostgresAsync(params (int id, string name, string email)[] rows)
    {
        await using var conn = new NpgsqlConnection(_fx.PostgresConnectionString);
        await conn.OpenAsync();
        await Exec(conn, "DROP TABLE IF EXISTS customers");
        await Exec(conn, @"
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )");
        foreach (var r in rows)
        {
            await using var cmd = new NpgsqlCommand("INSERT INTO customers (id, name, email) VALUES (@i, @n, @e)", conn);
            cmd.Parameters.AddWithValue("i", r.id);
            cmd.Parameters.AddWithValue("n", r.name);
            cmd.Parameters.AddWithValue("e", r.email);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task Exec(NpgsqlConnection conn, string sql)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private PgExtCtx CreateContext()
    {
        Environment.SetEnvironmentVariable("PG_HOST", $"{_fx.PostgresHostAlias}:5432");
        Environment.SetEnvironmentVariable("PG_DATABASE", _fx.PostgresDatabase);
        Environment.SetEnvironmentVariable("PG_USER", _fx.PostgresUser);
        Environment.SetEnvironmentVariable("PG_PASSWORD", _fx.PostgresPassword);

        var options = new DbContextOptionsBuilder<PgExtCtx>()
            .UseClickHouse(_fx.ClickHouseConnectionString)
            .Options;
        return new PgExtCtx(options);
    }

    public sealed class ChCustomer
    {
        public int id { get; set; }
        public string name { get; set; } = string.Empty;
        public string email { get; set; } = string.Empty;
    }

    public sealed class PgExtCtx(DbContextOptions<PgExtCtx> o) : DbContext(o)
    {
        public DbSet<ChCustomer> ExternalCustomers => Set<ChCustomer>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.ExternalPostgresEntity<ChCustomer>(ext => ext
                .FromTable("customers", "public")
                .Connection(c => c
                    .HostPort(env: "PG_HOST")
                    .Database(env: "PG_DATABASE")
                    .Credentials("PG_USER", "PG_PASSWORD")));
        }
    }
}
