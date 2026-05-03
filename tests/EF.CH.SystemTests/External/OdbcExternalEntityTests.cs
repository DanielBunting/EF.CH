using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.External;

/// <summary>
/// Integration tests for the <c>ExternalOdbcEntity</c> mapping (ODBC → MSSQL). The fixture
/// installs <c>unixodbc</c> + FreeTDS into the ClickHouse container at startup and pre-stages
/// an <c>/etc/odbc.ini</c> DSN pointing at the MSSQL container. ClickHouse queries MSSQL via
/// the <c>odbc()</c> table function. Smaller scope due to driver platform fragility.
/// </summary>
[Collection(OdbcMsSqlCollection.Name)]
public sealed class OdbcExternalEntityTests
{
    private readonly OdbcMsSqlFixture _fx;
    public OdbcExternalEntityTests(OdbcMsSqlFixture fx) => _fx = fx;

    [Fact(Skip = "Requires amd64 Linux + Microsoft ODBC driver for SQL Server. The driver isn't available for ARM64 (Apple Silicon, AWS Graviton). Existing EF.CH.Tests/External/ExternalOdbcMsSqlIntegrationTests.cs skips with the same reason. SQL-shape verification below covers config/translation.")]
    public async Task ExternalOdbcEntity_QueryFromMsSql_ReturnsRows()
    {
        await SeedMsSqlAsync(
            (1, "Region A"),
            (2, "Region B"));

        await using var ctx = CreateContext();

        var rows = await ctx.ExternalRegions.OrderBy(r => r.id).ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal("Region A", rows[0].name);
    }

    [Fact]
    public void ExternalOdbcEntity_QueryGeneration_IncludesOdbcFunction()
    {
        // Pin: configuration → translated SELECT references the odbc() table function with
        // the configured DSN. Live query is skipped on ARM64 platforms (see above).
        Environment.SetEnvironmentVariable("MSSQL_DSN", _fx.DsnName);

        var options = new DbContextOptionsBuilder<OdbcExtCtx>()
            .UseClickHouse(_fx.ClickHouseConnectionString)
            .Options;
        using var ctx = new OdbcExtCtx(options);

        var sql = ctx.ExternalRegions.ToQueryString();
        Assert.Contains("odbc(", sql);
        Assert.Contains("regions", sql);
    }

    private async Task SeedMsSqlAsync(params (int id, string name)[] rows)
    {
        await using var conn = new SqlConnection(_fx.MsSqlConnectionString);
        await conn.OpenAsync();
        await Exec(conn, "IF OBJECT_ID('regions', 'U') IS NOT NULL DROP TABLE regions");
        await Exec(conn, "CREATE TABLE regions (id INT PRIMARY KEY, name NVARCHAR(255) NOT NULL)");
        foreach (var r in rows)
        {
            await using var cmd = new SqlCommand("INSERT INTO regions (id, name) VALUES (@i, @n)", conn);
            cmd.Parameters.AddWithValue("@i", r.id);
            cmd.Parameters.AddWithValue("@n", r.name);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task Exec(SqlConnection conn, string sql)
    {
        await using var cmd = new SqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private OdbcExtCtx CreateContext()
    {
        Environment.SetEnvironmentVariable("MSSQL_DSN", _fx.DsnName);

        var options = new DbContextOptionsBuilder<OdbcExtCtx>()
            .UseClickHouse(_fx.ClickHouseConnectionString)
            .Options;
        return new OdbcExtCtx(options);
    }

    public sealed class ChRegion
    {
        public int id { get; set; }
        public string name { get; set; } = string.Empty;
    }

    public sealed class OdbcExtCtx(DbContextOptions<OdbcExtCtx> o) : DbContext(o)
    {
        public DbSet<ChRegion> ExternalRegions => Set<ChRegion>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.ExternalOdbcEntity<ChRegion>(ext => ext
                .FromTable("regions")
                .Dsn(env: "MSSQL_DSN"));
        }
    }
}
