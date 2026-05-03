using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.SystemTests.Cluster;

/// <summary>
/// End-to-end coverage of non-default credentials in the connection string. The
/// existing fixtures use the default Testcontainers credentials so authentication
/// is implicitly exercised, but never *negatively* — a wrong-password connection
/// should fail with a server-reported auth error rather than a malformed query.
///
/// TLS coverage is intentionally omitted: the official ClickHouse image needs a
/// custom server-side TLS config (cert + key + dhparams), which is non-trivial to
/// inject through Testcontainers' resource-mapping. If/when this matters, add a
/// dedicated TlsClickHouseFixture that ships a self-signed cert pair.
/// </summary>
public sealed class AuthenticationTests : IAsyncLifetime
{
    private const string CustomUser = "ef_ch_test_user";
    private const string CustomPassword = "p@ssw0rd!ef-ch";

    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage(ClusterConfigTemplates.ClickHouseImage)
        .WithUsername(CustomUser)
        .WithPassword(CustomPassword)
        .Build();

    public Task InitializeAsync() => _container.StartAsync();
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    [Fact]
    public async Task CorrectCredentials_ConnectAndQuery_Succeeds()
    {
        var conn = _container.GetConnectionString();
        var options = new DbContextOptionsBuilder<Ctx>().UseClickHouse(conn).Options;
        await using var ctx = new Ctx(options);
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.Add(new Row { Id = 1, Tag = "auth-ok" });
        await ctx.SaveChangesAsync();

        var fetched = await ctx.Rows.SingleAsync();
        Assert.Equal("auth-ok", fetched.Tag);
    }

    [Fact]
    public async Task WrongPassword_FailsWithAuthenticationError()
    {
        // Replace the password in the connection string with a wrong value.
        var connString = _container.GetConnectionString();
        var rewritten = ReplaceField(connString, "Password", "definitely-not-the-real-password");

        var options = new DbContextOptionsBuilder<Ctx>().UseClickHouse(rewritten).Options;
        await using var ctx = new Ctx(options);

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await ctx.Database.OpenConnectionAsync();
            await ctx.Database.ExecuteSqlRawAsync("SELECT 1");
        });
        // The error surfaces as a ClickHouse server exception. Authentication failures
        // mention the user (code 516 / AUTHENTICATION_FAILED) — keep the assertion loose
        // so it still passes if the driver wraps the inner exception.
        var message = (ex.InnerException ?? ex).Message;
        Assert.Contains(CustomUser, message, StringComparison.OrdinalIgnoreCase);
    }

    private static string ReplaceField(string connString, string field, string newValue)
    {
        var parts = connString.Split(';', StringSplitOptions.RemoveEmptyEntries);
        for (var i = 0; i < parts.Length; i++)
        {
            var kv = parts[i].Split('=', 2);
            if (kv.Length == 2 && kv[0].Trim().Equals(field, StringComparison.OrdinalIgnoreCase))
            {
                parts[i] = $"{kv[0]}={newValue}";
            }
        }
        return string.Join(";", parts);
    }

    public sealed class Row { public uint Id { get; set; } public string Tag { get; set; } = ""; }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e => { e.ToTable("Auth_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id); });
    }
}
