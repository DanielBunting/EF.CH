using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Database;

/// <summary>
/// Theme 3b coverage — every <c>Create*</c> method on
/// <see cref="DatabaseFacade"/> emits <c>IF NOT EXISTS</c> by default
/// (idempotent), and every <c>Drop*</c> method comes in two flavours:
/// the strict form throws on missing, the <c>*IfExists*</c> form is silent.
/// </summary>
public class IfExistsDefaultsTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public async Task InitializeAsync() => await _container.StartAsync();
    public async Task DisposeAsync() => await _container.DisposeAsync();

    private string Conn => _container.GetConnectionString();

    private IfExistsContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<IfExistsContext>()
            .UseClickHouse(Conn)
            .Options;
        return new IfExistsContext(options);
    }

    // ----------------------------------------------------------------
    // Plain views — DropViewAsync split
    // ----------------------------------------------------------------

    [Fact]
    public async Task CreateViewAsync_IsIdempotent()
    {
        await using var ctx = CreateContext();
        await ctx.Database.ExecuteSqlRawAsync("CREATE TABLE IF NOT EXISTS source_idem (id UInt64) ENGINE = MergeTree ORDER BY id");

        await ctx.Database.CreateViewAsync("v_idempotent", "SELECT id FROM source_idem");
        // Same call again must not throw — IF NOT EXISTS is always emitted.
        await ctx.Database.CreateViewAsync("v_idempotent", "SELECT id FROM source_idem");

        await ctx.Database.DropViewIfExistsAsync("v_idempotent");
        await ctx.Database.ExecuteSqlRawAsync("DROP TABLE source_idem");
    }

    [Fact]
    public async Task DropViewAsync_StrictThrowsOnMissing_DropViewIfExistsAsync_IsSilent()
    {
        await using var ctx = CreateContext();

        // Strict drop on a non-existent view throws.
        await Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(() =>
            ctx.Database.DropViewAsync("missing_view_strict"));

        // The IfExists variant is silent.
        await ctx.Database.DropViewIfExistsAsync("missing_view_strict");
    }

    // ----------------------------------------------------------------
    // Parameterized views — DropParameterizedViewAsync split
    // ----------------------------------------------------------------

    [Fact]
    public async Task CreateParameterizedViewAsync_IsIdempotent()
    {
        await using var ctx = CreateContext();

        await ctx.Database.CreateParameterizedViewAsync(
            "param_idem",
            "SELECT 1 AS value WHERE 1 = {p:UInt8}");
        await ctx.Database.CreateParameterizedViewAsync(
            "param_idem",
            "SELECT 1 AS value WHERE 1 = {p:UInt8}");

        await ctx.Database.DropParameterizedViewIfExistsAsync("param_idem");
    }

    [Fact]
    public async Task DropParameterizedViewAsync_StrictThrows_IfExistsIsSilent()
    {
        await using var ctx = CreateContext();

        await Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(() =>
            ctx.Database.DropParameterizedViewAsync("missing_param_strict"));

        await ctx.Database.DropParameterizedViewIfExistsAsync("missing_param_strict");
    }

    // ----------------------------------------------------------------
    // Ensure* — model-driven creation, always idempotent
    // ----------------------------------------------------------------

    [Fact]
    public async Task EnsureParameterizedViewsAsync_AlwaysIdempotent()
    {
        await using var ctx = CreateContext();

        // Create the source table for the parameterized view (the model-driven path
        // emits a CREATE VIEW that SELECTs from the source table). We do this by
        // raw SQL rather than via EnsureCreatedAsync so the test focuses on the
        // ensure-views idempotency contract.
        await ctx.Database.ExecuteSqlRawAsync(
            "CREATE TABLE IF NOT EXISTS ensure_param_source (id UInt64, name String) ENGINE = MergeTree ORDER BY id");

        var first = await ctx.Database.EnsureParameterizedViewsAsync();
        // Second call must not throw — IF NOT EXISTS is forced on regardless of
        // the AsParameterizedView config.
        var second = await ctx.Database.EnsureParameterizedViewsAsync();

        Assert.Equal(first, second);

        await ctx.Database.DropParameterizedViewIfExistsAsync("ensure_param_view");
        await ctx.Database.ExecuteSqlRawAsync("DROP TABLE ensure_param_source");
    }

    [Fact]
    public async Task EnsureParameterizedViewAsync_AlwaysIdempotent()
    {
        await using var ctx = CreateContext();

        await ctx.Database.ExecuteSqlRawAsync(
            "CREATE TABLE IF NOT EXISTS ensure_param_source (id UInt64, name String) ENGINE = MergeTree ORDER BY id");

        await ctx.Database.EnsureParameterizedViewAsync<EnsureParamView>();
        await ctx.Database.EnsureParameterizedViewAsync<EnsureParamView>();

        await ctx.Database.DropParameterizedViewIfExistsAsync("ensure_param_view");
        await ctx.Database.ExecuteSqlRawAsync("DROP TABLE ensure_param_source");
    }

    [Fact]
    public void GetParameterizedViewSql_AlwaysIncludesIfNotExists()
    {
        using var ctx = CreateContext();
        var sql = ctx.Database.GetParameterizedViewSql<EnsureParamView>();
        Assert.Contains("IF NOT EXISTS", sql);
    }
}

#region Test entities

public class EnsureParamSource
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class EnsureParamView
{
    public ulong Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public class IfExistsContext : DbContext
{
    public IfExistsContext(DbContextOptions<IfExistsContext> options) : base(options) { }

    public DbSet<EnsureParamSource> ParamSources => Set<EnsureParamSource>();
    public DbSet<EnsureParamView> ParamViews => Set<EnsureParamView>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<EnsureParamSource>(e =>
        {
            e.ToTable("ensure_param_source");
            e.HasKey(x => x.Id);
            e.UseMergeTree(x => x.Id);
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Name).HasColumnName("name");
        });

        modelBuilder.Entity<EnsureParamView>(e =>
        {
            e.AsParameterizedView<EnsureParamView, EnsureParamSource>(cfg => cfg
                .HasName("ensure_param_view")
                .FromTable()
                .Select(s => new EnsureParamView { Id = s.Id, Name = s.Name })
                .Parameter<ulong>("min_id")
                .Where((s, p) => s.Id >= p.Get<ulong>("min_id")));
        });
    }
}

#endregion
