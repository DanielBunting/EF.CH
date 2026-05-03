using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Integration tests for plain (non-materialized, non-parameterized) view APIs against a
/// real ClickHouse server. Covers the public surface in
/// <see cref="ClickHouseDatabaseExtensions"/>, <see cref="ClickHouseViewExtensions"/>, and
/// the entity-builder methods <c>HasView</c> / <c>AsView</c> / <c>AsViewRaw</c> /
/// <c>AsViewDeferred</c>, including the <c>EnsureCreatedAsync</c> post-pass that creates
/// configured views alongside their source tables.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class PlainViewLifecycleTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public PlainViewLifecycleTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task EnsureCreatedAsync_CreatesSourceAndView()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        // EnsureCreatedAsync must create both the source table (from ToTable) and the view
        // (from AsView) — and the view's SELECT must use the EF-mapped column names
        // (user_id, name, is_active) rather than the C# property names.
        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "users"));
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "active_users"));

        await ctx.Database.ExecuteSqlRawAsync(@"
            INSERT INTO users (user_id, name, is_active) VALUES
            (1, 'Alice', 1), (2, 'Bob', 0), (3, 'Carol', 1)");

        var rows = await ctx.Set<ActiveUserView>().OrderBy(v => v.UserId).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0].Name);
        Assert.Equal("Carol", rows[1].Name);
    }

    [Fact]
    public async Task EnsureCreatedAsync_IsIdempotent()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        await ctx.Database.EnsureCreatedAsync();
        await ctx.Database.EnsureCreatedAsync(); // second call must be a no-op

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "active_users"));
    }

    [Fact]
    public async Task EnsureCreatedAsync_SkipsDeferredViews()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<MixedCtx>(Conn);

        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "active_users"));
        Assert.False(await RawClickHouse.TableExistsAsync(Conn, "raw_sample"));
    }

    [Fact]
    public async Task EnsureCreatedAsync_GracefullySkipsHasViewOnly()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<HasViewOnlyCtx>(Conn);

        // HasView<T>(name) carries no DDL metadata. EnsureCreatedAsync must skip it
        // silently rather than throwing — the view is presumed to live outside EF.CH.
        await ctx.Database.EnsureCreatedAsync();

        Assert.False(await RawClickHouse.TableExistsAsync(Conn, "basic_view"));
    }

    [Fact]
    public async Task EnsureViewAsync_CreatesAndQueriesView()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE users (
                user_id UInt64,
                name String,
                is_active UInt8
            ) ENGINE = MergeTree() ORDER BY user_id");

        await ctx.Database.ExecuteSqlRawAsync(@"
            INSERT INTO users (user_id, name, is_active) VALUES
            (1, 'Alice', 1), (2, 'Bob', 0), (3, 'Carol', 1)");

        await ctx.Database.EnsureViewAsync<ActiveUserView>();
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "active_users"));

        var rows = await ctx.Set<ActiveUserView>().OrderBy(v => v.UserId).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0].Name);
        Assert.Equal("Carol", rows[1].Name);
    }

    [Fact]
    public async Task CreateViewAsync_ThenFromView_ComposesWithLinq()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<EmptyCtx>(Conn);

        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE source_rows (
                id UInt64,
                name String,
                score UInt32
            ) ENGINE = MergeTree() ORDER BY id");
        await ctx.Database.ExecuteSqlRawAsync(@"
            INSERT INTO source_rows (id, name, score) VALUES
            (1, 'one', 5), (2, 'two', 15), (3, 'three', 25)");

        await ctx.Database.CreateViewAsync(
            "scored_view",
            "SELECT id AS \"Id\", name AS \"Name\", score AS \"Score\" FROM source_rows");

        var rows = await ctx.FromView<ScoredView>("scored_view")
            .Where(r => r.Score >= 15)
            .OrderByDescending(r => r.Score)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal(25u, rows[0].Score);
        Assert.Equal(15u, rows[1].Score);
    }

    [Fact]
    public async Task EnsureViewsAsync_SkipsDeferredViews()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<MixedCtx>(Conn);

        // Only the source for ActiveUserView is needed; raw_sample is deferred so it won't be created.
        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE users (user_id UInt64, name String, is_active UInt8)
            ENGINE = MergeTree() ORDER BY user_id");

        var created = await ctx.Database.EnsureViewsAsync();

        Assert.Equal(1, created);
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "active_users"));
        Assert.False(await RawClickHouse.TableExistsAsync(Conn, "raw_sample"));
    }

    [Fact]
    public async Task EnsureViewsAsync_SkipsHasViewOnly()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<HasViewOnlyCtx>(Conn);

        var created = await ctx.Database.EnsureViewsAsync();

        // HasView<T>(name) carries no metadata for DDL — EnsureViewsAsync must skip it.
        Assert.Equal(0, created);
        Assert.False(await RawClickHouse.TableExistsAsync(Conn, "basic_view"));
    }

    [Fact]
    public async Task EnsureViewAsync_OrReplace_ReplacesBody()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE users (user_id UInt64, name String, is_active UInt8)
            ENGINE = MergeTree() ORDER BY user_id");
        await ctx.Database.ExecuteSqlRawAsync(
            "INSERT INTO users (user_id, name, is_active) VALUES (1, 'Alice', 1)");

        await ctx.Database.EnsureViewAsync<ActiveUserView>();
        var firstSql = ctx.Database.GetViewSql<ActiveUserView>();
        Assert.Contains("OR REPLACE", firstSql);

        // Re-issuing succeeds because the view config carries OR REPLACE.
        await ctx.Database.EnsureViewAsync<ActiveUserView>();

        var rows = await ctx.Set<ActiveUserView>().ToListAsync();
        Assert.Single(rows);
    }

    [Fact]
    public async Task DropViewAsync_StrictThrowsAndIfExistsIsSilent()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<EmptyCtx>(Conn);

        await ctx.Database.CreateViewAsync("temp_v", "SELECT 1 AS x");
        await ctx.Database.CreateViewAsync("temp_v", "SELECT 1 AS x", orReplace: true);

        await ctx.Database.DropViewAsync("temp_v");

        await Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(() =>
            ctx.Database.DropViewAsync("temp_v"));

        await ctx.Database.DropViewIfExistsAsync("temp_v"); // silent no-op
    }

    [Fact]
    public async Task FromView_QualifiesBySchema()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<EmptyCtx>(Conn);

        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE schema_rows (id UInt64, name String) ENGINE = MergeTree() ORDER BY id");
        await ctx.Database.ExecuteSqlRawAsync(
            "INSERT INTO schema_rows (id, name) VALUES (1, 'one'), (2, 'two')");

        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE VIEW IF NOT EXISTS ""default"".""schema_view"" AS
            SELECT id AS ""Id"", name AS ""Name"" FROM schema_rows");

        var rows = await ctx.FromView<BasicRow>("schema_view", schema: "default")
            .OrderBy(r => r.Id)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal("one", rows[0].Name);
    }

    /// <summary>
    /// Wipe every user-created table/view in the current database. The fixture pins this DB
    /// as default for the connection so we can't drop and recreate; per-object DROP is the
    /// only option.
    /// </summary>
    private async Task ResetAsync()
    {
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name NOT LIKE '.%'");
        foreach (var r in rows)
        {
            var name = (string)r["name"]!;
            var engine = (string?)r["engine"];
            var kind = engine is "View" or "MaterializedView" or "LiveView" ? "VIEW" : "TABLE";
            await RawClickHouse.ExecuteAsync(Conn, $"DROP {kind} IF EXISTS \"{name}\" SYNC");
        }
    }

    public sealed class ActiveUserView
    {
        public ulong UserId { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class ActiveUserSrc
    {
        public ulong UserId { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsActive { get; set; }
    }

    public sealed class ScoredView
    {
        public ulong Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public uint Score { get; set; }
    }

    public sealed class BasicRow
    {
        public ulong Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class RawSample
    {
        public ulong Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class EmptyCtx(DbContextOptions<EmptyCtx> o) : DbContext(o)
    {
        // Result types referenced by FromView<T>(name) must be in the model. HasView<T>(name)
        // is the lightest registration that doesn't require a real source entity.
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<ScoredView>(e => e.HasView("scored_view"));
            mb.Entity<BasicRow>(e => e.HasView("basic_view"));
        }
    }

    public sealed class FluentCtx(DbContextOptions<FluentCtx> o) : DbContext(o)
    {
        public DbSet<ActiveUserSrc> UserSources => Set<ActiveUserSrc>();
        public DbSet<ActiveUserView> ActiveUsers => Set<ActiveUserView>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            // Source must be in the model so the view's source column resolution finds the EF
            // column names. Tests create the actual table via raw CREATE TABLE matching this shape.
            mb.Entity<ActiveUserSrc>(e =>
            {
                e.ToTable("users");
                e.HasKey(u => u.UserId);
                e.Property(u => u.UserId).HasColumnName("user_id");
                e.Property(u => u.Name).HasColumnName("name");
                e.Property(u => u.IsActive).HasColumnName("is_active");
            });

            mb.Entity<ActiveUserView>(e =>
            {
                e.AsView<ActiveUserView, ActiveUserSrc>(cfg => cfg
                    .HasName("active_users")
                    .FromTable()
                    .Select(u => new ActiveUserView { UserId = u.UserId, Name = u.Name })
                    .Where(u => u.IsActive)
                    .OrReplace());
            });
        }
    }

    public sealed class MixedCtx(DbContextOptions<MixedCtx> o) : DbContext(o)
    {
        public DbSet<ActiveUserSrc> UserSources => Set<ActiveUserSrc>();
        public DbSet<ActiveUserView> ActiveUsers => Set<ActiveUserView>();
        public DbSet<RawSample> RawSamples => Set<RawSample>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<ActiveUserSrc>(e =>
            {
                e.ToTable("users");
                e.HasKey(u => u.UserId);
                e.Property(u => u.UserId).HasColumnName("user_id");
                e.Property(u => u.Name).HasColumnName("name");
                e.Property(u => u.IsActive).HasColumnName("is_active");
            });

            mb.Entity<ActiveUserView>(e =>
            {
                e.AsView<ActiveUserView, ActiveUserSrc>(cfg => cfg
                    .HasName("active_users")
                    .FromTable()
                    .Select(u => new ActiveUserView { UserId = u.UserId, Name = u.Name })
                    .Where(u => u.IsActive)
                    .OrReplace());
            });

            mb.Entity<RawSample>(e =>
            {
                e.AsViewRaw(
                    viewName: "raw_sample",
                    selectSql: "SELECT id AS \"Id\", name AS \"Name\" FROM source_rows",
                    ifNotExists: true);
                e.AsViewDeferred();
            });
        }
    }

    public sealed class HasViewOnlyCtx(DbContextOptions<HasViewOnlyCtx> o) : DbContext(o)
    {
        public DbSet<BasicRow> BasicViews => Set<BasicRow>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<BasicRow>(e => e.HasView("basic_view"));
        }
    }
}
