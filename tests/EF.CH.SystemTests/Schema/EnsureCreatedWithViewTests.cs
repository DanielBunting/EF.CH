using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// View-marked entities go through EF Core's standard table differ rather than
/// <c>ViewSqlGenerator</c>'s projection resolution; the existing
/// <c>EnsureCreatedDeploymentTests</c> are all table-only. These tests assert
/// that <c>EnsureCreatedAsync</c> emits a correct view DDL referencing the
/// mapped column names (not CLR property names) and that LINQ over the view
/// returns the projected rows. Mirrors the deployment-tests style but pivots
/// on a view entity registered via <c>AsView&lt;TView, TSource&gt;</c>.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class EnsureCreatedWithViewTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public EnsureCreatedWithViewTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task EnsureCreatedAsync_WithViewEntity_GeneratesCorrectViewDdl()
    {
        await using var ctx = TestContextFactory.Create<ViewCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "users"));
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "active_users"));

        var ddl = await RawClickHouse.EngineFullAsync(Conn, "active_users");
        Assert.NotNull(ddl); // engine_full is "" for views, so fall back to create_table_query.

        // Read the view's actual SELECT body. The view DDL's *SELECT body*
        // must reference the source's mapped column names (snake_case) — a
        // past regression hazard is the projection emitting the CLR property
        // name (e.g. `u.UserId`) directly, which would error at refresh time
        // with "Missing columns: 'UserId'". The view's own column declarations
        // can legitimately carry the CLR names (the entity has no
        // HasColumnName for itself), so we restrict the check to the
        // post-AS-SELECT clause.
        var createSql = await RawClickHouse.ScalarAsync<string>(Conn, $"""
            SELECT create_table_query
            FROM system.tables
            WHERE database = currentDatabase() AND name = 'active_users'
            """);

        var asIndex = createSql.IndexOf(" AS SELECT", StringComparison.OrdinalIgnoreCase);
        Assert.True(asIndex > 0, $"expected '... AS SELECT ...' in view DDL; got: {createSql}");
        var selectBody = createSql.Substring(asIndex);

        Assert.Contains("user_id", selectBody);
        Assert.Contains("name", selectBody);
        Assert.Contains("is_active", selectBody);
    }

    [Fact]
    public async Task EnsureCreatedAsync_WithViewEntity_QueryReturnsRows()
    {
        await using var ctx = TestContextFactory.Create<ViewCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        await ctx.Database.ExecuteSqlRawAsync("""
            INSERT INTO users (user_id, name, is_active) VALUES
            (1, 'Alice', 1), (2, 'Bob', 0), (3, 'Carol', 1), (4, 'Dave', 0)
            """);

        var rows = await ctx.ActiveUsers.OrderBy(v => v.UserId).ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal((ulong)1, rows[0].UserId);
        Assert.Equal("Alice", rows[0].Name);
        Assert.Equal((ulong)3, rows[1].UserId);
        Assert.Equal("Carol", rows[1].Name);
    }

    public sealed class UserSource
    {
        public ulong UserId { get; set; }
        public string Name { get; set; } = "";
        public bool IsActive { get; set; }
    }

    public sealed class ActiveUserView
    {
        public ulong UserId { get; set; }
        public string Name { get; set; } = "";
    }

    public sealed class ViewCtx(DbContextOptions<ViewCtx> o) : DbContext(o)
    {
        public DbSet<UserSource> Users => Set<UserSource>();
        public DbSet<ActiveUserView> ActiveUsers => Set<ActiveUserView>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<UserSource>(e =>
            {
                e.ToTable("users");
                e.HasKey(u => u.UserId);
                e.Property(u => u.UserId).HasColumnName("user_id");
                e.Property(u => u.Name).HasColumnName("name");
                e.Property(u => u.IsActive).HasColumnName("is_active");
            });

            mb.Entity<ActiveUserView>(e =>
            {
                e.AsView<ActiveUserView, UserSource>(cfg => cfg
                    .HasName("active_users")
                    .FromTable()
                    .Select(u => new ActiveUserView { UserId = u.UserId, Name = u.Name })
                    .Where(u => u.IsActive)
                    .OrReplace());
            });
        }
    }
}
