using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

[Collection(SingleNodeCollection.Name)]
public class ReplacingMergeTreeDedupTests
{
    private readonly SingleNodeClickHouseFixture _fixture;
    public ReplacingMergeTreeDedupTests(SingleNodeClickHouseFixture fixture) => _fixture = fixture;
    private string Conn => _fixture.ConnectionString;

    [Fact]
    public async Task DuplicateInserts_CollapseToLatestVersion()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();

        ctx.Users.Add(new UserRow { Id = id1, Name = "v1", Version = 1 });
        await ctx.SaveChangesAsync(); ctx.ChangeTracker.Clear();
        ctx.Users.Add(new UserRow { Id = id1, Name = "v2", Version = 2 });
        await ctx.SaveChangesAsync(); ctx.ChangeTracker.Clear();
        ctx.Users.Add(new UserRow { Id = id1, Name = "v3", Version = 3 });
        ctx.Users.Add(new UserRow { Id = id2, Name = "only", Version = 1 });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn, "OPTIMIZE TABLE \"Users\" FINAL");
        await RawClickHouse.WaitForMutationsAsync(Conn, "Users");

        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Name, toInt64(Version) AS Version FROM \"Users\" FINAL ORDER BY Name");
        Assert.Equal(2, rows.Count);
        Assert.Equal("only", (string)rows[0]["Name"]!);
        Assert.Equal("v3", (string)rows[1]["Name"]!);
        Assert.Equal(3L, Convert.ToInt64(rows[1]["Version"]));
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<UserRow> Users => Set<UserRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<UserRow>(e =>
            {
                e.ToTable("Users"); e.HasKey(x => x.Id);
                e.UseReplacingMergeTree("Version", "Id");
            });
    }

    public class UserRow { public Guid Id { get; set; } public string Name { get; set; } = ""; public long Version { get; set; } }
}
