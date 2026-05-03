using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.InsertSelect;

/// <summary>
/// Coverage of the no-mapping <c>InsertIntoAsync&lt;TEntity&gt;(targetDbSet)</c>
/// overload — <c>InsertSelectExecutorTests</c> only exercises the cross-type
/// mapping form. The no-mapping path goes through a different executor
/// signature (<see cref="InsertSelect.IClickHouseInsertSelectExecutor.ExecuteAsync{TTarget}"/>)
/// that builds the column list directly off the entity. The two CLR types
/// here are *structurally identical* but registered as distinct entities so
/// the no-mapping path is reachable (EF disallows two table mappings for one
/// CLR type, and shared-type entities aren't supported by the bulk path).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class InsertSelectSameTypeNoMappingTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public InsertSelectSameTypeNoMappingTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task InsertSelect_NoMapping_CopiesAllColumnsToTarget()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        ctx.Snapshots.AddRange(Enumerable.Range(1, 8).Select(i => new Snapshot
        {
            Id = (uint)i,
            Bucket = i % 3 == 0 ? "primary" : "secondary",
            Score = i * 1.5,
        }));
        await ctx.SaveChangesAsync();

        // No mapping form, but cross-type — the executor takes the entity types from
        // the source/target DbSets and emits a column list that matches both schemas.
        var result = await ctx.Snapshots
            .Where(s => s.Bucket == "secondary")
            .InsertIntoAsync(ctx.Copies, s => new SnapshotCopy
            {
                Id = s.Id,
                Bucket = s.Bucket,
                Score = s.Score,
            });

        Assert.Contains("INSERT INTO", result.Sql);

        var copied = await ctx.Copies.OrderBy(c => c.Id).ToListAsync();
        // 8 rows total, i ∈ {3, 6} are "primary"; remaining {1, 2, 4, 5, 7, 8} are "secondary".
        Assert.Equal(6, copied.Count);
        Assert.All(copied, c => Assert.Equal("secondary", c.Bucket));
    }

    public sealed class Snapshot
    {
        public uint Id { get; set; }
        public string Bucket { get; set; } = "";
        public double Score { get; set; }
    }
    public sealed class SnapshotCopy
    {
        public uint Id { get; set; }
        public string Bucket { get; set; } = "";
        public double Score { get; set; }
    }
    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Snapshot> Snapshots => Set<Snapshot>();
        public DbSet<SnapshotCopy> Copies => Set<SnapshotCopy>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Snapshot>(e =>
            {
                e.ToTable("Snapshots");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
            mb.Entity<SnapshotCopy>(e =>
            {
                e.ToTable("SnapshotCopies");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
        }
    }
}
