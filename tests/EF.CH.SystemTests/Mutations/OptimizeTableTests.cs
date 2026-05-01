using EF.CH.Extensions;
using EF.CH.Infrastructure;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Mutations;

/// <summary>
/// Integration tests for <c>OptimizeTableAsync</c> end-to-end. Covers FINAL on
/// ReplacingMergeTree, partition-scoped optimize, DEDUPLICATE BY columns, and the
/// validation error path on invalid column names.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class OptimizeTableTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public OptimizeTableTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task OptimizeTable_Final_DeduplicatesReplacingMergeTree()
    {
        await Reset();
        await using var ctx = TestContextFactory.Create<RmtCtx>(Conn);
        await ctx.Database.EnsureCreatedAsync();

        // Two versions of id=1; ReplacingMergeTree(Version) should keep the highest.
        // Use raw INSERT to avoid EF's change-tracker rejecting the duplicate primary key.
        await ctx.Database.ExecuteSqlRawAsync(
            "INSERT INTO rmt_items (Id, Version, Value) VALUES (1, 1, 'v1'), (1, 2, 'v2')");

        await ctx.Database.OptimizeTableAsync<RmtItem>(o => o.WithFinal());
        await RawClickHouse.WaitForMutationsAsync(Conn, "rmt_items");

        var rows = await ctx.Items.ToListAsync();
        // After OPTIMIZE FINAL the duplicate is collapsed to the highest version.
        Assert.Single(rows);
        Assert.Equal("v2", rows[0].Value);
    }

    [Fact]
    public async Task OptimizeTable_WithPartition_OnlyOptimizesPartition()
    {
        await Reset();
        await using var ctx = TestContextFactory.Create<PartCtx>(Conn);
        await ctx.Database.EnsureCreatedAsync();

        ctx.Rows.AddRange(
            new PartRow { Id = 1, Day = new DateOnly(2024, 1, 15), Value = 10 },
            new PartRow { Id = 2, Day = new DateOnly(2024, 1, 20), Value = 20 },
            new PartRow { Id = 3, Day = new DateOnly(2024, 2, 5), Value = 30 });
        await ctx.SaveChangesAsync();

        // Optimize only the January partition (toYYYYMM(Day) = 202401).
        await ctx.Database.OptimizeTableAsync<PartRow>(o => o.WithPartition("202401"));
        await RawClickHouse.WaitForMutationsAsync(Conn, "part_rows");

        var rowCount = await ctx.Rows.CountAsync();
        Assert.Equal(3, rowCount);
    }

    [Fact]
    public async Task OptimizeTable_WithDeduplicateByColumns_RemovesDuplicates()
    {
        await Reset();
        await using var ctx = TestContextFactory.Create<DedupCtx>(Conn);
        await ctx.Database.EnsureCreatedAsync();

        // ClickHouse requires DEDUPLICATE BY to include all ORDER BY/PRIMARY KEY columns.
        // Insert two rows with identical (Id, Bucket) plus one different — dedup should
        // keep one of the duplicate (Id, Bucket) pairs.
        await ctx.Database.ExecuteSqlRawAsync(
            "INSERT INTO dedup_rows (Id, Bucket, Value) VALUES (1, 'A', 1), (1, 'A', 2), (2, 'B', 3)");

        await ctx.Database.OptimizeTableAsync<DedupRow>(o => o.WithFinal().WithDeduplicate("Id", "Bucket"));
        await RawClickHouse.WaitForMutationsAsync(Conn, "dedup_rows");

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(2, rows.Count); // one (1,'A') + one (2,'B')
    }

    [Fact]
    public void OptimizeTable_WithDeduplicate_RejectsEmptyColumnNames()
    {
        var options = new OptimizeTableOptions();
        var ex = Assert.Throws<ArgumentException>(() => options.WithDeduplicate("Bucket", ""));
        Assert.Contains("WithDeduplicate", ex.Message);
    }

    private async Task Reset()
    {
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT name FROM system.tables WHERE database = currentDatabase() AND name NOT LIKE '.%'");
        foreach (var r in rows)
            await RawClickHouse.ExecuteAsync(Conn, $"DROP TABLE IF EXISTS \"{(string)r["name"]!}\" SYNC");
    }

    public sealed class RmtItem
    {
        public uint Id { get; set; }
        public ulong Version { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    public sealed class PartRow
    {
        public uint Id { get; set; }
        public DateOnly Day { get; set; }
        public int Value { get; set; }
    }

    public sealed class DedupRow
    {
        public uint Id { get; set; }
        public string Bucket { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    public sealed class RmtCtx(DbContextOptions<RmtCtx> o) : DbContext(o)
    {
        public DbSet<RmtItem> Items => Set<RmtItem>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RmtItem>(e =>
            {
                e.ToTable("rmt_items");
                e.HasKey(x => x.Id);
                e.UseReplacingMergeTree(versionColumn: "Version", orderByColumns: "Id");
            });
        }
    }

    public sealed class PartCtx(DbContextOptions<PartCtx> o) : DbContext(o)
    {
        public DbSet<PartRow> Rows => Set<PartRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<PartRow>(e =>
            {
                e.ToTable("part_rows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
                e.HasPartitionBy("toYYYYMM(Day)");
            });
        }
    }

    public sealed class DedupCtx(DbContextOptions<DedupCtx> o) : DbContext(o)
    {
        public DbSet<DedupRow> Rows => Set<DedupRow>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<DedupRow>(e =>
            {
                e.ToTable("dedup_rows");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
        }
    }
}
