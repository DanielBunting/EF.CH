using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

/// <summary>
/// Coverage of TTL clauses beyond simple row-expiry. <c>HasTtl</c> takes a raw
/// string, so any TTL form ClickHouse accepts is reachable through the fluent
/// API — but several non-trivial forms (compound clauses, RECOMPRESS, GROUP BY
/// rolling aggregation) have no dedicated test. A regression that quoted the
/// expression incorrectly or stripped clauses would silently produce a table
/// that retains data forever.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ExtendedTtlTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ExtendedTtlTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task TtlWithDeleteWhere_OnlyMatchingRowsExpire()
    {
        await using var ctx = TestContextFactory.Create<DeleteWhereCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "TtlDeleteWhere_Rows");
        Assert.Contains("TTL", engine);
        // DELETE WHERE may render as "WHERE" alone in normalized engine_full output —
        // assert on the predicate we configured (more stable than the keyword spelling).
        Assert.Contains("'archive'", engine);

        var now = DateTime.UtcNow;
        ctx.Rows.AddRange(
            // Old + Tier=archive  → matches DELETE WHERE, will be removed.
            new DeleteWhereRow { Id = 1, CreatedAt = now.AddDays(-400), Tier = "archive" },
            // Old + Tier=keep     → does NOT match DELETE WHERE, retained.
            new DeleteWhereRow { Id = 2, CreatedAt = now.AddDays(-400), Tier = "keep" },
            // Fresh + Tier=archive → not yet expired, retained.
            new DeleteWhereRow { Id = 3, CreatedAt = now,               Tier = "archive" });
        await ctx.SaveChangesAsync();

        await RawClickHouse.ExecuteAsync(Conn, "OPTIMIZE TABLE \"TtlDeleteWhere_Rows\" FINAL");
        await RawClickHouse.WaitForMutationsAsync(Conn, "TtlDeleteWhere_Rows");

        var ids = await RawClickHouse.ColumnAsync<uint>(Conn,
            "SELECT Id FROM \"TtlDeleteWhere_Rows\" ORDER BY Id");
        Assert.Equal(new uint[] { 2, 3 }, ids.ToArray());
    }

    [Fact]
    public async Task TtlWithRecompressCodec_AppearsInEngineDdl()
    {
        // RECOMPRESS doesn't visibly change row counts; the assertion is on the
        // emitted DDL (the SQL generator must preserve the codec literal verbatim).
        await using var ctx = TestContextFactory.Create<RecompressCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "TtlRecompress_Rows");
        Assert.Contains("RECOMPRESS", engine);
        Assert.Contains("ZSTD", engine);
    }

    [Fact]
    public async Task GroupByTtl_RollsUpExpiredRowsIntoBuckets()
    {
        await using var ctx = TestContextFactory.Create<GroupByCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var engine = await RawClickHouse.EngineFullAsync(Conn, "TtlGroupBy_Rows");
        Assert.Contains("TTL", engine);
        Assert.Contains("GROUP BY", engine);

        var now = DateTime.UtcNow;
        // Keyless entities can't be tracked by EF — insert via raw SQL.
        var oldDay = now.AddDays(-400).ToString("yyyy-MM-dd HH:mm:ss");
        var freshDay = now.ToString("yyyy-MM-dd HH:mm:ss");
        await RawClickHouse.ExecuteAsync(Conn,
            $"INSERT INTO TtlGroupBy_Rows (Bucket, CreatedAt, Value) VALUES " +
            $"('A', '{oldDay}', 10), ('A', '{oldDay}', 20), " +
            $"('B', '{oldDay}', 1), ('B', '{freshDay}', 99)");

        await RawClickHouse.ExecuteAsync(Conn, "OPTIMIZE TABLE \"TtlGroupBy_Rows\" FINAL");
        await RawClickHouse.WaitForMutationsAsync(Conn, "TtlGroupBy_Rows");

        // After GROUP BY TTL: the expired rows are collapsed by Bucket+CreatedAt key,
        // SET Value = sum(Value). The fresh "B" row remains untouched.
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT Bucket, Value FROM \"TtlGroupBy_Rows\" ORDER BY Bucket, Value");
        Assert.Contains(rows, r => (string)r["Bucket"]! == "A" && Convert.ToInt64(r["Value"]) == 30);
    }

    public sealed class DeleteWhereRow
    {
        public uint Id { get; set; }
        public DateTime CreatedAt { get; set; }
        public string Tier { get; set; } = "";
    }
    public sealed class DeleteWhereCtx(DbContextOptions<DeleteWhereCtx> o) : DbContext(o)
    {
        public DbSet<DeleteWhereRow> Rows => Set<DeleteWhereRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<DeleteWhereRow>(e =>
            {
                e.ToTable("TtlDeleteWhere_Rows"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.CreatedAt, x.Id });
                e.HasTtl("toDateTime(CreatedAt) + INTERVAL 90 DAY DELETE WHERE Tier = 'archive'");
            });
    }

    public sealed class RecompressRow
    {
        public uint Id { get; set; }
        public DateTime CreatedAt { get; set; }
        public string Body { get; set; } = "";
    }
    public sealed class RecompressCtx(DbContextOptions<RecompressCtx> o) : DbContext(o)
    {
        public DbSet<RecompressRow> Rows => Set<RecompressRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<RecompressRow>(e =>
            {
                e.ToTable("TtlRecompress_Rows"); e.HasKey(x => x.Id);
                e.UseMergeTree(x => new { x.CreatedAt, x.Id });
                e.HasTtl("toDateTime(CreatedAt) + INTERVAL 7 DAY RECOMPRESS CODEC(ZSTD(1))");
            });
    }

    public sealed class GroupByRow
    {
        public string Bucket { get; set; } = "";
        public DateTime CreatedAt { get; set; }
        public long Value { get; set; }
    }
    public sealed class GroupByCtx(DbContextOptions<GroupByCtx> o) : DbContext(o)
    {
        public DbSet<GroupByRow> Rows => Set<GroupByRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<GroupByRow>(e =>
            {
                e.ToTable("TtlGroupBy_Rows");
                e.HasNoKey();
                e.UseMergeTree("Bucket", "CreatedAt");
                e.HasTtl("toDateTime(CreatedAt) + INTERVAL 90 DAY GROUP BY Bucket SET Value = sum(Value)");
            });
    }
}
