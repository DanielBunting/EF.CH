using EF.CH;
using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// All five existing identifier-generator tests assert DDL emission and
/// annotation state but not <em>generation behaviour</em> at runtime. Silent
/// ID collision is a Tier-4 surface used by every entity that opts in. These
/// tests insert a large batch from multiple parallel writers and verify
/// uniqueness (and, for v7, time ordering) on the generated identifiers.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class IdentifierGeneratorUniquenessTests
{
    private const int InsertsPerTask = 2_500;
    private const int Tasks = 4;
    private const int TotalRows = InsertsPerTask * Tasks; // 10 000

    private readonly SingleNodeClickHouseFixture _fx;
    public IdentifierGeneratorUniquenessTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task SerialId_IsStrictlyMonotonicAcrossBatches()
    {
        await using var ctx = await PrepareContextAsync<SerialCtx>();

        // Three sequential batches; serial IDs must be strictly monotonic
        // both within and across batches because the Keeper-backed counter
        // never goes backwards.
        for (int batch = 0; batch < 3; batch++)
        {
            ctx.Rows.AddRange(Enumerable.Range(0, 100).Select(_ => new SerialRow()));
            await ctx.SaveChangesAsync();
            ctx.ChangeTracker.Clear();
        }

        var ordered = await ctx.Rows.OrderBy(x => x.Id).Select(x => x.Id).ToListAsync();
        Assert.Equal(300, ordered.Count);
        for (int i = 1; i < ordered.Count; i++)
            Assert.True(ordered[i] > ordered[i - 1],
                $"expected strictly monotonic serial IDs; index {i} value {ordered[i]} not > {ordered[i-1]}");
    }

    [Fact]
    public async Task UuidV7_IsTimeOrderedAcrossBatches()
    {
        await using var ctx = await PrepareContextAsync<UuidV7Ctx>();

        // Insert via raw SQL so CH's `generateUUIDv7()` runs server-side. EF
        // Core's built-in GuidValueGenerator otherwise mints client-side
        // (v4) UUIDs before the INSERT lands, ignoring the server default.
        // Two batches separated by a short delay so the v7 timestamp prefix
        // advances; UuidV7Comparer must sort the second batch strictly after
        // the first.
        for (int i = 0; i < 50; i++)
            await ctx.Database.ExecuteSqlRawAsync(
                "INSERT INTO UuidV7Row (Id, Batch) VALUES (generateUUIDv7(), 0)");

        await Task.Delay(100); // advance the v7 ms tick.

        for (int i = 0; i < 50; i++)
            await ctx.Database.ExecuteSqlRawAsync(
                "INSERT INTO UuidV7Row (Id, Batch) VALUES (generateUUIDv7(), 1)");

        var rows = await ctx.Rows.Select(x => new { x.Id, x.Batch }).ToListAsync();
        Assert.Equal(100, rows.Count);

        var sorted = rows.OrderBy(r => r.Id, UuidV7Comparer.Instance).ToList();
        var batches = sorted.Select(r => r.Batch).ToList();

        Assert.Equal(50, batches.Count(b => b == 0));
        Assert.Equal(50, batches.Count(b => b == 1));
        Assert.All(batches.Take(50), b => Assert.Equal(0, b));
        Assert.All(batches.Skip(50), b => Assert.Equal(1, b));
    }

    [Fact]
    public async Task SnowflakeId_NoCollisionsUnderConcurrentInserts()
    {
        await using var ctx = await PrepareContextAsync<SnowflakeCtx>();
        await InsertConcurrentlyAsync(ctx, () => new SnowflakeRow());

        var (count, distinct) = await CountAndDistinctAsync(ctx, "SnowflakeIdRow");
        Assert.Equal((ulong)TotalRows, count);
        Assert.Equal(count, distinct);
    }

    [Fact]
    public async Task UuidV4_NoCollisionsUnderConcurrentInserts()
    {
        await using var ctx = await PrepareContextAsync<UuidV4Ctx>();
        await InsertConcurrentlyAsync(ctx, () => new UuidV4Row());

        var (count, distinct) = await CountAndDistinctAsync(ctx, "UuidV4Row");
        Assert.Equal((ulong)TotalRows, count);
        Assert.Equal(count, distinct);
    }

    [Fact]
    public async Task SerialId_NoCollisionsUnderConcurrentInserts()
    {
        await using var ctx = await PrepareContextAsync<SerialCtx>();
        await InsertConcurrentlyAsync(ctx, () => new SerialRow());

        var (count, distinct) = await CountAndDistinctAsync(ctx, "SerialIdRow");
        Assert.Equal((ulong)TotalRows, count);
        Assert.Equal(count, distinct);
    }

    private async Task InsertConcurrentlyAsync<TCtx, TRow>(TCtx _, Func<TRow> factory)
        where TCtx : DbContext
        where TRow : class
    {
        // Each task uses its own context so EF Core's change tracker isn't
        // shared across threads.
        var tasks = Enumerable.Range(0, Tasks).Select(async _ =>
        {
            await using var c = TestContextFactory.Create<TCtx>(Conn);
            for (int i = 0; i < InsertsPerTask; i++)
                c.Set<TRow>().Add(factory());
            await c.SaveChangesAsync();
        });
        await Task.WhenAll(tasks);
    }

    private async Task<(ulong Count, ulong Distinct)> CountAndDistinctAsync(DbContext ctx, string table)
    {
        var count = await RawClickHouse.RowCountAsync(Conn, table);
        var distinct = await RawClickHouse.ScalarAsync<ulong>(Conn,
            $"SELECT countDistinct(Id) FROM \"{table}\"");
        return (count, distinct);
    }

    private async Task<TCtx> PrepareContextAsync<TCtx>() where TCtx : DbContext
    {
        await using (var dropCtx = TestContextFactory.Create<TCtx>(Conn))
        {
            await dropCtx.Database.EnsureDeletedAsync();
        }
        var ctx = TestContextFactory.Create<TCtx>(Conn);
        await ctx.Database.EnsureCreatedAsync();
        return ctx;
    }

    public sealed class SerialRow { public ulong Id { get; set; } }
    public sealed class UuidV4Row { public Guid Id { get; set; } }
    public sealed class UuidV7Row { public Guid Id { get; set; } public byte Batch { get; set; } }
    public sealed class SnowflakeRow { public long Id { get; set; } }

    public sealed class SerialCtx(DbContextOptions<SerialCtx> o) : DbContext(o)
    {
        public DbSet<SerialRow> Rows => Set<SerialRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<SerialRow>(e =>
            {
                e.ToTable("SerialIdRow"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Id).HasSerialIDDefault("ef_ch_test_serial");
            });
    }

    public sealed class UuidV4Ctx(DbContextOptions<UuidV4Ctx> o) : DbContext(o)
    {
        public DbSet<UuidV4Row> Rows => Set<UuidV4Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<UuidV4Row>(e =>
            {
                e.ToTable("UuidV4Row"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Id).HasUuidV4Default();
            });
    }

    public sealed class UuidV7Ctx(DbContextOptions<UuidV7Ctx> o) : DbContext(o)
    {
        public DbSet<UuidV7Row> Rows => Set<UuidV7Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<UuidV7Row>(e =>
            {
                e.ToTable("UuidV7Row"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Id).HasUuidV7Default();
            });
    }

    public sealed class SnowflakeCtx(DbContextOptions<SnowflakeCtx> o) : DbContext(o)
    {
        public DbSet<SnowflakeRow> Rows => Set<SnowflakeRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<SnowflakeRow>(e =>
            {
                e.ToTable("SnowflakeIdRow"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Id).HasSnowflakeIDDefault();
            });
    }
}
