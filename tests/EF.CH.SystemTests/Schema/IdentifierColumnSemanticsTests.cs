using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Round-trip semantics for the four identifier-column flavours:
/// <c>HasMaterializedExpression</c>, <c>HasAliasExpression</c>,
/// <c>HasDefaultExpression</c>, and <c>HasEphemeralExpression</c>. The existing
/// <c>ComputedColumnTests</c> pins <c>default_kind</c> in <c>system.columns</c>
/// for one combined entity; this test focuses on the *runtime* behavioural
/// differences (storage, read-back, write-time) on standalone tables, which
/// surface different bugs (e.g. type-coupling between MATERIALIZED and its
/// EPHEMERAL source column).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class IdentifierColumnSemanticsTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public IdentifierColumnSemanticsTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task MaterializedColumn_StoredOnDisk_AndRecomputedFromExpression()
    {
        await using var ctx = TestContextFactory.Create<MatCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.Equal("MATERIALIZED",
            await RawClickHouse.ColumnDefaultKindAsync(Conn, "IdCol_Mat", "Doubled"));

        ctx.Rows.AddRange(new MatRow { Id = 1, V = 7 }, new MatRow { Id = 2, V = 8 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(14, rows[0].Doubled);
        Assert.Equal(16, rows[1].Doubled);
    }

    [Fact]
    public async Task AliasColumn_NotStored_ComputedOnRead()
    {
        await using var ctx = TestContextFactory.Create<AliasCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.Equal("ALIAS",
            await RawClickHouse.ColumnDefaultKindAsync(Conn, "IdCol_Alias", "Upper"));
        // Integer-typed alias is the regression case for the model validator —
        // it used to fire the identity-column check on Squared because
        // HasAliasExpression sets ValueGeneratedOnAddOrUpdate (which has the
        // OnAdd flag). The validator now exempts AliasExpression annotations.
        Assert.Equal("ALIAS",
            await RawClickHouse.ColumnDefaultKindAsync(Conn, "IdCol_Alias", "Squared"));

        ctx.Rows.AddRange(
            new AliasRow { Id = 1, Name = "alpha", V = 3 },
            new AliasRow { Id = 2, Name = "beta", V = 5 });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal("ALPHA", rows[0].Upper);
        Assert.Equal("BETA", rows[1].Upper);
        Assert.Equal(9, rows[0].Squared);
        Assert.Equal(25, rows[1].Squared);

        // Bytes-on-disk for ALIAS columns are zero — verify by reading column-stats.
        var aliasBytes = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() " +
            "AND table = 'IdCol_Alias' AND name = 'Upper'");
        Assert.Equal(0ul, aliasBytes);
    }

    [Fact]
    public async Task DefaultExpression_ServerComputesValue_WhenClientOmits()
    {
        await using var ctx = TestContextFactory.Create<DefaultCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.Equal("DEFAULT",
            await RawClickHouse.ColumnDefaultKindAsync(Conn, "IdCol_Default", "Stamp"));

        // EF will INSERT explicit values for every property; to verify the server-side
        // default actually fires we need to bypass EF and let the server compute it.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO IdCol_Default (Id, V) VALUES (1, 100), (2, 200)");

        var stamps = await RawClickHouse.ColumnAsync<int>(Conn,
            "SELECT Stamp FROM IdCol_Default ORDER BY Id");
        // "V * 10" default applies — the inserted rows had Stamp omitted.
        Assert.Equal(new[] { 1000, 2000 }, stamps);
    }

    [Fact]
    public async Task EphemeralColumn_FeedsMaterialized_AndIsAbsentFromSelectStar()
    {
        await using var ctx = TestContextFactory.Create<EphemeralCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        Assert.Equal("EPHEMERAL",
            await RawClickHouse.ColumnDefaultKindAsync(Conn, "IdCol_Ephemeral", "RawKey"));
        Assert.Equal("MATERIALIZED",
            await RawClickHouse.ColumnDefaultKindAsync(Conn, "IdCol_Ephemeral", "HashedKey"));

        // Insert via raw SQL so we can supply RawKey directly. EF emits an INSERT that omits
        // ALIAS/MATERIALIZED columns but includes EPHEMERAL — both pathways are valid here.
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO IdCol_Ephemeral (Id, RawKey, V) VALUES (1, 'alpha', 1), (2, 'beta', 2)");

        // SELECT * does not include ephemeral columns — `system.columns` knows about RawKey
        // but the column projection in a wildcard read leaves it out.
        var wildcardRows = await RawClickHouse.RowsAsync(Conn,
            "SELECT * FROM IdCol_Ephemeral ORDER BY Id");
        Assert.False(wildcardRows[0].ContainsKey("RawKey"),
            "EPHEMERAL column 'RawKey' should not appear in SELECT *");

        // The materialized HashedKey was computed from the supplied RawKey and persisted.
        var hashes = await RawClickHouse.ColumnAsync<uint>(Conn,
            "SELECT HashedKey FROM IdCol_Ephemeral ORDER BY Id");
        Assert.NotEqual(0u, hashes[0]);
        Assert.NotEqual(hashes[0], hashes[1]);
    }

    public sealed class MatRow
    {
        public uint Id { get; set; }
        public int V { get; set; }
        public int Doubled { get; set; }
    }
    public sealed class MatCtx(DbContextOptions<MatCtx> o) : DbContext(o)
    {
        public DbSet<MatRow> Rows => Set<MatRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<MatRow>(e =>
            {
                e.ToTable("IdCol_Mat"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Doubled).HasMaterializedExpression("V * 2");
            });
    }

    public sealed class AliasRow
    {
        public uint Id { get; set; }
        public string Name { get; set; } = "";
        public int V { get; set; }
        public string Upper { get; set; } = "";
        public int Squared { get; set; }
    }
    public sealed class AliasCtx(DbContextOptions<AliasCtx> o) : DbContext(o)
    {
        public DbSet<AliasRow> Rows => Set<AliasRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<AliasRow>(e =>
            {
                e.ToTable("IdCol_Alias"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Upper).HasAliasExpression("upperUTF8(Name)");
                e.Property(x => x.Squared).HasAliasExpression("V * V");
            });
    }

    public sealed class DefaultRow
    {
        public uint Id { get; set; }
        public int V { get; set; }
        public int Stamp { get; set; }
    }
    public sealed class DefaultCtx(DbContextOptions<DefaultCtx> o) : DbContext(o)
    {
        public DbSet<DefaultRow> Rows => Set<DefaultRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<DefaultRow>(e =>
            {
                e.ToTable("IdCol_Default"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Stamp).HasDefaultExpression("V * 10");
            });
    }

    public sealed class EphemeralRow
    {
        public uint Id { get; set; }
        public string RawKey { get; set; } = "";
        public uint HashedKey { get; set; }
        public int V { get; set; }
    }
    public sealed class EphemeralCtx(DbContextOptions<EphemeralCtx> o) : DbContext(o)
    {
        public DbSet<EphemeralRow> Rows => Set<EphemeralRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<EphemeralRow>(e =>
            {
                e.ToTable("IdCol_Ephemeral"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.RawKey).HasEphemeralExpression("''");
                e.Property(x => x.HashedKey).HasMaterializedExpression("toUInt32(sipHash64(RawKey))");
            });
    }
}
