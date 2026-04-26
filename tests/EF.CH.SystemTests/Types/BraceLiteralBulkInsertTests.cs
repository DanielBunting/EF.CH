using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Bulk-insert tests for type mappings that historically emitted brace-style
/// literals — the trigger that caused the original Map(K,V) "conflicting type
/// hints" bug, where ClickHouse.Driver's parameter-hint scanner mis-matched
/// <c>{key: value}</c> as a malformed <c>{name:Type}</c> hint. These tests
/// guard against regressions across every adjacent type mapping.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class BraceLiteralBulkInsertTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public BraceLiteralBulkInsertTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task MapStringString_BulkInsertWithColonInValues_DoesNotTripParamScanner()
    {
        await using var ctx = TestContextFactory.Create<MapCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Values containing colons would have triggered the old {k: v} → false-positive
        // parameter type hint match in the driver. With map(k, v) form the brace
        // is gone entirely.
        await ctx.BulkInsertAsync(new[]
        {
            new MapRow { Id = 1, Tags = new() { ["env"] = "prod:us-east", ["region"] = "key:val" } },
            new MapRow { Id = 2, Tags = new() { ["env"] = "dev:eu", ["queue"] = "high:priority" } },
        });

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("prod:us-east", rows[0].Tags["env"]);
        Assert.Equal("high:priority", rows[1].Tags["queue"]);
    }

    [Fact]
    public async Task TupleType_BulkInsert_DoesNotTripParamScanner()
    {
        await using var ctx = TestContextFactory.Create<TupleCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Tuple literals emit (v1, v2) parens — verify they round-trip cleanly
        // for entries whose string values contain a colon.
        await ctx.BulkInsertAsync(new[]
        {
            new TupleRow { Id = 1, Coord = Tuple.Create("name:north", 100) },
            new TupleRow { Id = 2, Coord = Tuple.Create("type:south", 200) },
        });

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Equal("name:north", rows[0].Coord.Item1);
        Assert.Equal(100, rows[0].Coord.Item2);
        Assert.Equal("type:south", rows[1].Coord.Item1);
    }

    [Fact]
    public async Task JsonStringPayload_BulkInsert_PreservesEmbeddedBraces()
    {
        await using var ctx = TestContextFactory.Create<JsonCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // JSON payloads contain {"k":"v"} braces inside the quoted string
        // literal. The driver's parameter scanner sees the raw bytes and can
        // mistake {"key": "value"} for a {name:Type} parameter hint.
        await ctx.BulkInsertAsync(new[]
        {
            new JsonRow { Id = 1, Payload = """{"source": "organic", "device": "mobile"}""" },
            new JsonRow { Id = 2, Payload = """{"source": "paid", "device": "tablet"}""" },
        });

        var rows = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(2, rows.Count);
        Assert.Contains("organic", rows[0].Payload);
        Assert.Contains("paid", rows[1].Payload);
    }

    public sealed class MapRow
    {
        public uint Id { get; set; }
        public Dictionary<string, string> Tags { get; set; } = new();
    }
    public sealed class MapCtx(DbContextOptions<MapCtx> o) : DbContext(o)
    {
        public DbSet<MapRow> Rows => Set<MapRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<MapRow>(e =>
            {
                e.ToTable("MapBraceRows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Tags).HasColumnType("Map(String, String)");
            });
    }

    public sealed class TupleRow
    {
        public uint Id { get; set; }
        // Reference Tuple — the driver returns Tuple<>, and EF Core can't
        // currently convert that to ValueTuple on read (separate latent
        // issue). This test only validates the bulk-insert literal form, so
        // we use Tuple<> for round-trip parity.
        public Tuple<string, int> Coord { get; set; } = Tuple.Create("", 0);
    }
    public sealed class TupleCtx(DbContextOptions<TupleCtx> o) : DbContext(o)
    {
        public DbSet<TupleRow> Rows => Set<TupleRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<TupleRow>(e =>
            {
                e.ToTable("TupleBraceRows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Coord).HasColumnType("Tuple(String, Int32)");
            });
    }

    public sealed class JsonRow
    {
        public uint Id { get; set; }
        public string Payload { get; set; } = "";
    }
    public sealed class JsonCtx(DbContextOptions<JsonCtx> o) : DbContext(o)
    {
        public DbSet<JsonRow> Rows => Set<JsonRow>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<JsonRow>(e =>
            {
                e.ToTable("JsonBraceRows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
