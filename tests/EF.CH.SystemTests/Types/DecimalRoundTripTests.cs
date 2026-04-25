using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip tests for ClickHouse Decimal types at various precision/scale boundaries.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class DecimalRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public DecimalRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task Decimal_AllSizes_RoundTrip()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        var row = new Row
        {
            Id = 1,
            D32 = 12345.6789m,                          // Decimal32(4)
            D64 = 9876543210.123456m,                   // Decimal64(6)
            // Decimal128(18) can hold 38-digit mantissas; .NET decimal is limited to ~28-29
            // significant digits, so the test value stays inside that range while still
            // exercising 18-place fractional precision.
            D128 = 1234567890.123456789012345678m,
        };
        ctx.Rows.Add(row);
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal(row.D32, read.D32);
        Assert.Equal(row.D64, read.D64);
        Assert.Equal(row.D128, read.D128);

        // Schema-side: column types reflect the requested precision/scale exactly.
        // ClickHouse normalises Decimal32(S) → Decimal(9, S), Decimal64(S) → Decimal(18, S),
        // Decimal128(S) → Decimal(38, S) in system.columns.type.
        var t32 = await RawClickHouse.ColumnTypeAsync(Conn, "DecimalRoundTrip_Rows", "D32");
        var t64 = await RawClickHouse.ColumnTypeAsync(Conn, "DecimalRoundTrip_Rows", "D64");
        var t128 = await RawClickHouse.ColumnTypeAsync(Conn, "DecimalRoundTrip_Rows", "D128");
        Assert.Equal("Decimal(9, 4)", t32);
        Assert.Equal("Decimal(18, 6)", t64);
        Assert.Equal("Decimal(38, 18)", t128);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public decimal D32 { get; set; }
        public decimal D64 { get; set; }
        public decimal D128 { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("DecimalRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.D32).HasColumnType("Decimal32(4)");
                e.Property(x => x.D64).HasColumnType("Decimal64(6)");
                e.Property(x => x.D128).HasColumnType("Decimal128(18)");
            });
    }
}
