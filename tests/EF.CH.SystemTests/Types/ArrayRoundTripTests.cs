using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for <c>Array(T)</c> across primitives, nullable elements, and
/// arrays-of-arrays.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class ArrayRoundTripTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ArrayRoundTripTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task ArrayInt32_RoundTrips()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, Numbers = new[] { 1, 2, 3 }, Words = new[] { "a", "b" }, Matrix = new[] { new[] { 1, 2 }, new[] { 3 } } });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();

        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal(new[] { 1, 2, 3 }, read.Numbers);
        Assert.Equal(new[] { "a", "b" }, read.Words);
        Assert.Equal(2, read.Matrix.Length);
        Assert.Equal(new[] { 1, 2 }, read.Matrix[0]);
        Assert.Equal(new[] { 3 }, read.Matrix[1]);

        var t = await RawClickHouse.ColumnTypeAsync(Conn, "ArrayRoundTrip_Rows", "Matrix");
        Assert.Equal("Array(Array(Int32))", t);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int[] Numbers { get; set; } = Array.Empty<int>();
        public string[] Words { get; set; } = Array.Empty<string>();
        public int[][] Matrix { get; set; } = Array.Empty<int[]>();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("ArrayRoundTrip_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.Matrix).HasColumnType("Array(Array(Int32))");
            });
    }
}
