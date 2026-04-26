using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Types;

/// <summary>
/// Round-trip for an <c>AggregateFunction(uniq, String)</c> raw column outside
/// any MV scenario. The state is built via <c>uniqState(...)</c>; we read it back
/// via <c>uniqMerge(...)</c> through a raw query to verify the column shape.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class AggregateFunctionRawColumnTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public AggregateFunctionRawColumnTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task AggregateFunctionUniqState_StoredAndMergedBack()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        // Populate AggregateFunction(uniq, String) state via raw INSERT … SELECT
        // (EF doesn't have a clean LINQ surface for emitting *State directly outside MVs).
        await RawClickHouse.ExecuteAsync(Conn,
            "INSERT INTO \"AggFnRaw_Rows\" (Id, UniqState) " +
            "SELECT number AS Id, uniqState(toString(number % 10)) AS UniqState FROM numbers(100) GROUP BY Id");

        var unique = await RawClickHouse.ScalarAsync<ulong>(Conn,
            "SELECT uniqMerge(UniqState) FROM \"AggFnRaw_Rows\"");
        // toString(number % 10) for numbers 0..99 yields exactly 10 distinct values.
        // uniq is approximate (HLL-based); 8..12 is the canonical tight band.
        Assert.InRange(unique, 8ul, 12ul);

        var t = await RawClickHouse.ColumnTypeAsync(Conn, "AggFnRaw_Rows", "UniqState");
        Assert.Contains("AggregateFunction", t);
    }

    public sealed class Row
    {
        public ulong Id { get; set; }
        public byte[] UniqState { get; set; } = Array.Empty<byte>();
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("AggFnRaw_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
                e.Property(x => x.UniqState).HasColumnType("AggregateFunction(uniq, String)");
            });
    }
}
