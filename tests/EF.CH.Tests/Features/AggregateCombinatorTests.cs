using EF.CH.Extensions;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Features;

/// <summary>
/// Tests for ClickHouse aggregate combinator functions (-State, -Merge, -If, Array*).
/// </summary>
public class AggregateCombinatorTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private string GetConnectionString() => _container.GetConnectionString();

    #region Type Mapping Tests

    [Fact]
    public void AggregateFunctionTypeMapping_GeneratesCorrectStoreType_Sum()
    {
        var underlyingMapping = new ClickHouseInt64TypeMapping();
        var mapping = new ClickHouseAggregateFunctionTypeMapping("sum", underlyingMapping);

        Assert.Equal("AggregateFunction(sum, Int64)", mapping.StoreType);
        Assert.Equal(typeof(byte[]), mapping.ClrType);
        Assert.Equal("sum", mapping.FunctionName);
    }

    [Fact]
    public void AggregateFunctionTypeMapping_GeneratesCorrectStoreType_Avg()
    {
        var underlyingMapping = new ClickHouseFloat64TypeMapping();
        var mapping = new ClickHouseAggregateFunctionTypeMapping("avg", underlyingMapping);

        Assert.Equal("AggregateFunction(avg, Float64)", mapping.StoreType);
        Assert.Equal(typeof(byte[]), mapping.ClrType);
    }

    [Fact]
    public void AggregateFunctionTypeMapping_GeneratesCorrectStoreType_Uniq()
    {
        var underlyingMapping = new ClickHouseStringTypeMapping();
        var mapping = new ClickHouseAggregateFunctionTypeMapping("uniq", underlyingMapping);

        Assert.Equal("AggregateFunction(uniq, String)", mapping.StoreType);
        Assert.Equal(typeof(byte[]), mapping.ClrType);
    }

    [Fact]
    public void AggregateFunctionTypeMapping_ThrowsOnLiteral()
    {
        var underlyingMapping = new ClickHouseInt64TypeMapping();
        var mapping = new ClickHouseAggregateFunctionTypeMapping("sum", underlyingMapping);

        Assert.Throws<InvalidOperationException>(() => mapping.GenerateSqlLiteral(new byte[] { 1, 2, 3 }));
    }

    #endregion

    #region Fluent API Tests

    [Fact]
    public void FluentApi_HasAggregateFunction_SetsCorrectColumnType()
    {
        var options = new DbContextOptionsBuilder<AggregateTestContext>()
            .UseClickHouse("Host=localhost")
            .Options;

        using var context = new AggregateTestContext(options);

        var property = context.Model.FindEntityType(typeof(HourlyStats))!
            .FindProperty(nameof(HourlyStats.SumAmountState))!;

        Assert.Equal("AggregateFunction(sum, Int64)", property.GetColumnType());
    }

    [Fact]
    public void FluentApi_HasAggregateFunction_RejectsNonByteArray()
    {
        var options = new DbContextOptionsBuilder<InvalidAggregateContext>()
            .UseClickHouse("Host=localhost")
            .Options;

        Assert.Throws<InvalidOperationException>(() =>
        {
            using var context = new InvalidAggregateContext(options);
            _ = context.Model;
        });
    }

    #endregion

    #region State Combinator Marker Method Tests

    [Fact]
    public void CountState_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.CountState());
    }

    [Fact]
    public void SumState_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.SumState(x => x));
    }

    [Fact]
    public void AvgState_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.AvgState(x => x));
    }

    [Fact]
    public void UniqState_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { "a", "b", "c" });
        Assert.Throws<InvalidOperationException>(() => g.UniqState(x => x));
    }

    [Fact]
    public void QuantileState_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.QuantileState(0.95, x => x));
    }

    #endregion

    #region Merge Combinator Marker Method Tests

    [Fact]
    public void CountMerge_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { new StateHolder { State = [1, 2, 3] } });
        Assert.Throws<InvalidOperationException>(() => g.CountMerge(x => x.State));
    }

    [Fact]
    public void SumMerge_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { new StateHolder { State = [1, 2, 3] } });
        Assert.Throws<InvalidOperationException>(() => g.SumMerge<string, StateHolder, long>(x => x.State));
    }

    private class StateHolder
    {
        public byte[] State { get; set; } = [];
    }

    [Fact]
    public void AvgMerge_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { new StateHolder { State = [1, 2, 3] } });
        Assert.Throws<InvalidOperationException>(() => g.AvgMerge(x => x.State));
    }

    #endregion

    #region If Combinator Marker Method Tests

    [Fact]
    public void CountIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.CountIf(x => x > 1));
    }

    [Fact]
    public void SumIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.SumIf(x => x, x => x > 1));
    }

    [Fact]
    public void AvgIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.AvgIf(x => x, x => x > 1));
    }

    [Fact]
    public void UniqIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.UniqIf(x => x, x => x > 1));
    }

    [Fact]
    public void MinIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.MinIf(x => x, x => x > 1));
    }

    [Fact]
    public void MaxIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.MaxIf(x => x, x => x > 1));
    }

    [Fact]
    public void UniqExactIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.UniqExactIf(x => x, x => x > 1));
    }

    [Fact]
    public void AnyIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.AnyIf(x => x, x => x > 1));
    }

    [Fact]
    public void AnyLastIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.AnyLastIf(x => x, x => x > 1));
    }

    [Fact]
    public void QuantileIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.QuantileIf(0.95, x => x, x => x > 1));
    }

    [Fact]
    public void ArgMaxIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { new { A = 1, B = 2 } });
        Assert.Throws<InvalidOperationException>(() => g.ArgMaxIf(x => x.A, x => x.B, x => x.B > 0));
    }

    [Fact]
    public void ArgMinIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { new { A = 1, B = 2 } });
        Assert.Throws<InvalidOperationException>(() => g.ArgMinIf(x => x.A, x => x.B, x => x.B > 0));
    }

    [Fact]
    public void TopKIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.TopKIf(2, x => x, x => x > 1));
    }

    [Fact]
    public void TopKWeightedIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.TopKWeightedIf(2, x => x, x => x, x => x > 1));
    }

    [Fact]
    public void GroupArrayIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.GroupArrayIf(x => x, x => x > 1));
    }

    [Fact]
    public void GroupArrayIfWithLimit_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.GroupArrayIf(10, x => x, x => x > 1));
    }

    [Fact]
    public void GroupUniqArrayIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.GroupUniqArrayIf(x => x, x => x > 1));
    }

    [Fact]
    public void GroupUniqArrayIfWithLimit_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.GroupUniqArrayIf(10, x => x, x => x > 1));
    }

    [Fact]
    public void MedianIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.MedianIf(x => x, x => x > 1));
    }

    [Fact]
    public void StddevPopIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.StddevPopIf(x => x, x => x > 1));
    }

    [Fact]
    public void StddevSampIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.StddevSampIf(x => x, x => x > 1));
    }

    [Fact]
    public void VarPopIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.VarPopIf(x => x, x => x > 1));
    }

    [Fact]
    public void VarSampIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.VarSampIf(x => x, x => x > 1));
    }

    [Fact]
    public void UniqCombinedIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.UniqCombinedIf(x => x, x => x > 1));
    }

    [Fact]
    public void UniqCombined64If_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.UniqCombined64If(x => x, x => x > 1));
    }

    [Fact]
    public void UniqHLL12If_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.UniqHLL12If(x => x, x => x > 1));
    }

    [Fact]
    public void UniqThetaIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.UniqThetaIf(x => x, x => x > 1));
    }

    [Fact]
    public void QuantileTDigestIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.QuantileTDigestIf(0.95, x => x, x => x > 1));
    }

    [Fact]
    public void QuantileExactIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.QuantileExactIf(0.95, x => x, x => x > 1));
    }

    [Fact]
    public void QuantileTimingIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.QuantileTimingIf(0.95, x => x, x => x > 1));
    }

    [Fact]
    public void QuantileDDIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.QuantileDDIf(0.01, 0.95, x => x, x => x > 1));
    }

    [Fact]
    public void QuantilesIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.QuantilesIf(new[] { 0.5, 0.9 }, x => x, x => x > 1));
    }

    [Fact]
    public void QuantilesTDigestIf_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1.0, 2.0, 3.0 });
        Assert.Throws<InvalidOperationException>(() => g.QuantilesTDigestIf(new[] { 0.5, 0.9 }, x => x, x => x > 1));
    }

    [Fact]
    public void CountUInt64_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.CountUInt64());
    }

    [Fact]
    public void SumUInt64_ThrowsWhenInvokedDirectly()
    {
        var g = MakeGrouping("k", new[] { 1, 2, 3 });
        Assert.Throws<InvalidOperationException>(() => g.SumUInt64(x => x));
    }

    #endregion

    private static IGrouping<TKey, TElement> MakeGrouping<TKey, TElement>(TKey key, IEnumerable<TElement> elements)
        => new TestGrouping<TKey, TElement>(key, elements);

    private sealed class TestGrouping<TKey, TElement>(TKey key, IEnumerable<TElement> elements)
        : IGrouping<TKey, TElement>
    {
        public TKey Key { get; } = key;
        public IEnumerator<TElement> GetEnumerator() => elements.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    #region Array Combinator Marker Method Tests

    [Fact]
    public void ArraySum_ThrowsWhenInvokedDirectly()
    {
        var array = new[] { 1, 2, 3 };
        Assert.Throws<InvalidOperationException>(() => array.ArraySum());
    }

    [Fact]
    public void ArrayAvg_ThrowsWhenInvokedDirectly()
    {
        var array = new[] { 1.0, 2.0, 3.0 };
        Assert.Throws<InvalidOperationException>(() => array.ArrayAvg());
    }

    [Fact]
    public void ArrayMin_ThrowsWhenInvokedDirectly()
    {
        var array = new[] { 1, 2, 3 };
        Assert.Throws<InvalidOperationException>(() => array.ArrayMin());
    }

    [Fact]
    public void ArrayMax_ThrowsWhenInvokedDirectly()
    {
        var array = new[] { 1, 2, 3 };
        Assert.Throws<InvalidOperationException>(() => array.ArrayMax());
    }

    [Fact]
    public void ArrayCount_ThrowsWhenInvokedDirectly()
    {
        var array = new[] { 1, 2, 3 };
        Assert.Throws<InvalidOperationException>(() => array.ArrayCount());
    }

    #endregion

    #region Integration Tests - AggregateFunction Column Creation

    [Fact]
    public async Task CanCreateTableWithAggregateFunctionColumn()
    {
        await using var context = CreateContext();

        // Create table with AggregateFunction columns via raw SQL
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "TestAggregateStats" (
                "Hour" DateTime64(3),
                "CountState" AggregateFunction(count, UInt64),
                "SumAmountState" AggregateFunction(sum, Int64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY "Hour"
            """);

        // Verify table was created by checking system tables
        var count = await context.Database.SqlQueryRaw<ulong>(
            """SELECT count() AS "Value" FROM system.tables WHERE name = 'TestAggregateStats'"""
        ).FirstOrDefaultAsync();

        Assert.Equal(1UL, count);
    }

    [Fact]
    public async Task CanInsertAndMergeAggregateFunctionStates()
    {
        await using var context = CreateContext();

        // Create source table
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "RawEvents" (
                "Timestamp" DateTime64(3),
                "Amount" Int64
            )
            ENGINE = MergeTree()
            ORDER BY "Timestamp"
            """);

        // Create aggregated table with AggregateFunction columns
        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "HourlyAggregates" (
                "Hour" DateTime64(3),
                "CountState" AggregateFunction(count, UInt64),
                "SumAmountState" AggregateFunction(sum, Int64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY "Hour"
            """);

        // Insert using State functions
        await context.Database.ExecuteSqlRawAsync("""
            INSERT INTO "HourlyAggregates"
            SELECT
                toStartOfHour("Timestamp") AS "Hour",
                countState() AS "CountState",
                sumState("Amount") AS "SumAmountState"
            FROM (
                SELECT toDateTime64('2024-01-01 10:00:00', 3) AS "Timestamp", toInt64(100) AS "Amount"
                UNION ALL
                SELECT toDateTime64('2024-01-01 10:30:00', 3) AS "Timestamp", toInt64(200) AS "Amount"
            )
            GROUP BY "Hour"
            """);

        // Query using Merge functions
        var result = await context.Database.SqlQueryRaw<long>("""
            SELECT sumMerge("SumAmountState") AS "Value" FROM "HourlyAggregates"
            """).FirstOrDefaultAsync();

        Assert.Equal(300L, result);
    }

    #endregion

    private AggregateCombinatorTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<AggregateCombinatorTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new AggregateCombinatorTestContext(options);
    }
}

#region Test Entities and Contexts

public class HourlyStats
{
    public DateTime Hour { get; set; }
    public byte[] CountState { get; set; } = [];
    public byte[] SumAmountState { get; set; } = [];
    public byte[] AvgResponseTimeState { get; set; } = [];
}

public class AggregateTestContext : DbContext
{
    public AggregateTestContext(DbContextOptions<AggregateTestContext> options)
        : base(options) { }

    public DbSet<HourlyStats> HourlyStats { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<HourlyStats>(entity =>
        {
            entity.HasNoKey();
            entity.UseAggregatingMergeTree(x => x.Hour);

            entity.Property(e => e.CountState)
                .HasAggregateFunction("count", typeof(ulong));
            entity.Property(e => e.SumAmountState)
                .HasAggregateFunction("sum", typeof(long));
            entity.Property(e => e.AvgResponseTimeState)
                .HasAggregateFunction("avg", typeof(double));
        });
    }
}

public class InvalidEntity
{
    public DateTime Date { get; set; }
    public long NotByteArray { get; set; }
}

public class InvalidAggregateContext : DbContext
{
    public InvalidAggregateContext(DbContextOptions<InvalidAggregateContext> options)
        : base(options) { }

    public DbSet<InvalidEntity> Invalid { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InvalidEntity>(entity =>
        {
            entity.HasNoKey();
            // This should throw because NotByteArray is not byte[]
            entity.Property(e => e.NotByteArray)
                .HasAggregateFunction("sum", typeof(long));
        });
    }
}

public class AggregateCombinatorTestContext : DbContext
{
    public AggregateCombinatorTestContext(DbContextOptions<AggregateCombinatorTestContext> options)
        : base(options) { }
}

#endregion
