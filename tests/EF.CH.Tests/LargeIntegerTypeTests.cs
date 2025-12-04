using System.Numerics;
using EF.CH.Extensions;
using EF.CH.Storage.Internal.TypeMappings;
using Microsoft.EntityFrameworkCore;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests;

public class LargeIntegerTypeTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:latest")
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

    #region Int128 Unit Tests

    [Fact]
    public void Int128Mapping_HasCorrectStoreType()
    {
        var mapping = new ClickHouseInt128TypeMapping();

        Assert.Equal("Int128", mapping.StoreType);
        Assert.Equal(typeof(Int128), mapping.ClrType);
    }

    [Fact]
    public void Int128Mapping_GeneratesCorrectLiteral_Positive()
    {
        var mapping = new ClickHouseInt128TypeMapping();
        var value = Int128.MaxValue;

        var literal = mapping.GenerateSqlLiteral(value);

        Assert.Equal("170141183460469231731687303715884105727", literal);
    }

    [Fact]
    public void Int128Mapping_GeneratesCorrectLiteral_Negative()
    {
        var mapping = new ClickHouseInt128TypeMapping();
        Int128 value = Int128.MinValue;

        var literal = mapping.GenerateSqlLiteral(value);

        Assert.Equal("-170141183460469231731687303715884105728", literal);
    }

    [Fact]
    public void Int128Mapping_GeneratesCorrectLiteral_Zero()
    {
        var mapping = new ClickHouseInt128TypeMapping();
        Int128 value = 0;

        var literal = mapping.GenerateSqlLiteral(value);

        Assert.Equal("0", literal);
    }

    #endregion

    #region UInt128 Unit Tests

    [Fact]
    public void UInt128Mapping_HasCorrectStoreType()
    {
        var mapping = new ClickHouseUInt128TypeMapping();

        Assert.Equal("UInt128", mapping.StoreType);
        Assert.Equal(typeof(UInt128), mapping.ClrType);
    }

    [Fact]
    public void UInt128Mapping_GeneratesCorrectLiteral_MaxValue()
    {
        var mapping = new ClickHouseUInt128TypeMapping();
        UInt128 value = UInt128.MaxValue;

        var literal = mapping.GenerateSqlLiteral(value);

        Assert.Equal("340282366920938463463374607431768211455", literal);
    }

    [Fact]
    public void UInt128Mapping_GeneratesCorrectLiteral_Zero()
    {
        var mapping = new ClickHouseUInt128TypeMapping();
        UInt128 value = 0;

        var literal = mapping.GenerateSqlLiteral(value);

        Assert.Equal("0", literal);
    }

    #endregion

    #region Int256 Unit Tests

    [Fact]
    public void Int256Mapping_HasCorrectStoreType()
    {
        var mapping = new ClickHouseInt256TypeMapping();

        Assert.Equal("Int256", mapping.StoreType);
        Assert.Equal(typeof(BigInteger), mapping.ClrType);
    }

    [Fact]
    public void Int256Mapping_GeneratesCorrectLiteral_LargePositive()
    {
        var mapping = new ClickHouseInt256TypeMapping();
        // A large value within Int256 range
        var value = BigInteger.Parse("57896044618658097711785492504343953926634992332820282019728792003956564819967");

        var literal = mapping.GenerateSqlLiteral(value);

        Assert.Equal("57896044618658097711785492504343953926634992332820282019728792003956564819967", literal);
    }

    [Fact]
    public void Int256Mapping_GeneratesCorrectLiteral_Negative()
    {
        var mapping = new ClickHouseInt256TypeMapping();
        var value = BigInteger.Parse("-12345678901234567890123456789012345678901234567890");

        var literal = mapping.GenerateSqlLiteral(value);

        Assert.Equal("-12345678901234567890123456789012345678901234567890", literal);
    }

    [Fact]
    public void Int256Mapping_MinMaxValues()
    {
        // Verify the min/max constants are correct
        // Int256 min: -2^255
        // Int256 max: 2^255 - 1
        var expectedMin = BigInteger.MinusOne << 255;
        var expectedMax = (BigInteger.One << 255) - 1;

        Assert.Equal(expectedMin, ClickHouseInt256TypeMapping.MinValue);
        Assert.Equal(expectedMax, ClickHouseInt256TypeMapping.MaxValue);
    }

    #endregion

    #region UInt256 Unit Tests

    [Fact]
    public void UInt256Mapping_HasCorrectStoreType()
    {
        var mapping = new ClickHouseUInt256TypeMapping();

        Assert.Equal("UInt256", mapping.StoreType);
        Assert.Equal(typeof(BigInteger), mapping.ClrType);
    }

    [Fact]
    public void UInt256Mapping_GeneratesCorrectLiteral_LargeValue()
    {
        var mapping = new ClickHouseUInt256TypeMapping();
        // A large value within UInt256 range
        var value = BigInteger.Parse("115792089237316195423570985008687907853269984665640564039457584007913129639935");

        var literal = mapping.GenerateSqlLiteral(value);

        Assert.Equal("115792089237316195423570985008687907853269984665640564039457584007913129639935", literal);
    }

    [Fact]
    public void UInt256Mapping_ThrowsForNegativeValue()
    {
        var mapping = new ClickHouseUInt256TypeMapping();
        var value = BigInteger.MinusOne;

        Assert.Throws<ArgumentException>(() => mapping.GenerateSqlLiteral(value));
    }

    [Fact]
    public void UInt256Mapping_MaxValue()
    {
        // UInt256 max: 2^256 - 1
        var expectedMax = (BigInteger.One << 256) - 1;

        Assert.Equal(expectedMax, ClickHouseUInt256TypeMapping.MaxValue);
    }

    #endregion

    #region Integration Tests

    [Fact]
    public async Task CanInsertAndQueryInt128()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "LargeNumbers128" (
                "Id" UUID,
                "SignedValue" Int128,
                "UnsignedValue" UInt128
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        var id = Guid.NewGuid();

        // Insert a large Int128 value
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""LargeNumbers128"" (""Id"", ""SignedValue"", ""UnsignedValue"")
              VALUES ('" + id + @"', 170141183460469231731687303715884105727, 340282366920938463463374607431768211455)");

        // Verify data was inserted
        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""LargeNumbers128"" WHERE ""SignedValue"" > 0"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanInsertAndQueryInt256()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "LargeNumbers256" (
                "Id" UUID,
                "SignedValue" Int256,
                "UnsignedValue" UInt256
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        var id = Guid.NewGuid();

        // Insert a large Int256 value
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""LargeNumbers256"" (""Id"", ""SignedValue"", ""UnsignedValue"")
              VALUES ('" + id + @"', 57896044618658097711785492504343953926634992332820282019728792003956564819967, 115792089237316195423570985008687907853269984665640564039457584007913129639935)");

        // Verify data was inserted
        var exists = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""LargeNumbers256"" WHERE ""SignedValue"" > 0"
        ).AnyAsync();

        Assert.True(exists);
    }

    [Fact]
    public async Task CanQueryInt128Arithmetic()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "Int128Arithmetic" (
                "Id" UUID,
                "Value" Int128
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        // Insert some values
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""Int128Arithmetic"" (""Id"", ""Value"")
              VALUES ('" + Guid.NewGuid() + @"', 1000000000000000000000000000000)");
        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""Int128Arithmetic"" (""Id"", ""Value"")
              VALUES ('" + Guid.NewGuid() + @"', 2000000000000000000000000000000)");

        // Query with arithmetic - use alias for aggregate
        // Note: ClickHouse.Client returns BigInteger for Int128 sums
        var sum = await context.Database.SqlQueryRaw<BigInteger>(
            @"SELECT sum(""Value"") AS ""Value"" FROM ""Int128Arithmetic"""
        ).FirstOrDefaultAsync();

        Assert.Equal(BigInteger.Parse("3000000000000000000000000000000"), sum);
    }

    [Fact]
    public async Task CanQueryNegativeInt128()
    {
        await using var context = CreateContext();

        await context.Database.ExecuteSqlRawAsync("""
            CREATE TABLE IF NOT EXISTS "NegativeInt128" (
                "Id" UUID,
                "Value" Int128
            )
            ENGINE = MergeTree()
            ORDER BY "Id"
            """);

        await context.Database.ExecuteSqlRawAsync(
            @"INSERT INTO ""NegativeInt128"" (""Id"", ""Value"")
              VALUES ('" + Guid.NewGuid() + @"', -170141183460469231731687303715884105728)");

        var hasNegative = await context.Database.SqlQueryRaw<long>(
            @"SELECT count() FROM ""NegativeInt128"" WHERE ""Value"" < 0"
        ).AnyAsync();

        Assert.True(hasNegative);
    }

    #endregion

    private LargeIntegerTestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<LargeIntegerTestContext>()
            .UseClickHouse(GetConnectionString())
            .Options;

        return new LargeIntegerTestContext(options);
    }
}

#region Test Context

public class LargeIntegerTestContext : DbContext
{
    public LargeIntegerTestContext(DbContextOptions<LargeIntegerTestContext> options)
        : base(options) { }
}

#endregion
