using EF.CH.Infrastructure;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Unit tests for ClickHouseDownMigrationNotSupportedException.
/// </summary>
public class ForwardOnlyMigrationTests
{
    [Fact]
    public void ClickHouseDownMigrationNotSupportedException_ContainsMigrationId()
    {
        var migrationId = "20250107100000_AddOrders_001";

        var ex = new ClickHouseDownMigrationNotSupportedException(migrationId);

        Assert.Equal(migrationId, ex.MigrationId);
    }

    [Fact]
    public void ClickHouseDownMigrationNotSupportedException_MessageContainsMigrationId()
    {
        var migrationId = "20250107100000_AddOrders_001";

        var ex = new ClickHouseDownMigrationNotSupportedException(migrationId);

        Assert.Contains(migrationId, ex.Message);
    }

    [Fact]
    public void ClickHouseDownMigrationNotSupportedException_MessageExplainsReason()
    {
        var ex = new ClickHouseDownMigrationNotSupportedException("test_migration");

        Assert.Contains("ACID", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("transaction", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ClickHouseDownMigrationNotSupportedException_IsNotSupportedException()
    {
        var ex = new ClickHouseDownMigrationNotSupportedException("test_migration");

        Assert.IsType<ClickHouseDownMigrationNotSupportedException>(ex);
        Assert.IsAssignableFrom<NotSupportedException>(ex);
    }

    [Fact]
    public void ClickHouseDownMigrationNotSupportedException_SuggestsForwardMigration()
    {
        var ex = new ClickHouseDownMigrationNotSupportedException("test_migration");

        Assert.Contains("forward", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("new migration", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
