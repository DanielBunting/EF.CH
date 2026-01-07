using EF.CH.Infrastructure;
using Xunit;

namespace EF.CH.Tests.Migrations;

/// <summary>
/// Tests for forward-only migration enforcement.
/// Verifies that Down migrations throw the appropriate exception.
/// </summary>
public class ForwardOnlyMigrationTests
{
    [Fact]
    public void ClickHouseDownMigrationNotSupportedException_ContainsMigrationId()
    {
        var migrationId = "20250107100000_AddOrders_001";

        var exception = new ClickHouseDownMigrationNotSupportedException(migrationId);

        Assert.Equal(migrationId, exception.MigrationId);
    }

    [Fact]
    public void ClickHouseDownMigrationNotSupportedException_MessageExplainsReason()
    {
        var migrationId = "20250107100000_AddOrders_001";

        var exception = new ClickHouseDownMigrationNotSupportedException(migrationId);

        Assert.Contains("not supported", exception.Message);
        Assert.Contains("forward-only", exception.Message.ToLowerInvariant());
        Assert.Contains(migrationId, exception.Message);
    }

    [Fact]
    public void ClickHouseDownMigrationNotSupportedException_IsNotSupportedException()
    {
        var exception = new ClickHouseDownMigrationNotSupportedException("test");

        Assert.IsAssignableFrom<NotSupportedException>(exception);
    }

    [Fact]
    public void ClickHouseDownMigrationNotSupportedException_SuggestsForwardMigration()
    {
        var exception = new ClickHouseDownMigrationNotSupportedException("test");

        // The exception message should guide users to use forward-only migrations
        Assert.Contains("new migration", exception.Message.ToLowerInvariant());
    }
}
