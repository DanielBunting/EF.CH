using EF.CH.Extensions;
using EF.CH.Metadata;
using EF.CH.Tests.Engines;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.Tests.Configuration;

public class ShardingKeyTests
{
    [Fact]
    public void WithShardingKey_AcceptsDirectMemberAccess()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestDistributedEntity>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.Id);
            entity.UseDistributed("c", "events_local")
                  .WithShardingKey(x => x.UserId);
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestDistributedEntity))!;

        Assert.Equal(
            "UserId",
            entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedShardingKey)?.Value);
    }

    [Fact]
    public void WithShardingKey_RejectsFunctionCallExpression()
    {
        var builder = new ModelBuilder();

        var ex = Assert.Throws<ArgumentException>(() =>
        {
            builder.Entity<TestDistributedEntity>(entity =>
            {
                entity.ToTable("events");
                entity.HasKey(e => e.Id);
                entity.UseDistributed("c", "events_local")
                      // intHash64(UserId) is a method call — must throw
                      .WithShardingKey(x => SqlHash(x.UserId));
            });
        });

        Assert.Contains("WithShardingKeyExpression", ex.Message);
    }

    [Fact]
    public void WithShardingKey_RejectsBinaryExpression()
    {
        var builder = new ModelBuilder();

        Assert.Throws<ArgumentException>(() =>
        {
            builder.Entity<TestDistributedEntity>(entity =>
            {
                entity.ToTable("events");
                entity.HasKey(e => e.Id);
                entity.UseDistributed("c", "events_local")
                      .WithShardingKey(x => x.UserId + 1);
            });
        });
    }

    [Fact]
    public void WithShardingKeyExpression_AcceptsRawSql()
    {
        var builder = new ModelBuilder();

        builder.Entity<TestDistributedEntity>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(e => e.Id);
            entity.UseDistributed("c", "events_local")
                  .WithShardingKeyExpression("cityHash64(UserId)");
        });

        var model = builder.FinalizeModel();
        var entityType = model.FindEntityType(typeof(TestDistributedEntity))!;

        Assert.Equal(
            "cityHash64(UserId)",
            entityType.FindAnnotation(ClickHouseAnnotationNames.DistributedShardingKey)?.Value);
    }

    [Fact]
    public void WithShardingKeyExpression_RejectsEmpty()
    {
        var builder = new ModelBuilder();

        Assert.Throws<ArgumentException>(() =>
        {
            builder.Entity<TestDistributedEntity>(entity =>
            {
                entity.ToTable("events");
                entity.HasKey(e => e.Id);
                entity.UseDistributed("c", "events_local")
                      .WithShardingKeyExpression("");
            });
        });
    }

    // Stand-in for any client-side function used inside an expression — it must
    // never be evaluated; we want the sharding-key validator to reject the lambda
    // before the expression compiler ever invokes it.
    private static long SqlHash(long value) => value;
}
