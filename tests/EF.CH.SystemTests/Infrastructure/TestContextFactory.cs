using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

namespace EF.CH.SystemTests.Infrastructure;

public static class TestContextFactory
{
    public static TContext Create<TContext>(string connectionString)
        where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(connectionString)
            .Options;
        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    public static TContext Create<TContext>(
        string connectionString,
        Action<ClickHouseDbContextOptionsBuilder> configure)
        where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(connectionString, configure)
            .Options;
        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }

    public static TContext CreateWithCluster<TContext>(string connectionString, string clusterName)
        where TContext : DbContext
    {
        var options = new DbContextOptionsBuilder<TContext>()
            .UseClickHouse(connectionString, o => o.UseCluster(clusterName))
            .Options;
        return (TContext)Activator.CreateInstance(typeof(TContext), options)!;
    }
}
