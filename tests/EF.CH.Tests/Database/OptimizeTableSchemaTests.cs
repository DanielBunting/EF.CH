using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using System.Data.Common;
using Xunit;

namespace EF.CH.Tests.Database;

/// <summary>
/// Pins schema-qualification of admin commands that target an entity's mapped
/// table — <c>OPTIMIZE TABLE</c> and <c>SYSTEM SYNC REPLICA</c>. Without
/// schema-qualification, the command runs against the connection's current
/// database, not the model-mapped database, and silently targets the wrong
/// table (or errors with "table not found") in multi-database setups.
/// </summary>
public class OptimizeTableSchemaTests
{
    [Fact]
    public async Task OptimizeTableAsync_TypedOverload_QualifiesWithEntitySchema()
    {
        var interceptor = new CapturingCommandInterceptor();
        await using var ctx = CreateCtx(interceptor);

        try { await ctx.Database.OptimizeTableAsync<SchemaQualifiedEntity>(); }
        catch { /* connection is fake; we only care about the SQL */ }

        Assert.Contains(
            interceptor.CommandTexts,
            sql => sql.Contains("OPTIMIZE TABLE", StringComparison.OrdinalIgnoreCase)
                && sql.Contains("\"analytics\".\"events_v1\"", StringComparison.Ordinal));
    }

    [Fact]
    public async Task SyncReplicaAsync_TypedOverload_QualifiesWithEntitySchema()
    {
        var interceptor = new CapturingCommandInterceptor();
        await using var ctx = CreateCtx(interceptor);

        try { await ctx.Database.SyncReplicaAsync<SchemaQualifiedEntity>(); }
        catch { /* connection is fake; we only care about the SQL */ }

        Assert.Contains(
            interceptor.CommandTexts,
            sql => sql.Contains("SYSTEM SYNC REPLICA", StringComparison.OrdinalIgnoreCase)
                && sql.Contains("\"analytics\".\"events_v1\"", StringComparison.Ordinal));
    }

    private static SchemaCtx CreateCtx(CapturingCommandInterceptor interceptor) =>
        new(new DbContextOptionsBuilder<SchemaCtx>()
            .UseClickHouse("Host=localhost;Port=1;Database=default")
            .AddInterceptors(interceptor)
            .Options);

    public sealed class SchemaQualifiedEntity { public Guid Id { get; set; } }

    public sealed class SchemaCtx(DbContextOptions<SchemaCtx> o) : DbContext(o)
    {
        public DbSet<SchemaQualifiedEntity> Events => Set<SchemaQualifiedEntity>();
        protected override void OnModelCreating(ModelBuilder mb)
            => mb.Entity<SchemaQualifiedEntity>(e =>
            {
                e.ToTable("events_v1", "analytics");
                e.HasKey(x => x.Id);
                e.UseMergeTree(x => x.Id);
            });
    }

    private sealed class CapturingCommandInterceptor : DbCommandInterceptor
    {
        public List<string> CommandTexts { get; } = new();

        public override InterceptionResult<int> NonQueryExecuting(
            DbCommand command,
            CommandEventData eventData,
            InterceptionResult<int> result)
        {
            CommandTexts.Add(command.CommandText);
            return result;
        }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
            DbCommand command,
            CommandEventData eventData,
            InterceptionResult<int> result,
            CancellationToken cancellationToken = default)
        {
            CommandTexts.Add(command.CommandText);
            return new(result);
        }
    }
}
