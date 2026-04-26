using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using EF.CH.TempTable;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.TempTable;

/// <summary>
/// Coverage of <c>BeginTempTableScope</c> — multiple temp tables created within the
/// scope, all dropped on scope dispose.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class TempTableScopeTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public TempTableScopeTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task TempTableScope_ManagesMultipleHandles_AndDropsThemOnScopeDispose()
    {
        await using var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();

        string n1Name, n2Name;
        await using (var scope = ctx.BeginTempTableScope())
        {
            var t1 = await scope.CreateAsync<RowA>();
            var t2 = await scope.CreateAsync<RowB>();
            n1Name = t1.TableName;
            n2Name = t2.TableName;

            await t1.InsertAsync(new[] { new RowA { Id = 1, S = "x" } });
            await t2.InsertAsync(new[] { new RowB { Id = 1, V = 7 } });
            Assert.Equal(1, await t1.Query().CountAsync());
            Assert.Equal(1, await t2.Query().CountAsync());
        }

        // After scope dispose, both temp tables must be gone.
        var remaining = await RawClickHouse.ScalarAsync<ulong>(Conn,
            $"SELECT count() FROM system.tables WHERE name IN ('{RawClickHouse.Esc(n1Name)}', '{RawClickHouse.Esc(n2Name)}')");
        Assert.Equal(0ul, remaining);
    }

    public sealed class RowA { public uint Id { get; set; } public string S { get; set; } = ""; }
    public sealed class RowB { public uint Id { get; set; } public int V { get; set; } }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<RowA> A => Set<RowA>();
        public DbSet<RowB> B => Set<RowB>();
        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<RowA>(e =>
            {
                e.ToTable("TempScope_A"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
            mb.Entity<RowB>(e =>
            {
                e.ToTable("TempScope_B"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
        }
    }
}
