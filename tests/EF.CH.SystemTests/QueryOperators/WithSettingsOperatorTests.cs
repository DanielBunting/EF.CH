using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.QueryOperators;

/// <summary>
/// Coverage of <c>WithSettings(dict)</c> and <c>WithSetting(name, value)</c>. These
/// thread-local SETTINGS pass through the postprocessor and SQL generator — exactly
/// the kind of surface that breaks silently if the [ThreadStatic] cleanup regresses.
///
/// Each test uses a setting with an <i>observable side effect</i> (a hostile threshold
/// that makes the query throw) so we know the SETTINGS clause was actually honoured —
/// asserting only that the row count is correct would also pass if the operator were
/// silently dropped.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class WithSettingsOperatorTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public WithSettingsOperatorTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        for (uint i = 1; i <= 50; i++) ctx.Rows.Add(new Row { Id = i, Value = (int)i });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task WithSetting_HostileMaxRowsToRead_ThrowsTooManyRows()
    {
        // max_rows_to_read=5 with read_overflow_mode='throw' (the default) must reject
        // a 50-row scan. If WithSetting silently drops the setting, the query succeeds
        // and the assertion on the thrown exception fails.
        await using var ctx = await SeededAsync();

        var ex = await Record.ExceptionAsync(() => ctx.Rows
            .WithSetting("max_rows_to_read", 5)
            .ToListAsync());
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task WithSettings_DictionaryWithHostileMaxResultRows_Throws()
    {
        await using var ctx = await SeededAsync();
        var dict = new Dictionary<string, object>
        {
            ["max_result_rows"] = 5,
            ["result_overflow_mode"] = "throw",
        };
        var ex = await Record.ExceptionAsync(() => ctx.Rows.WithSettings(dict).ToListAsync());
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task WithSetting_BenignMaxThreads_StillCountsRows()
    {
        // max_threads=1 has no row-level effect — useful as a control case to prove the
        // setting itself parses correctly and the query still executes end-to-end.
        await using var ctx = await SeededAsync();
        var n = await ctx.Rows.WithSetting("max_threads", 1).CountAsync();
        Assert.Equal(50, n);
    }

    [Fact]
    public async Task WithSettings_NotLeakedToNextQuery()
    {
        // First query installs a hostile setting that, if leaked, would break the
        // second. Second query is unscoped — must succeed on the full 50-row count.
        await using var ctx = await SeededAsync();
        var ex = await Record.ExceptionAsync(() => ctx.Rows
            .WithSetting("max_rows_to_read", 5)
            .ToListAsync());
        Assert.NotNull(ex);

        var allList = await ctx.Rows.OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(50, allList.Count);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public int Value { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("WithSettingsOpTests_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
