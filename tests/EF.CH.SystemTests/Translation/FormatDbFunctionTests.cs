using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;
using EfClass = Microsoft.EntityFrameworkCore.EF;

namespace EF.CH.SystemTests.Translation;

/// <summary>
/// Coverage of <see cref="ClickHouseFormatDbFunctionsExtensions"/>: pin the
/// translated SQL output for canonical inputs against a live ClickHouse
/// server. Tests for stub methods that don't yet have a translator are left
/// to fail at translation so the gap is visible in CI (the audit's
/// "specification" pattern).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class FormatDbFunctionTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public FormatDbFunctionTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<Ctx> SeededAsync()
    {
        var ctx = TestContextFactory.Create<Ctx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row
        {
            Id = 1,
            Text = "hello",
            OtherText = "world",
            Number = 42,
            At = new DateTime(2024, 6, 15, 12, 34, 56, DateTimeKind.Utc),
        });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        return ctx;
    }

    [Fact]
    public async Task FormatDateTime_RendersExpectedString()
    {
        await using var ctx = await SeededAsync();
        // ClickHouse's formatDateTime uses %F = ISO date and %T = ISO time
        // (HH:MM:SS), which avoids the %M=minute-vs-month-name ambiguity that
        // bites people writing strftime patterns directly.
        var s = await ctx.Rows.Select(x =>
            EfClass.Functions.FormatDateTime(x.At, "%F %T")).FirstAsync();
        Assert.Equal("2024-06-15 12:34:56", s);
    }

    [Fact]
    public async Task FormatRow_CsvProducer_ReturnsRoundTrippableLine()
    {
        await using var ctx = await SeededAsync();
        var line = await ctx.Rows.Select(x =>
            EfClass.Functions.FormatRow("CSV", x.Text, x.OtherText)).FirstAsync();
        Assert.Contains("\"hello\"", line);
        Assert.Contains("\"world\"", line);
    }

    [Fact]
    public async Task FormatString_AppliesPositionalPlaceholders()
    {
        await using var ctx = await SeededAsync();
        var s = await ctx.Rows.Select(x =>
            EfClass.Functions.FormatString("{0} = {1}", x.Text, x.OtherText)).FirstAsync();
        Assert.Equal("hello = world", s);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Text { get; set; } = "";
        public string OtherText { get; set; } = "";
        public int Number { get; set; }
        public DateTime At { get; set; }
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("FormatFn_Rows"); e.HasKey(x => x.Id); e.UseMergeTree(x => x.Id);
            });
    }
}
