using EF.CH.Extensions;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Engines;

/// <summary>
/// Coverage of the Log family: <c>UseLogEngine</c>, <c>UseTinyLogEngine</c>, <c>UseStripeLogEngine</c>.
/// All store rows append-only; we round-trip and check the <i>exact</i> engine name from
/// <c>system.tables.engine</c> (substring matches against engine_full would conflate Log/TinyLog/StripeLog).
/// </summary>
[Collection(SingleNodeCollection.Name)]
public class LogFamilyEngineTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public LogFamilyEngineTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    private async Task<string> EngineNameAsync(string table) =>
        await RawClickHouse.ScalarAsync<string>(Conn,
            $"SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = '{RawClickHouse.Esc(table)}'");

    [Fact]
    public async Task LogEngine_RoundTrips_AndDeclaresExactlyLog()
    {
        await using var ctx = TestContextFactory.Create<LogCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, Note = "log" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        Assert.Equal("Log", await EngineNameAsync("LogEng_Rows"));
        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal("log", read.Note);
    }

    [Fact]
    public async Task TinyLogEngine_RoundTrips_AndDeclaresExactlyTinyLog()
    {
        await using var ctx = TestContextFactory.Create<TinyCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, Note = "tiny" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        Assert.Equal("TinyLog", await EngineNameAsync("TinyLogEng_Rows"));
        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal("tiny", read.Note);
    }

    [Fact]
    public async Task StripeLogEngine_RoundTrips_AndDeclaresExactlyStripeLog()
    {
        await using var ctx = TestContextFactory.Create<StripeCtx>(Conn);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
        ctx.Rows.Add(new Row { Id = 1, Note = "stripe" });
        await ctx.SaveChangesAsync();
        ctx.ChangeTracker.Clear();
        Assert.Equal("StripeLog", await EngineNameAsync("StripeLogEng_Rows"));
        var read = await ctx.Rows.SingleAsync(r => r.Id == 1);
        Assert.Equal("stripe", read.Note);
    }

    public sealed class Row
    {
        public uint Id { get; set; }
        public string Note { get; set; } = "";
    }
    public sealed class LogCtx(DbContextOptions<LogCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("LogEng_Rows"); e.HasKey(x => x.Id); e.UseLogEngine();
            });
    }
    public sealed class TinyCtx(DbContextOptions<TinyCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("TinyLogEng_Rows"); e.HasKey(x => x.Id); e.UseTinyLogEngine();
            });
    }
    public sealed class StripeCtx(DbContextOptions<StripeCtx> o) : DbContext(o)
    {
        public DbSet<Row> Rows => Set<Row>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Row>(e =>
            {
                e.ToTable("StripeLogEng_Rows"); e.HasKey(x => x.Id); e.UseStripeLogEngine();
            });
    }
}
