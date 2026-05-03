using EF.CH.Extensions;
using EF.CH.ParameterizedViews;
using EF.CH.SystemTests.Fixtures;
using EF.CH.SystemTests.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EF.CH.SystemTests.Schema;

/// <summary>
/// Integration tests for parameterized view APIs against a real ClickHouse server. Covers
/// the entry points <c>FromParameterizedView</c>, <see cref="ClickHouseParameterizedView{T}"/>,
/// <c>WithParameterAsync</c>, and the DDL surface
/// (<c>CreateParameterizedViewAsync</c>, <c>EnsureParameterizedViewAsync</c>,
/// <c>EnsureParameterizedViewsAsync</c>, <c>Drop[IfExists]Async</c>), including the
/// <c>EnsureCreatedAsync</c> post-pass that creates configured views alongside their source
/// tables.
///
/// Mapping note: <c>WithParameterAsync</c> resolves the view name via EF's
/// <c>GetViewName()</c> with a fallback to the EF.CH-specific
/// <c>ParameterizedViewName</c> annotation. <c>ToParameterizedView</c> and
/// <c>AsParameterizedView</c> both call <c>ToView</c> internally so the entity is
/// registered as a view rather than a table.
/// </summary>
[Collection(SingleNodeCollection.Name)]
public sealed class ParameterizedViewLifecycleTests
{
    private readonly SingleNodeClickHouseFixture _fx;
    public ParameterizedViewLifecycleTests(SingleNodeClickHouseFixture fx) => _fx = fx;
    private string Conn => _fx.ConnectionString;

    [Fact]
    public async Task EnsureCreatedAsync_CreatesSourceAndParameterizedView()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        // EnsureCreatedAsync must create both the events source table (from ToTable) and
        // the parameterized view (from AsParameterizedView). The view's SELECT must use
        // the EF-mapped column names (event_id, event_type, etc.) rather than C# property
        // names — verified by querying through the view, which would fail if the SELECT
        // referenced unknown columns.
        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "events"));
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "events_by_user_since"));

        await ctx.Database.ExecuteSqlRawAsync(@"
            INSERT INTO events (event_id, event_type, user_id, timestamp, value) VALUES
            (1, 'click', 100, '2024-01-15 00:00:00', 1.0),
            (2, 'view', 100, '2024-01-16 00:00:00', 2.0),
            (3, 'click', 200, '2024-01-15 00:00:00', 3.0)");

        var rows = await ctx.FromParameterizedView<EventsByUserSinceView>(
                "events_by_user_since",
                new { user_id = 100UL, start_date = new DateTime(2024, 1, 15) })
            .OrderBy(e => e.Timestamp)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(100UL, r.UserId));
    }

    [Fact]
    public async Task EnsureCreatedAsync_IsIdempotentForParameterizedViews()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        await ctx.Database.EnsureCreatedAsync();
        await ctx.Database.EnsureCreatedAsync(); // second call must be a no-op

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "events_by_user_since"));
    }

    [Fact]
    public async Task EnsureCreatedAsync_GracefullySkipsAnnotationOnlyView()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<MixedCtx>(Conn);

        // MixedCtx contains both a fluent AsParameterizedView (with metadata) and a
        // ToParameterizedView-only entity (annotations only, no DDL metadata).
        // EnsureCreatedAsync must create the former and skip the latter without throwing.
        await ctx.Database.EnsureCreatedAsync();

        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "events_by_user_since"));
        Assert.False(await RawClickHouse.TableExistsAsync(Conn, "logs_by_level"));
    }

    [Fact]
    public async Task EnsureParameterizedViewAsync_CreatesAndQueriesFilteredRows()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        await SeedEventsTableAsync(ctx,
            (1, "click", 100, "2024-01-15", 1.0m),
            (2, "view", 100, "2024-01-16", 2.0m),
            (3, "click", 200, "2024-01-15", 3.0m),
            (4, "purchase", 100, "2024-01-14", 50.0m));

        await ctx.Database.EnsureParameterizedViewAsync<EventsByUserSinceView>();
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "events_by_user_since"));

        var rows = await ctx.FromParameterizedView<EventsByUserSinceView>(
                "events_by_user_since",
                new { user_id = 100UL, start_date = new DateTime(2024, 1, 15) })
            .OrderBy(e => e.Timestamp)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(100UL, r.UserId));
        Assert.All(rows, r => Assert.True(r.Timestamp >= new DateTime(2024, 1, 15)));
    }

    [Fact]
    public async Task FromParameterizedView_AnonymousAndDictionary_ReturnSameResults()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        await SeedEventsTableAsync(ctx,
            (1, "x", 7, "2024-06-01", 1m),
            (2, "y", 8, "2024-06-01", 2m));

        await ctx.Database.EnsureParameterizedViewAsync<EventsByUserSinceView>();

        var fromAnon = await ctx.FromParameterizedView<EventsByUserSinceView>(
                "events_by_user_since",
                new { user_id = 7UL, start_date = new DateTime(2024, 1, 1) })
            .ToListAsync();

        var fromDict = await ctx.FromParameterizedView<EventsByUserSinceView>(
                "events_by_user_since",
                new Dictionary<string, object?>
                {
                    ["user_id"] = 7UL,
                    ["start_date"] = new DateTime(2024, 1, 1)
                })
            .ToListAsync();

        Assert.Single(fromAnon);
        Assert.Equal(fromAnon.Count, fromDict.Count);
        Assert.Equal(fromAnon[0].EventId, fromDict[0].EventId);
    }

    [Fact]
    public async Task ClickHouseParameterizedView_Query_ComposesWithLinq()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        await SeedEventsTableAsync(ctx,
            (10, "a", 1, "2024-01-01", 5m),
            (11, "b", 1, "2024-02-01", 15m),
            (12, "c", 1, "2024-03-01", 25m));

        await ctx.Database.EnsureParameterizedViewAsync<EventsByUserSinceView>();

        var typed = new ClickHouseParameterizedView<EventsByUserSinceView>(ctx);
        Assert.Equal("events_by_user_since", typed.ViewName);

        var rows = await typed
            .Query(new { user_id = 1UL, start_date = new DateTime(2024, 1, 1) })
            .Where(e => e.Value > 10m)
            .OrderByDescending(e => e.Value)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal(25m, rows[0].Value);
        Assert.Equal(15m, rows[1].Value);
    }

    [Fact]
    public async Task ClickHouseParameterizedView_ConvenienceMethods_ToListFirstCountAny()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<FluentCtx>(Conn);

        await SeedEventsTableAsync(ctx, (1, "x", 42, "2024-05-01", 1m));

        await ctx.Database.EnsureParameterizedViewAsync<EventsByUserSinceView>();

        var typed = new ClickHouseParameterizedView<EventsByUserSinceView>(ctx);
        var args = new { user_id = 42UL, start_date = new DateTime(2024, 1, 1) };

        Assert.Single(await typed.ToListAsync(args));
        Assert.NotNull(await typed.FirstOrDefaultAsync(args));
        Assert.Equal(1, await typed.CountAsync(args));
        Assert.True(await typed.AnyAsync(args));
        Assert.False(await typed.AnyAsync(new { user_id = 999UL, start_date = new DateTime(2024, 1, 1) }));
    }

    [Fact]
    public async Task WithParameterAsync_QueriesViewMappedViaToParameterizedView()
    {
        // `WithParameterAsync` resolves the view name via EF's `GetViewName()` with a
        // fallback to the `ParameterizedViewName` annotation. HasOnlyCtx maps via
        // `ToParameterizedView`, the canonical fluent surface.
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<HasOnlyCtx>(Conn);

        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE source_logs (
                log_id UInt64,
                level String,
                message String
            ) ENGINE = MergeTree() ORDER BY log_id");
        await ctx.Database.ExecuteSqlRawAsync(@"
            INSERT INTO source_logs (log_id, level, message) VALUES
            (1, 'ERROR', 'a'), (2, 'WARN', 'b'), (3, 'ERROR', 'c')");

        await ctx.Database.CreateParameterizedViewAsync(
            "logs_by_level",
            @"SELECT log_id AS ""LogId"", level AS ""Level"", message AS ""Message""
              FROM source_logs WHERE level = {level:String}");

        var rows = await ctx.Set<LogView>().WithParameterAsync("level", "ERROR");

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal("ERROR", r.Level));
    }

    [Fact]
    public async Task WithParameterAsync_QueriesViewMappedViaAsParameterizedView()
    {
        // Pins the broader fix: WithParameterAsync must also work for entities mapped via
        // the rich `AsParameterizedView<TView, TSource>` builder, not just ToParameterizedView.
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<MixedCtx>(Conn);

        await ctx.Database.EnsureCreatedAsync();

        await ctx.Database.ExecuteSqlRawAsync(@"
            INSERT INTO events (event_id, event_type, user_id, timestamp, value) VALUES
            (1, 'click', 100, '2024-01-15 00:00:00', 1.0),
            (2, 'view', 100, '2024-01-16 00:00:00', 2.0),
            (3, 'click', 200, '2024-01-15 00:00:00', 3.0)");

        var rows = await ctx.Set<EventsByUserSinceView>().WithParameterAsync(
            new Dictionary<string, object?>
            {
                ["user_id"] = 100UL,
                ["start_date"] = new DateTime(2024, 1, 15),
            });

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(100UL, r.UserId));
    }

    [Fact]
    public async Task ParameterizedView_LiteralFormatting_EscapesDangerousStringsAtQueryTime()
    {
        // Pins end-to-end: parameter literal escape → ClickHouse accepts the produced string
        // and the WHERE match is exact (no over-match against decoys). Covers comma (separator
        // probe), single-quote, and backslash — the latter two regress if WithParameterAsync's
        // formatter ever drifts back to SQL-standard `''` from the backslash convention shared
        // with FromParameterizedView and the SQL generators.
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<HasOnlyCtx>(Conn);

        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE source_logs (
                log_id UInt64,
                level String,
                message String
            ) ENGINE = MergeTree() ORDER BY log_id");
        await ctx.Database.ExecuteSqlRawAsync(@"
            INSERT INTO source_logs (log_id, level, message) VALUES
            (1, 'normal', 'plain'),
            (2, 'has,comma', 'with-comma'),
            (3, 'has,more,commas', 'multi-comma'),
            (4, 'it\'s', 'apostrophe'),
            (5, 'a\\b', 'backslash')");

        await ctx.Database.CreateParameterizedViewAsync(
            "logs_by_level",
            @"SELECT log_id AS ""LogId"", level AS ""Level"", message AS ""Message""
              FROM source_logs WHERE level = {level:String}");

        // Plain string round-trip.
        var plain = await ctx.Set<LogView>().WithParameterAsync("level", "normal");
        Assert.Single(plain);
        Assert.Equal(1uL, plain[0].LogId);

        // Comma in parameter value must NOT be parsed as parameter-list separator.
        var comma = await ctx.Set<LogView>().WithParameterAsync("level", "has,comma");
        Assert.Single(comma);
        Assert.Equal(2uL, comma[0].LogId);

        var multi = await ctx.Set<LogView>().WithParameterAsync("level", "has,more,commas");
        Assert.Single(multi);
        Assert.Equal(3uL, multi[0].LogId);

        var apostrophe = await ctx.Set<LogView>().WithParameterAsync("level", "it's");
        Assert.Single(apostrophe);
        Assert.Equal(4uL, apostrophe[0].LogId);

        var backslash = await ctx.Set<LogView>().WithParameterAsync("level", @"a\b");
        Assert.Single(backslash);
        Assert.Equal(5uL, backslash[0].LogId);
    }

    [Fact]
    public async Task ParameterizedView_DropStrictAndIfExistsPaths()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<EmptyCtx>(Conn);

        await ctx.Database.CreateParameterizedViewAsync("temp_pv", "SELECT 1 AS value");
        await ctx.Database.CreateParameterizedViewAsync("temp_pv", "SELECT 1 AS value"); // idempotent

        await ctx.Database.DropParameterizedViewAsync("temp_pv");

        await Assert.ThrowsAsync<ClickHouse.Driver.ClickHouseServerException>(() =>
            ctx.Database.DropParameterizedViewAsync("temp_pv"));

        await ctx.Database.DropParameterizedViewIfExistsAsync("temp_pv"); // silent no-op
    }

    [Fact]
    public async Task EnsureParameterizedViewsAsync_SkipsAnnotationOnlyView()
    {
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<MixedCtx>(Conn);

        // Source for the fluent view; the annotation-only entity has no source/DDL.
        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE events (
                event_id UInt64,
                event_type String,
                user_id UInt64,
                timestamp DateTime,
                value Decimal(18, 4)
            ) ENGINE = MergeTree() ORDER BY (user_id, timestamp)");

        var created = await ctx.Database.EnsureParameterizedViewsAsync();

        // Only the fluent EventsByUserSinceView is created; LogViewHasOnly is skipped.
        Assert.Equal(1, created);
        Assert.True(await RawClickHouse.TableExistsAsync(Conn, "events_by_user_since"));
        Assert.False(await RawClickHouse.TableExistsAsync(Conn, "logs_by_level"));
    }

    [Fact]
    public async Task ParameterizedView_TypeInference_SupportedTypes_Roundtrip()
    {
        // Exercises GetClickHouseType() across the supported CLR types end-to-end. Each parameter
        // is constructed (DDL placeholder → query literal → match) so any drift surfaces as a
        // row-count mismatch.
        await ResetAsync();
        await using var ctx = TestContextFactory.Create<TypesCtx>(Conn);

        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE type_probes (
                id UInt64,
                flag UInt8,
                tiny UInt8,
                number Int32,
                big Int64,
                money Decimal(18, 4),
                label String,
                stamp DateTime,
                day Date,
                token UUID
            ) ENGINE = MergeTree() ORDER BY id");
        await ctx.Database.ExecuteSqlRawAsync(@"
            INSERT INTO type_probes
            (id, flag, tiny, number, big, money, label, stamp, day, token) VALUES
            (1, 1, 1, 1, 1, 1.0, 'anything', '2024-01-01 00:00:00', '2024-01-01',
             toUUID('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'))");

        await ctx.Database.EnsureParameterizedViewAsync<TypeProbeView>();

        var hit = await ctx.FromParameterizedView<TypeProbeView>(
                "type_probe_view",
                new
                {
                    p_bool = true,
                    p_byte = (byte)1,
                    p_int = 42,
                    p_long = 9_999_999_999L,
                    p_decimal = 123.45m,
                    p_string = "alpha",
                    p_dt = new DateTime(2024, 6, 15, 12, 0, 0),
                    p_date = new DateOnly(2024, 6, 15),
                    p_guid = Guid.Parse("11111111-2222-3333-4444-555555555555"),
                })
            .ToListAsync();

        Assert.NotEmpty(hit);
    }

    /// <summary>
    /// Wipe every user-created table/view in the current database (mirrors
    /// <see cref="PlainViewLifecycleTests"/>).
    /// </summary>
    private async Task ResetAsync()
    {
        var rows = await RawClickHouse.RowsAsync(Conn,
            "SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name NOT LIKE '.%'");
        foreach (var r in rows)
        {
            var name = (string)r["name"]!;
            var engine = (string?)r["engine"];
            var kind = engine is "View" or "MaterializedView" or "LiveView" ? "VIEW" : "TABLE";
            await RawClickHouse.ExecuteAsync(Conn, $"DROP {kind} IF EXISTS \"{name}\" SYNC");
        }
    }

    private static async Task SeedEventsTableAsync(DbContext ctx,
        params (ulong id, string type, ulong userId, string date, decimal value)[] rows)
    {
        await ctx.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE events (
                event_id UInt64,
                event_type String,
                user_id UInt64,
                timestamp DateTime,
                value Decimal(18, 4)
            ) ENGINE = MergeTree() ORDER BY (user_id, timestamp)");

        if (rows.Length == 0) return;

        var values = string.Join(", ", rows.Select(r =>
            $"({r.id}, '{r.type}', {r.userId}, '{r.date} 00:00:00', {r.value.ToString(System.Globalization.CultureInfo.InvariantCulture)})"));
        await ctx.Database.ExecuteSqlRawAsync(
            $"INSERT INTO events (event_id, event_type, user_id, timestamp, value) VALUES {values}");
    }

    public sealed class Event
    {
        public ulong EventId { get; set; }
        public string EventType { get; set; } = string.Empty;
        public ulong UserId { get; set; }
        public DateTime Timestamp { get; set; }
        public decimal Value { get; set; }
    }

    public sealed class EventsByUserSinceView
    {
        public ulong EventId { get; set; }
        public string EventType { get; set; } = string.Empty;
        public ulong UserId { get; set; }
        public DateTime Timestamp { get; set; }
        public decimal Value { get; set; }
    }

    public sealed class LogView
    {
        public ulong LogId { get; set; }
        public string Level { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
    }

    public sealed class LogViewHasOnly
    {
        public ulong LogId { get; set; }
        public string Level { get; set; } = string.Empty;
    }

    public sealed class TypeProbe
    {
        public ulong Id { get; set; }
        public bool Flag { get; set; }
        public byte Tiny { get; set; }
        public int Number { get; set; }
        public long Big { get; set; }
        public decimal Money { get; set; }
        public string Label { get; set; } = string.Empty;
        public DateTime Stamp { get; set; }
        public DateOnly Day { get; set; }
        public Guid Token { get; set; }
    }

    public sealed class TypeProbeView
    {
        public ulong Id { get; set; }
    }

    public sealed class EmptyCtx(DbContextOptions<EmptyCtx> o) : DbContext(o)
    {
    }

    public sealed class FluentCtx(DbContextOptions<FluentCtx> o) : DbContext(o)
    {
        public DbSet<Event> Events => Set<Event>();
        public DbSet<EventsByUserSinceView> EventsByUserSince => Set<EventsByUserSinceView>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Event>(e =>
            {
                e.ToTable("events");
                e.HasKey(x => x.EventId);
                e.Property(x => x.EventId).HasColumnName("event_id");
                e.Property(x => x.EventType).HasColumnName("event_type");
                e.Property(x => x.UserId).HasColumnName("user_id");
                e.Property(x => x.Timestamp).HasColumnName("timestamp");
                e.Property(x => x.Value).HasColumnName("value");
            });

            mb.Entity<EventsByUserSinceView>(e =>
            {
                e.AsParameterizedView<EventsByUserSinceView, Event>(cfg => cfg
                    .HasName("events_by_user_since")
                    .FromTable()
                    .Select(s => new EventsByUserSinceView
                    {
                        EventId = s.EventId,
                        EventType = s.EventType,
                        UserId = s.UserId,
                        Timestamp = s.Timestamp,
                        Value = s.Value,
                    })
                    .Parameter<ulong>("user_id")
                    .Parameter<DateTime>("start_date")
                    .Where((s, p) => s.UserId == p.Get<ulong>("user_id"))
                    .Where((s, p) => s.Timestamp >= p.Get<DateTime>("start_date")));
            });
        }
    }

    public sealed class HasOnlyCtx(DbContextOptions<HasOnlyCtx> o) : DbContext(o)
    {
        public DbSet<LogView> Logs => Set<LogView>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            // ToParameterizedView populates EF's GetViewName(), which WithParameterAsync requires.
            mb.Entity<LogView>(e => e.ToParameterizedView("logs_by_level"));
        }
    }

    public sealed class MixedCtx(DbContextOptions<MixedCtx> o) : DbContext(o)
    {
        public DbSet<Event> Events => Set<Event>();
        public DbSet<EventsByUserSinceView> EventsByUserSince => Set<EventsByUserSinceView>();
        public DbSet<LogViewHasOnly> Logs => Set<LogViewHasOnly>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Event>(e =>
            {
                e.ToTable("events");
                e.HasKey(x => x.EventId);
                e.Property(x => x.EventId).HasColumnName("event_id");
                e.Property(x => x.EventType).HasColumnName("event_type");
                e.Property(x => x.UserId).HasColumnName("user_id");
                e.Property(x => x.Timestamp).HasColumnName("timestamp");
                e.Property(x => x.Value).HasColumnName("value");
            });

            mb.Entity<EventsByUserSinceView>(e =>
            {
                e.AsParameterizedView<EventsByUserSinceView, Event>(cfg => cfg
                    .HasName("events_by_user_since")
                    .FromTable()
                    .Select(s => new EventsByUserSinceView
                    {
                        EventId = s.EventId,
                        EventType = s.EventType,
                        UserId = s.UserId,
                        Timestamp = s.Timestamp,
                        Value = s.Value,
                    })
                    .Parameter<ulong>("user_id")
                    .Parameter<DateTime>("start_date")
                    .Where((s, p) => s.UserId == p.Get<ulong>("user_id"))
                    .Where((s, p) => s.Timestamp >= p.Get<DateTime>("start_date")));
            });

            // ToParameterizedView only: annotations + ToView, but no DDL metadata.
            // EnsureParameterizedViewsAsync skips this.
            mb.Entity<LogViewHasOnly>(e => e.ToParameterizedView("logs_by_level"));
        }
    }

    public sealed class TypesCtx(DbContextOptions<TypesCtx> o) : DbContext(o)
    {
        public DbSet<TypeProbe> Probes => Set<TypeProbe>();
        public DbSet<TypeProbeView> ProbeView => Set<TypeProbeView>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<TypeProbe>(e =>
            {
                e.ToTable("type_probes");
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasColumnName("id");
                e.Property(x => x.Flag).HasColumnName("flag");
                e.Property(x => x.Tiny).HasColumnName("tiny");
                e.Property(x => x.Number).HasColumnName("number");
                e.Property(x => x.Big).HasColumnName("big");
                e.Property(x => x.Money).HasColumnName("money");
                e.Property(x => x.Label).HasColumnName("label");
                e.Property(x => x.Stamp).HasColumnName("stamp");
                e.Property(x => x.Day).HasColumnName("day");
                e.Property(x => x.Token).HasColumnName("token");
            });

            mb.Entity<TypeProbeView>(e =>
            {
                e.AsParameterizedView<TypeProbeView, TypeProbe>(cfg => cfg
                    .HasName("type_probe_view")
                    .FromTable()
                    .Select(p => new TypeProbeView { Id = p.Id })
                    .Parameter<bool>("p_bool")
                    .Parameter<byte>("p_byte")
                    .Parameter<int>("p_int")
                    .Parameter<long>("p_long")
                    .Parameter<decimal>("p_decimal")
                    .Parameter<string>("p_string")
                    .Parameter<DateTime>("p_dt")
                    .Parameter<DateOnly>("p_date")
                    .Parameter<Guid>("p_guid")
                    // The WHERE visitor only permits source-property and parameter-access expressions.
                    // Compare each source column against its corresponding parameter so every type
                    // round-trips end-to-end.
                    .Where((p, a) => p.Flag == a.Get<bool>("p_bool")
                                  && p.Tiny == a.Get<byte>("p_byte")
                                  && p.Number < a.Get<int>("p_int")
                                  && p.Big < a.Get<long>("p_long")
                                  && p.Money < a.Get<decimal>("p_decimal")
                                  && p.Label != a.Get<string>("p_string")
                                  && p.Stamp < a.Get<DateTime>("p_dt")
                                  && p.Day < a.Get<DateOnly>("p_date")
                                  && p.Token != a.Get<Guid>("p_guid")));
            });
        }
    }
}
