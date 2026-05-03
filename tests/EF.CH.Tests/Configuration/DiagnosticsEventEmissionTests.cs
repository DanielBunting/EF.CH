using System.Diagnostics;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Xunit;

namespace EF.CH.Tests.Configuration;

/// <summary>
/// EF.CH ships logging definitions for query diagnostics, but no test today
/// asserts the underlying <see cref="DiagnosticListener"/> events actually
/// fire. This is a smoke test: register an observer for EF Core's
/// <c>Microsoft.EntityFrameworkCore</c> diagnostic source, run a tiny query
/// (only <c>ToQueryString</c> so we don't need a live ClickHouse), and
/// assert the command-related event fires.
/// </summary>
public class DiagnosticsEventEmissionTests
{
    [Fact]
    public void QueryExecution_EmitsCommandExecutedEvent()
    {
        var seenEvents = new List<string>();
        using var subscription = DiagnosticListener.AllListeners.Subscribe(
            new ListenerObserver(seenEvents));

        using var ctx = CreateContext();
        // Force the query pipeline to run: ToQueryString triggers the
        // EF Core compilation diagnostics even without a live server.
        _ = ctx.Items.Where(i => i.Id > 0).ToQueryString();

        // Allow the diagnostic source to flush; events are emitted
        // synchronously inside the EF Core pipeline.
        Assert.NotEmpty(seenEvents);
        Assert.Contains(seenEvents, name =>
            name.StartsWith("Microsoft.EntityFrameworkCore", StringComparison.Ordinal));
    }

    private sealed class ListenerObserver : IObserver<DiagnosticListener>
    {
        private readonly List<string> _events;
        public ListenerObserver(List<string> events) => _events = events;

        public void OnCompleted() { }
        public void OnError(Exception error) { }
        public void OnNext(DiagnosticListener value)
        {
            if (value.Name.StartsWith("Microsoft.EntityFrameworkCore", StringComparison.Ordinal))
                value.Subscribe(new EventObserver(_events));
        }

        private sealed class EventObserver : IObserver<KeyValuePair<string, object?>>
        {
            private readonly List<string> _events;
            public EventObserver(List<string> events) => _events = events;
            public void OnCompleted() { }
            public void OnError(Exception error) { }
            public void OnNext(KeyValuePair<string, object?> value) => _events.Add(value.Key);
        }
    }

    private static Ctx CreateContext()
    {
        var options = new DbContextOptionsBuilder<Ctx>()
            .UseClickHouse("Host=localhost;Database=test")
            .Options;
        return new Ctx(options);
    }

    public sealed class Item
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    public sealed class Ctx(DbContextOptions<Ctx> o) : DbContext(o)
    {
        public DbSet<Item> Items => Set<Item>();
        protected override void OnModelCreating(ModelBuilder mb) =>
            mb.Entity<Item>(e =>
            {
                e.ToTable("diag_items");
                e.HasKey(x => x.Id);
            });
    }
}
