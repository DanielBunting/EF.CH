using System.Collections.Concurrent;
using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Testcontainers.ClickHouse;
using Xunit;

namespace EF.CH.Tests.Storage;

public class NoOpTransactionTests : IAsyncLifetime
{
    private readonly ClickHouseContainer _container = new ClickHouseBuilder()
        .WithImage("clickhouse/clickhouse-server:25.6")
        .Build();

    public Task InitializeAsync() => _container.StartAsync();
    public Task DisposeAsync() => _container.DisposeAsync().AsTask();

    [Fact]
    public async Task BeginTransaction_LogsWarningOnce_PerConnection()
    {
        var loggerProvider = new CapturingLoggerProvider();

        await using var context = CreateContext(loggerProvider);

        // Open the underlying connection so transactions actually attach to a real one.
        await context.Database.OpenConnectionAsync();

        for (var i = 0; i < 3; i++)
        {
            using var tx = await context.Database.BeginTransactionAsync();
            await tx.CommitAsync();
        }

        var warnings = loggerProvider.GetLogs(LogLevel.Warning, "ClickHouseTransactionsNotSupported");
        Assert.Single(warnings);
        Assert.Contains("ClickHouse does not support transactions", warnings[0].Message);
    }

    [Fact]
    public async Task Commit_DoesNotEmitWarning()
    {
        var loggerProvider = new CapturingLoggerProvider();

        await using var context = CreateContext(loggerProvider);
        await context.Database.OpenConnectionAsync();

        using var tx = await context.Database.BeginTransactionAsync();

        var beforeCommit = loggerProvider.GetLogs(LogLevel.Warning, "ClickHouseTransactionsNotSupported").Count;
        await tx.CommitAsync();
        var afterCommit = loggerProvider.GetLogs(LogLevel.Warning, "ClickHouseTransactionsNotSupported").Count;

        Assert.Equal(beforeCommit, afterCommit);
    }

    [Fact]
    public async Task SyncBeginTransaction_AlsoEmitsWarning()
    {
        var loggerProvider = new CapturingLoggerProvider();

        await using var context = CreateContext(loggerProvider);
        await context.Database.OpenConnectionAsync();

        using var tx = context.Database.BeginTransaction();
        tx.Commit();

        var warnings = loggerProvider.GetLogs(LogLevel.Warning, "ClickHouseTransactionsNotSupported");
        Assert.Single(warnings);
    }


    private TxTestContext CreateContext(ILoggerProvider loggerProvider)
    {
        var loggerFactory = LoggerFactory.Create(b => b.AddProvider(loggerProvider).SetMinimumLevel(LogLevel.Trace));

        var options = new DbContextOptionsBuilder<TxTestContext>()
            .UseLoggerFactory(loggerFactory)
            .UseClickHouse(_container.GetConnectionString())
            .Options;

        return new TxTestContext(options);
    }
}

#region Test context

public class TxTestEntity
{
    public Guid Id { get; set; }
}

public class TxTestContext : DbContext
{
    public TxTestContext(DbContextOptions<TxTestContext> options) : base(options) { }

    public DbSet<TxTestEntity> Entities => Set<TxTestEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TxTestEntity>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.ToTable("tx_test_entities");
            entity.UseMergeTree(x => x.Id);
        });
    }
}

#endregion

#region Capturing logger plumbing

internal sealed record LogEntry(LogLevel Level, string Category, EventId EventId, string Message);

internal sealed class CapturingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentBag<LogEntry> _entries = new();

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(categoryName, _entries);

    public IReadOnlyList<LogEntry> GetLogs(LogLevel level, string eventName)
        => _entries.Where(e => e.Level == level && e.EventId.Name == eventName).ToList();

    public void Dispose() { }

    private sealed class CapturingLogger : ILogger
    {
        private readonly string _category;
        private readonly ConcurrentBag<LogEntry> _entries;

        public CapturingLogger(string category, ConcurrentBag<LogEntry> entries)
        {
            _category = category;
            _entries = entries;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            _entries.Add(new LogEntry(logLevel, _category, eventId, formatter(state, exception)));
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }
}

#endregion
