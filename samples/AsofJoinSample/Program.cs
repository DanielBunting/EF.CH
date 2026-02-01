using EF.CH.Extensions;
using Microsoft.EntityFrameworkCore;

// ============================================================
// ASOF JOIN Sample
// ============================================================
// Demonstrates ClickHouse ASOF JOIN for time-series data:
// - Matching trades to the most recent quote
// - Finding state at a point in time
// - Correlating events from different sources
//
// Note: ASOF JOIN uses raw SQL because EF Core's query pipeline
// doesn't support custom join types in LINQ extensions.
// ============================================================

Console.WriteLine("ASOF JOIN Sample");
Console.WriteLine("=================\n");

await using var context = new TradingDbContext();

Console.WriteLine("Creating database and tables...");
await context.Database.EnsureDeletedAsync();
await context.Database.EnsureCreatedAsync();

// Insert quotes at various times
Console.WriteLine("Inserting quotes...\n");

var quotes = new List<Quote>
{
    new() { Symbol = "AAPL", QuoteTime = DateTime.UtcNow.AddMinutes(-10), BidPrice = 150.00m, AskPrice = 150.10m },
    new() { Symbol = "AAPL", QuoteTime = DateTime.UtcNow.AddMinutes(-5), BidPrice = 150.50m, AskPrice = 150.60m },
    new() { Symbol = "AAPL", QuoteTime = DateTime.UtcNow.AddMinutes(-1), BidPrice = 151.00m, AskPrice = 151.10m },
    new() { Symbol = "GOOG", QuoteTime = DateTime.UtcNow.AddMinutes(-8), BidPrice = 140.00m, AskPrice = 140.20m },
    new() { Symbol = "GOOG", QuoteTime = DateTime.UtcNow.AddMinutes(-3), BidPrice = 140.50m, AskPrice = 140.70m },
};
context.Quotes.AddRange(quotes);
await context.SaveChangesAsync();

// Insert trades at times between quotes
Console.WriteLine("Inserting trades...\n");

var trades = new List<Trade>
{
    new() { TradeId = Guid.NewGuid(), Symbol = "AAPL", TradeTime = DateTime.UtcNow.AddMinutes(-7), Quantity = 100, Price = 150.25m },
    new() { TradeId = Guid.NewGuid(), Symbol = "AAPL", TradeTime = DateTime.UtcNow.AddMinutes(-2), Quantity = 50, Price = 150.75m },
    new() { TradeId = Guid.NewGuid(), Symbol = "GOOG", TradeTime = DateTime.UtcNow.AddMinutes(-4), Quantity = 200, Price = 140.40m },
};
context.Trades.AddRange(trades);
await context.SaveChangesAsync();

Console.WriteLine($"Inserted {quotes.Count} quotes and {trades.Count} trades.\n");

// Demonstrate ASOF JOIN using raw SQL
Console.WriteLine("--- ASOF JOIN: Trades with Most Recent Quote ---");
Console.WriteLine("Each trade is matched to the quote that was active at trade time.\n");

var asofJoinSql = @"
    SELECT
        t.Symbol,
        t.TradeTime,
        t.Price,
        t.Quantity,
        q.QuoteTime,
        q.BidPrice AS QuoteBid,
        q.AskPrice AS QuoteAsk,
        t.Price - q.BidPrice AS Slippage
    FROM ""Trades"" AS t
    ASOF JOIN ""Quotes"" AS q
    ON t.Symbol = q.Symbol AND t.TradeTime >= q.QuoteTime
    ORDER BY t.Symbol, t.TradeTime";

var tradesWithQuotes = await context.Database
    .SqlQueryRaw<TradeWithQuote>(asofJoinSql)
    .ToListAsync();

foreach (var row in tradesWithQuotes)
{
    Console.WriteLine($"  {row.Symbol} Trade @ {row.TradeTime:HH:mm}: {row.Quantity} @ ${row.Price}");
    Console.WriteLine($"    Quote @ {row.QuoteTime:HH:mm}: Bid ${row.QuoteBid} / Ask ${row.QuoteAsk}");
    Console.WriteLine($"    Slippage: ${row.Slippage:F2}\n");
}

// Demonstrate ASOF LEFT JOIN - includes trades even without matching quotes
Console.WriteLine("--- ASOF LEFT JOIN: Include Trades Without Quotes ---");

// Insert a trade for a symbol with no quotes
context.Trades.Add(new Trade
{
    TradeId = Guid.NewGuid(),
    Symbol = "MSFT",  // No quotes for MSFT
    TradeTime = DateTime.UtcNow.AddMinutes(-6),
    Quantity = 75,
    Price = 380.00m
});
await context.SaveChangesAsync();

// Note: In ClickHouse ASOF LEFT JOIN, unmatched rows get default values (0 for decimals)
// We detect unmatched rows by checking if the join key (Symbol) from the right table is empty
var asofLeftJoinSql = @"
    SELECT
        t.Symbol,
        t.TradeTime,
        t.Price,
        q.Symbol != '' AS HasQuote,
        q.BidPrice AS QuoteBid
    FROM ""Trades"" AS t
    ASOF LEFT JOIN ""Quotes"" AS q
    ON t.Symbol = q.Symbol AND t.TradeTime >= q.QuoteTime
    ORDER BY t.Symbol";

var allTradesWithQuotes = await context.Database
    .SqlQueryRaw<TradeWithQuoteNullable>(asofLeftJoinSql)
    .ToListAsync();

foreach (var row in allTradesWithQuotes)
{
    if (row.HasQuote)
        Console.WriteLine($"  {row.Symbol}: Trade ${row.Price}, Quote Bid ${row.QuoteBid}");
    else
        Console.WriteLine($"  {row.Symbol}: Trade ${row.Price}, NO MATCHING QUOTE");
}

Console.WriteLine("\nDone!");

// ============================================================
// Result Types for Raw SQL Queries
// ============================================================

public class TradeWithQuote
{
    public string Symbol { get; set; } = string.Empty;
    public DateTime TradeTime { get; set; }
    public decimal Price { get; set; }
    public int Quantity { get; set; }
    public DateTime QuoteTime { get; set; }
    public decimal QuoteBid { get; set; }
    public decimal QuoteAsk { get; set; }
    public decimal Slippage { get; set; }
}

public class TradeWithQuoteNullable
{
    public string Symbol { get; set; } = string.Empty;
    public DateTime TradeTime { get; set; }
    public decimal Price { get; set; }
    public bool HasQuote { get; set; }
    public decimal? QuoteBid { get; set; }
}

// ============================================================
// Entity Definitions
// ============================================================

public class Quote
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Symbol { get; set; } = string.Empty;
    public DateTime QuoteTime { get; set; }
    public decimal BidPrice { get; set; }
    public decimal AskPrice { get; set; }
}

public class Trade
{
    public Guid TradeId { get; set; }
    public string Symbol { get; set; } = string.Empty;
    public DateTime TradeTime { get; set; }
    public int Quantity { get; set; }
    public decimal Price { get; set; }
}

// ============================================================
// DbContext Definition
// ============================================================

public class TradingDbContext : DbContext
{
    public DbSet<Quote> Quotes => Set<Quote>();
    public DbSet<Trade> Trades => Set<Trade>();

    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        options.UseClickHouse("Host=localhost;Database=asof_join_sample");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Quote>(entity =>
        {
            entity.ToTable("Quotes");
            entity.HasKey(e => e.Id);
            // ORDER BY must include ASOF column for efficient joins
            entity.UseMergeTree(x => new { x.Symbol, x.QuoteTime });
        });

        modelBuilder.Entity<Trade>(entity =>
        {
            entity.ToTable("Trades");
            entity.HasKey(e => e.TradeId);
            entity.UseMergeTree(x => new { x.Symbol, x.TradeTime });
        });
    }
}
